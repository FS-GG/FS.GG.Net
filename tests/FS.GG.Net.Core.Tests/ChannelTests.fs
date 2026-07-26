module FS.GG.Net.Core.Tests.ChannelTests

open System
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Expecto
open FS.GG.Net.Core

// ---------------------------------------------------------------------------
// A tiny test envelope + codec (id in the first 8 bytes, UTF-8 text after).
// The transport delivers whole messages, so no length framing is needed.
// ---------------------------------------------------------------------------
type Env = { Id: uint64; Text: string }

let codec: IMessageCodec<Env> =
    { new IMessageCodec<Env> with
        member _.Encode(v: Env) =
            let idBytes = BitConverter.GetBytes v.Id
            let txt = Text.Encoding.UTF8.GetBytes v.Text
            ReadOnlyMemory<byte>(Array.append idBytes txt)

        member _.Decode(bytes: ReadOnlyMemory<byte>) =
            let arr = bytes.ToArray()
            let id = BitConverter.ToUInt64(arr, 0)
            let text = Text.Encoding.UTF8.GetString(arr, 8, arr.Length - 8)
            { Id = id; Text = text } }

let idEcho: IdEcho<Env, Env> =
    { Stamp = fun m id -> { m with Id = id }
      Read = fun m -> m.Id }

let serverEcho: ServerEcho<Env, Env> =
    { ReadId = fun m -> m.Id
      StampId = fun m id -> { m with Id = id } }

let private unwrap (ex: exn) =
    match ex with
    | :? AggregateException as agg -> agg.Flatten().InnerException
    | _ -> ex

type TrackingAsyncEnumerable<'T>(source: Collections.Generic.IAsyncEnumerable<'T>) =
    let mutable disposeCount = 0
    member _.DisposeCount = Volatile.Read(&disposeCount)

    interface Collections.Generic.IAsyncEnumerable<'T> with
        member _.GetAsyncEnumerator(ct: CancellationToken) =
            let inner = source.GetAsyncEnumerator ct

            { new Collections.Generic.IAsyncEnumerator<'T> with
                member _.Current = inner.Current
                member _.MoveNextAsync() = inner.MoveNextAsync()

                member _.DisposeAsync() =
                    Interlocked.Increment(&disposeCount) |> ignore
                    inner.DisposeAsync() }

/// A fake transport: `respondTo` models a server replying to each sent message (invoked on Send,
/// after the channel has registered its outstanding exchange), and `Push` injects an unsolicited
/// message with no request outstanding.
type FakeTransport(respondTo: ReadOnlyMemory<byte> -> ReadOnlyMemory<byte> option) =
    let inbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>()
    member _.Push(bytes: ReadOnlyMemory<byte>) = inbound.Writer.TryWrite bytes |> ignore

    interface ITransport with
        member _.State = Connected
        member _.Receive = inbound.Reader.ReadAllAsync()

        member _.Send(message: ReadOnlyMemory<byte>, _ct: CancellationToken) : ValueTask =
            match respondTo message with
            | Some r -> inbound.Writer.TryWrite r |> ignore
            | None -> ()

            ValueTask.CompletedTask

        member _.DisposeAsync() : ValueTask =
            inbound.Writer.TryComplete() |> ignore
            ValueTask.CompletedTask

/// A transport whose replies the TEST drives: `Send` only records the request bytes; `Respond` pushes
/// a response. Lets a test reply out of order, to prove Multiplexed matches by id, not arrival order.
type ManualTransport() =
    let inbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>()
    let trackedInbound = TrackingAsyncEnumerable(inbound.Reader.ReadAllAsync())
    let sent = System.Collections.Concurrent.ConcurrentQueue<ReadOnlyMemory<byte>>()
    let mutable disposeCount = 0
    member _.Sent = sent
    member _.DisposeCount = Volatile.Read(&disposeCount)
    member _.ReceiveDisposeCount = trackedInbound.DisposeCount
    member _.Respond(bytes: ReadOnlyMemory<byte>) = inbound.Writer.TryWrite bytes |> ignore
    member _.CompleteReceive() = inbound.Writer.TryComplete() |> ignore

    interface ITransport with
        member _.State = Connected
        member _.Receive = trackedInbound

        member _.Send(message: ReadOnlyMemory<byte>, _ct: CancellationToken) : ValueTask =
            sent.Enqueue message
            ValueTask.CompletedTask

        member _.DisposeAsync() : ValueTask =
            Interlocked.Increment(&disposeCount) |> ignore
            inbound.Writer.TryComplete() |> ignore
            ValueTask.CompletedTask

/// One end of an in-memory duplex link: sends on `outbound`, receives on `inbound`.
type private LinkedTransport(outbound: Channel<ReadOnlyMemory<byte>>, inbound: Channel<ReadOnlyMemory<byte>>) =
    interface ITransport with
        member _.State = Connected
        member _.Receive = inbound.Reader.ReadAllAsync()

        member _.Send(message: ReadOnlyMemory<byte>, _ct: CancellationToken) : ValueTask =
            outbound.Writer.TryWrite message |> ignore
            ValueTask.CompletedTask

        member _.DisposeAsync() : ValueTask =
            outbound.Writer.TryComplete() |> ignore
            ValueTask.CompletedTask

/// A connected pair of transports: whatever one sends, the other receives.
let private pair () : ITransport * ITransport =
    let a2b = Channel.CreateUnbounded<ReadOnlyMemory<byte>>()
    let b2a = Channel.CreateUnbounded<ReadOnlyMemory<byte>>()
    LinkedTransport(a2b, b2a) :> ITransport, LinkedTransport(b2a, a2b) :> ITransport

[<Tests>]
let tests =
    testList
        "MessageChannel / Sequential"
        [ testCaseAsync "returns the correlated response"
          <| async {
              let server =
                  fun (req: ReadOnlyMemory<byte>) ->
                      let m = codec.Decode req
                      Some(codec.Encode { m with Text = "reply:" + m.Text })

              let transport = FakeTransport(server) :> ITransport
              let channel = MessageChannel.create transport codec codec (Sequential(Some idEcho))

              let! resp =
                  channel.Exchange({ Id = 0UL; Text = "ping" }, CancellationToken.None)
                  |> Async.AwaitTask

              Expect.equal resp.Text "reply:ping" "payload round-trips through the channel"
          }

          testCaseAsync "raises CorrelationMismatch when the response id does not echo"
          <| async {
              let server =
                  fun (req: ReadOnlyMemory<byte>) ->
                      let m = codec.Decode req
                      // Echo a WRONG id — the desync the guard exists to catch.
                      Some(codec.Encode { Id = m.Id + 999UL; Text = "stale" })

              let transport = FakeTransport(server) :> ITransport
              let channel = MessageChannel.create transport codec codec (Sequential(Some idEcho))

              let! result =
                  channel.Exchange({ Id = 0UL; Text = "ping" }, CancellationToken.None)
                  |> Async.AwaitTask
                  |> Async.Catch

              match result with
              | Choice2Of2 ex when (unwrap ex :? CorrelationMismatch) -> ()
              | Choice2Of2 ex -> failtestf "expected CorrelationMismatch, got %O" ex
              | Choice1Of2 _ -> failtest "expected CorrelationMismatch, exchange returned a value"
          }

          testCaseAsync "routes an unsolicited message to Incoming"
          <| async {
              let transport = FakeTransport(fun _ -> None)
              let channel =
                  MessageChannel.create (transport :> ITransport) codec codec (Sequential None)

              transport.Push(codec.Encode { Id = 7UL; Text = "push" })

              let! first =
                  task {
                      use e = channel.Incoming.GetAsyncEnumerator(CancellationToken.None)
                      let! _ = e.MoveNextAsync()
                      return e.Current
                  }
                  |> Async.AwaitTask

              Expect.equal first.Text "push" "unsolicited message surfaced on Incoming"
          }

          testCaseAsync "clean EOF faults an outstanding exchange with MessageChannelClosed"
          <| async {
              let transport = ManualTransport()

              let channel =
                  MessageChannel.create (transport :> ITransport) codec codec (Sequential None)

              let exchange =
                  channel.Exchange({ Id = 0UL; Text = "pending" }, CancellationToken.None)

              while transport.Sent.IsEmpty do
                  do! Async.Sleep 5

              transport.CompleteReceive()

              let! result =
                  exchange.WaitAsync(TimeSpan.FromSeconds 5.0)
                  |> Async.AwaitTask
                  |> Async.Catch

              match result with
              | Choice2Of2 ex when (unwrap ex :? MessageChannelClosed) -> ()
              | Choice2Of2 ex -> failtestf "expected MessageChannelClosed, got %O" ex
              | Choice1Of2 _ -> failtest "an exchange returned after EOF without a response"

              do! channel.DisposeAsync().AsTask() |> Async.AwaitTask
          }

          testCaseAsync "dispose stops admission, settles an exchange, and is idempotent"
          <| async {
              let transport = ManualTransport()

              let channel =
                  MessageChannel.create (transport :> ITransport) codec codec (Sequential None)

              let activeExchange =
                  channel.Exchange({ Id = 0UL; Text = "pending" }, CancellationToken.None)

              while transport.Sent.IsEmpty do
                  do! Async.Sleep 5

              // This second call is admitted but parked on the one-permit gate. Disposal must
              // settle both it and the active waiter before that gate can be disposed.
              let queuedExchange =
                  channel.Exchange({ Id = 0UL; Text = "queued" }, CancellationToken.None)

              let firstDispose = channel.DisposeAsync().AsTask()
              let secondDispose = channel.DisposeAsync().AsTask()

              let! exchangeResults =
                  [| activeExchange; queuedExchange |]
                  |> Array.map (Async.AwaitTask >> Async.Catch)
                  |> Async.Parallel

              for exchangeResult in exchangeResults do
                  match exchangeResult with
                  | Choice2Of2 ex when (unwrap ex :? MessageChannelClosed) -> ()
                  | Choice2Of2 ex -> failtestf "expected MessageChannelClosed, got %O" ex
                  | Choice1Of2 _ ->
                      failtest "an admitted exchange must settle when the channel closes"

              let! rejected =
                  channel.Exchange({ Id = 0UL; Text = "late" }, CancellationToken.None)
                  |> Async.AwaitTask
                  |> Async.Catch

              match rejected with
              | Choice2Of2 ex when (unwrap ex :? MessageChannelClosed) -> ()
              | Choice2Of2 ex ->
                  failtestf "expected MessageChannelClosed after admission stopped, got %O" ex
              | Choice1Of2 _ -> failtest "an exchange was admitted after disposal started"

              do! Task.WhenAll(firstDispose, secondDispose) |> Async.AwaitTask
              Expect.equal transport.DisposeCount 1 "concurrent DisposeAsync calls dispose the transport once"
              Expect.equal transport.ReceiveDisposeCount 1 "shutdown disposes the receive enumerator"
          }

          testCaseAsync "Multiplexed disposal awaits receive-enumerator disposal"
          <| async {
              let transport = ManualTransport()

              let channel =
                  MessageChannel.create (transport :> ITransport) codec codec (Multiplexed idEcho)

              do! channel.DisposeAsync().AsTask() |> Async.AwaitTask
              Expect.equal transport.ReceiveDisposeCount 1 "DisposeAsync completes after enumerator disposal"
          }

          testCaseAsync "serve disposes its receive enumerator when the transport completes"
          <| async {
              let transport = ManualTransport()

              let serving =
                  MessageChannel.serve
                      (transport :> ITransport)
                      codec
                      codec
                      None
                      (fun request -> Task.FromResult request)

              transport.CompleteReceive()
              do! serving |> Async.AwaitTask
              Expect.equal transport.ReceiveDisposeCount 1 "normal server shutdown disposes the enumerator"
          }

          testCaseAsync "serveWithOptions bounds concurrent handlers"
          <| async {
              let transport = ManualTransport()
              let release = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
              let started = System.Collections.Concurrent.ConcurrentQueue<int>()
              let sync = obj ()
              let mutable active = 0
              let mutable maximum = 0

              let handler (request: Env) (_ct: CancellationToken) =
                  task {
                      let current = Interlocked.Increment(&active)
                      lock sync (fun () -> maximum <- max maximum current)
                      started.Enqueue(int request.Id)
                      do! release.Task
                      Interlocked.Decrement(&active) |> ignore
                      return request
                  }

              let options =
                  { ServeOptions.defaults with
                      MaxConcurrentHandlers = 2 }

              let serving =
                  MessageChannel.serveWithOptions
                      (transport :> ITransport)
                      codec
                      codec
                      (Some serverEcho)
                      options
                      handler

              for id in 1UL..3UL do
                  transport.Respond(codec.Encode { Id = id; Text = string id })

              while started.Count < 2 do
                  do! Async.Sleep 5

              do! Async.Sleep 25
              Expect.equal started.Count 2 "the third request waits behind the concurrency bound"
              Expect.equal maximum 2 "no more than two handlers run concurrently"

              transport.CompleteReceive()
              Expect.isFalse serving.IsCompleted "normal shutdown drains active and queued requests"
              release.TrySetResult() |> ignore
              do! serving |> Async.AwaitTask
              Expect.equal started.Count 3 "every admitted request is drained"
          }

          testCaseAsync "serveWithOptions links cancellation and drains active handlers"
          <| async {
              let transport = ManualTransport()
              use stop = new CancellationTokenSource()
              let started = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
              let cancelled = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
              let finish = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

              let handler (request: Env) (ct: CancellationToken) =
                  task {
                      started.TrySetResult() |> ignore

                      try
                          do! Task.Delay(Timeout.InfiniteTimeSpan, ct)
                      with :? OperationCanceledException ->
                          cancelled.TrySetResult() |> ignore

                      do! finish.Task
                      return request
                  }

              let options =
                  { ServeOptions.defaults with
                      CancellationToken = stop.Token }

              let serving =
                  MessageChannel.serveWithOptions
                      (transport :> ITransport)
                      codec
                      codec
                      (Some serverEcho)
                      options
                      handler

              transport.Respond(codec.Encode { Id = 1UL; Text = "active" })
              do! started.Task |> Async.AwaitTask
              stop.Cancel()
              do! cancelled.Task |> Async.AwaitTask
              Expect.isFalse serving.IsCompleted "shutdown waits for the active handler to drain"
              finish.TrySetResult() |> ignore
              do! serving |> Async.AwaitTask
          }

          testCaseAsync "serveWithOptions reports handler failures promptly"
          <| async {
              let transport = ManualTransport()

              let diagnostic =
                  TaskCompletionSource<ServeDiagnostic>(TaskCreationOptions.RunContinuationsAsynchronously)

              let options =
                  { ServeOptions.defaults with
                      OnDiagnostic = fun event -> diagnostic.TrySetResult event |> ignore }

              let serving =
                  MessageChannel.serveWithOptions
                      (transport :> ITransport)
                      codec
                      codec
                      (Some serverEcho)
                      options
                      (fun _ _ -> Task.FromException<Env>(InvalidOperationException "boom"))

              transport.Respond(codec.Encode { Id = 1UL; Text = "bad" })
              let! observed = diagnostic.Task.WaitAsync(TimeSpan.FromSeconds 5.0) |> Async.AwaitTask
              Expect.equal observed.Stage ServeFailureStage.HandlerExecution "failure stage is structured"
              Expect.equal observed.Error.Message "boom" "the original handler failure is retained"
              Expect.isFalse serving.IsCompleted "the diagnostic is emitted before another message or shutdown"
              transport.CompleteReceive()
              do! serving |> Async.AwaitTask
          }

          testCaseAsync "Multiplexed matches concurrent responses by id, out of order"
          <| async {
              let transport = ManualTransport()

              let channel =
                  MessageChannel.create (transport :> ITransport) codec codec (Multiplexed idEcho)

              // Two exchanges in flight at once.
              let ta = channel.Exchange({ Id = 0UL; Text = "A" }, CancellationToken.None)
              let tb = channel.Exchange({ Id = 0UL; Text = "B" }, CancellationToken.None)

              // Wait until both requests have been sent, then learn their stamped ids.
              while transport.Sent.Count < 2 do
                  do! Async.Sleep 5

              let reqs = transport.Sent.ToArray() |> Array.map codec.Decode

              // Reply in REVERSE send order — the point is that the echoed id, not arrival order,
              // routes each response to its own waiter.
              for r in Array.rev reqs do
                  transport.Respond(codec.Encode { r with Text = "reply:" + r.Text })

              let! ra = ta |> Async.AwaitTask
              let! rb = tb |> Async.AwaitTask
              Expect.equal ra.Text "reply:A" "exchange A got A's response despite out-of-order delivery"
              Expect.equal rb.Text "reply:B" "exchange B got B's response"
          }

          testCaseAsync "serve + ServerEcho answers a Multiplexed client over a linked transport"
          <| async {
              // The server side (MessageChannel.serve, id-echoed, concurrent) wired to a Multiplexed
              // client over an in-memory duplex pair — the full client/server correlation loop.
              let clientEnd, serverEnd = pair ()

              let handler (req: Env) : Task<Env> = Task.FromResult { req with Text = "ok:" + req.Text }
              let _serveLoop = MessageChannel.serve serverEnd codec codec (Some serverEcho) handler

              let channel = MessageChannel.create clientEnd codec codec (Multiplexed idEcho)
              let ta = channel.Exchange({ Id = 0UL; Text = "A" }, CancellationToken.None)
              let tb = channel.Exchange({ Id = 0UL; Text = "B" }, CancellationToken.None)
              let! ra = ta |> Async.AwaitTask
              let! rb = tb |> Async.AwaitTask
              Expect.equal ra.Text "ok:A" "server answered A (id-echoed)"
              Expect.equal rb.Text "ok:B" "server answered B (id-echoed)"
          } ]
