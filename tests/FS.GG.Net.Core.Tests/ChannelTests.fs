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

              // Async.AwaitTask surfaces a faulted Task's error as an AggregateException; a `task {}`
              // consumer gets it unwrapped. Unwrap here before matching.
              let unwrap (ex: exn) =
                  match ex with
                  | :? AggregateException as agg -> agg.Flatten().InnerException
                  | _ -> ex

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
                      let e = channel.Incoming.GetAsyncEnumerator(CancellationToken.None)
                      let! _ = e.MoveNextAsync()
                      return e.Current
                  }
                  |> Async.AwaitTask

              Expect.equal first.Text "push" "unsolicited message surfaced on Incoming"
          } ]
