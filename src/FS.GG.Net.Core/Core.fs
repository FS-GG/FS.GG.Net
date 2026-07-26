namespace FS.GG.Net.Core

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks

type ConnectionState =
    | Disconnected
    | Connecting
    | Connected
    | Closing
    | Faulted of error: exn

type ITransport =
    inherit IAsyncDisposable
    abstract member State: ConnectionState
    abstract member Send: message: ReadOnlyMemory<byte> * ct: CancellationToken -> ValueTask
    abstract member Receive: IAsyncEnumerable<ReadOnlyMemory<byte>>

type IMessageCodec<'T> =
    abstract member Encode: value: 'T -> ReadOnlyMemory<byte>
    abstract member Decode: bytes: ReadOnlyMemory<byte> -> 'T

type IdEcho<'Req, 'Resp> =
    { Stamp: 'Req -> uint64 -> 'Req
      Read: 'Resp -> uint64 }

type ServerEcho<'Req, 'Resp> =
    { ReadId: 'Req -> uint64
      StampId: 'Resp -> uint64 -> 'Resp }

[<RequireQualifiedAccess>]
type ServeFailureStage =
    | HandlerExecution
    | ResponseSend

type ServeDiagnostic =
    { Stage: ServeFailureStage
      Error: exn }

type ServeOptions =
    { MaxConcurrentHandlers: int
      CancellationToken: CancellationToken
      OnDiagnostic: ServeDiagnostic -> unit }

[<RequireQualifiedAccess>]
module ServeOptions =
    let defaults =
        { MaxConcurrentHandlers = 64
          CancellationToken = CancellationToken.None
          OnDiagnostic = ignore }

type Correlation<'Req, 'Resp> =
    | Sequential of idEcho: IdEcho<'Req, 'Resp> option
    | Multiplexed of idEcho: IdEcho<'Req, 'Resp>

type IMessageChannel<'Req, 'Resp> =
    inherit IAsyncDisposable
    abstract member State: ConnectionState
    abstract member Exchange: request: 'Req * ct: CancellationToken -> Task<'Resp>
    abstract member Incoming: IAsyncEnumerable<'Resp>

exception CorrelationMismatch of expected: uint64 * actual: uint64

exception MessageChannelClosed of cause: exn option

[<RequireQualifiedAccess>]
module MessageChannel =

    /// The Sequential channel: single request in flight (a 1-permit gate serializes callers), a
    /// background loop decodes inbound messages and either completes the one outstanding exchange or
    /// — when nothing is outstanding — routes the message to `Incoming`. An optional id-echo turns a
    /// misordered/lost response into a `CorrelationMismatch` instead of a silent stale result.
    let private createSequential
        (transport: ITransport)
        (requestCodec: IMessageCodec<'Req>)
        (responseCodec: IMessageCodec<'Resp>)
        (idEcho: IdEcho<'Req, 'Resp> option)
        : IMessageChannel<'Req, 'Resp> =

        let gate = new SemaphoreSlim(1, 1)
        let sync = obj ()
        let mutable pending: TaskCompletionSource<'Resp> option = None
        let incoming = Channel.CreateUnbounded<'Resp>()
        let loopCts = new CancellationTokenSource()
        let disposeCts = new CancellationTokenSource()
        let lifecycleSync = obj ()
        let disposeCompletion =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable disposeStarted = false
        let mutable activeExchanges = 0
        let mutable drainWaiter: TaskCompletionSource<unit> option = None
        let mutable terminalError: exn option = None
        // Guarded by `gate` (single in-flight), so a plain increment is safe — no Interlocked needed.
        let mutable nextId = 0UL

        let tryEnterExchange () =
            lock lifecycleSync (fun () ->
                match terminalError with
                | Some error -> Error error
                | None ->
                    activeExchanges <- activeExchanges + 1
                    Ok())

        let getTerminalError () =
            lock lifecycleSync (fun () -> terminalError)

        let leaveExchange () =
            let waiter =
                lock lifecycleSync (fun () ->
                    activeExchanges <- activeExchanges - 1

                    if disposeStarted && activeExchanges = 0 then
                        let waiter = drainWaiter
                        drainWaiter <- None
                        waiter
                    else
                        None)

            waiter
            |> Option.iter (fun tcs -> tcs.TrySetResult() |> ignore)

        let terminate (cause: exn option) =
            let error, waiting, firstTermination =
                lock lifecycleSync (fun () ->
                    match terminalError with
                    | Some error -> error, None, false
                    | None ->
                        let error = MessageChannelClosed cause
                        terminalError <- Some error

                        let waiting =
                            lock sync (fun () ->
                                let waiting = pending
                                pending <- None
                                waiting)

                        error, waiting, true)

            if firstTermination then
                // Settle the active waiter before waking callers queued on the gate. Every admitted
                // exchange therefore observes the same typed terminal error, never a bare cancellation.
                waiting
                |> Option.iter (fun tcs -> tcs.TrySetException error |> ignore)

                incoming.Writer.TryComplete error |> ignore
                disposeCts.Cancel()

            error

        let receiveLoop () : Task =
            task {
                try
                    use e = transport.Receive.GetAsyncEnumerator(loopCts.Token)
                    let mutable go = true
                    while go do
                        let! moved = e.MoveNextAsync()
                        if not moved then
                            go <- false
                        else
                            let resp = responseCodec.Decode e.Current
                            let waiting =
                                lock sync (fun () ->
                                    match pending with
                                    | Some tcs ->
                                        pending <- None
                                        Some tcs
                                    | None -> None)

                            match waiting with
                            | Some tcs -> tcs.TrySetResult resp |> ignore
                            | None -> incoming.Writer.TryWrite resp |> ignore

                    terminate None |> ignore
                with ex ->
                    let cause =
                        match ex with
                        | :? OperationCanceledException when loopCts.IsCancellationRequested -> None
                        | _ -> Some ex

                    terminate cause |> ignore
            }

        let loop = receiveLoop ()

        let dispose () : Task =
            // Stop admission and settle all admitted exchanges before waiting for them to drain.
            terminate None |> ignore

            let shouldStart, drained =
                lock lifecycleSync (fun () ->
                    if disposeStarted then
                        false, disposeCompletion.Task :> Task
                    else
                        disposeStarted <- true

                        let drained =
                            if activeExchanges = 0 then
                                Task.CompletedTask
                            else
                                let waiter =
                                    TaskCompletionSource<unit>(
                                        TaskCreationOptions.RunContinuationsAsynchronously
                                    )

                                drainWaiter <- Some waiter
                                waiter.Task :> Task

                        true, drained)

            if shouldStart then
                task {
                    try
                        try
                            do! drained
                            loopCts.Cancel()
                            incoming.Writer.TryComplete() |> ignore
                            do! loop
                            do! transport.DisposeAsync().AsTask()
                        finally
                            gate.Dispose()
                            disposeCts.Dispose()
                            loopCts.Dispose()

                        disposeCompletion.TrySetResult() |> ignore
                    with ex ->
                        disposeCompletion.TrySetException ex |> ignore
                }
                |> ignore

            disposeCompletion.Task

        { new IMessageChannel<'Req, 'Resp> with
            member _.State = transport.State
            member _.Incoming = incoming.Reader.ReadAllAsync()

            member _.Exchange(request: 'Req, ct: CancellationToken) : Task<'Resp> =
                task {
                    match tryEnterExchange () with
                    | Error error -> raise error
                    | Ok() -> ()

                    try
                        try
                            use linkedCts =
                                CancellationTokenSource.CreateLinkedTokenSource(ct, disposeCts.Token)

                            let exchangeCt = linkedCts.Token
                            do! gate.WaitAsync exchangeCt

                            try
                                let tcs =
                                    TaskCompletionSource<'Resp>(
                                        TaskCreationOptions.RunContinuationsAsynchronously
                                    )

                                let stamped, expected =
                                    match idEcho with
                                    | Some echo ->
                                        nextId <- nextId + 1UL
                                        echo.Stamp request nextId, Some nextId
                                    | None -> request, None

                                lock sync (fun () -> pending <- Some tcs)
                                use _reg =
                                    exchangeCt.Register(fun () ->
                                        tcs.TrySetCanceled exchangeCt |> ignore)

                                try
                                    do! transport.Send(requestCodec.Encode stamped, exchangeCt)
                                    let! resp = tcs.Task

                                    match idEcho, expected with
                                    | Some echo, Some expectedId ->
                                        let actual = echo.Read resp
                                        if actual <> expectedId then
                                            raise (CorrelationMismatch(expectedId, actual))
                                    | _ -> ()

                                    return resp
                                finally
                                    // Drop our own slot if it is still outstanding (cancel/fault path), so a
                                    // late response cannot land on the NEXT exchange's waiter.
                                    lock sync (fun () ->
                                        match pending with
                                        | Some p when obj.ReferenceEquals(p, tcs) -> pending <- None
                                        | _ -> ())
                            finally
                                gate.Release() |> ignore
                        with :? OperationCanceledException as canceled ->
                            match getTerminalError () with
                            | Some error -> return raise error
                            | None -> return raise canceled
                    finally
                        leaveExchange ()
                }

            member _.DisposeAsync() : ValueTask =
                ValueTask(dispose ()) }

    /// The Multiplexed channel: many requests in flight at once. Each Exchange stamps a unique,
    /// monotonic id, registers its waiter in a concurrent map keyed by that id, and sends. The
    /// background loop matches each response to its waiter by the echoed id (routing an id it does not
    /// recognise to `Incoming`), so responses may arrive in any order. No gate serialises callers.
    let private createMultiplexed
        (transport: ITransport)
        (requestCodec: IMessageCodec<'Req>)
        (responseCodec: IMessageCodec<'Resp>)
        (idEcho: IdEcho<'Req, 'Resp>)
        : IMessageChannel<'Req, 'Resp> =

        let pending = ConcurrentDictionary<uint64, TaskCompletionSource<'Resp>>()
        let incoming = Channel.CreateUnbounded<'Resp>()
        let loopCts = new CancellationTokenSource()
        // A boxed counter so Interlocked can take its address (a captured `let mutable` cannot).
        let counter = [| 0L |]

        let faultAll (ex: exn) =
            for kv in pending do
                kv.Value.TrySetException ex |> ignore

        let receiveLoop () : Task =
            task {
                try
                    use e = transport.Receive.GetAsyncEnumerator(loopCts.Token)
                    let mutable go = true

                    while go do
                        let! moved = e.MoveNextAsync()

                        if not moved then
                            go <- false
                        else
                            let resp = responseCodec.Decode e.Current

                            match pending.TryRemove(idEcho.Read resp) with
                            | true, tcs -> tcs.TrySetResult resp |> ignore
                            | false, _ -> incoming.Writer.TryWrite resp |> ignore

                    incoming.Writer.TryComplete() |> ignore
                    faultAll (Exception "channel closed with request(s) outstanding")
                with ex ->
                    faultAll ex
                    incoming.Writer.TryComplete ex |> ignore
            }

        let loop = receiveLoop ()

        { new IMessageChannel<'Req, 'Resp> with
            member _.State = transport.State
            member _.Incoming = incoming.Reader.ReadAllAsync()

            member _.Exchange(request: 'Req, ct: CancellationToken) : Task<'Resp> =
                task {
                    let id = uint64 (Interlocked.Increment(&counter[0]))

                    let tcs =
                        TaskCompletionSource<'Resp>(TaskCreationOptions.RunContinuationsAsynchronously)

                    pending[id] <- tcs
                    use _reg = ct.Register(fun () -> tcs.TrySetCanceled ct |> ignore)

                    try
                        do! transport.Send(requestCodec.Encode(idEcho.Stamp request id), ct)
                        return! tcs.Task
                    finally
                        // Success path: the loop already removed it. Cancel/fault path: remove it here
                        // so a late response cannot land on a dead waiter.
                        pending.TryRemove id |> ignore
                }

            member _.DisposeAsync() : ValueTask =
                loopCts.Cancel()
                incoming.Writer.TryComplete() |> ignore
                faultAll (Exception "channel disposed")

                ValueTask(
                    task {
                        do! loop
                        loopCts.Dispose()
                        do! transport.DisposeAsync().AsTask()
                    }
                ) }

    let create
        (transport: ITransport)
        (requestCodec: IMessageCodec<'Req>)
        (responseCodec: IMessageCodec<'Resp>)
        (correlation: Correlation<'Req, 'Resp>)
        : IMessageChannel<'Req, 'Resp> =
        match correlation with
        | Sequential idEcho -> createSequential transport requestCodec responseCodec idEcho
        | Multiplexed idEcho -> createMultiplexed transport requestCodec responseCodec idEcho

    let serveWithOptions
        (transport: ITransport)
        (requestCodec: IMessageCodec<'Req>)
        (responseCodec: IMessageCodec<'Resp>)
        (echo: ServerEcho<'Req, 'Resp> option)
        (options: ServeOptions)
        (handler: 'Req -> CancellationToken -> Task<'Resp>)
        : Task =
        if options.MaxConcurrentHandlers < 1 then
            invalidArg (nameof options.MaxConcurrentHandlers) "the handler concurrency limit must be positive"

        task {
            use shutdown = new CancellationTokenSource()

            use linked =
                CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken, shutdown.Token)

            use sendGate = new SemaphoreSlim(1, 1)
            let ct = linked.Token

            let report stage error =
                try
                    options.OnDiagnostic { Stage = stage; Error = error }
                with _ ->
                    // Diagnostics must not become a second connection failure.
                    ()

            let handleOne (request: 'Req) : Task =
                task {
                    try
                        let! response = handler request ct

                        try
                            let response =
                                match echo with
                                | Some e -> e.StampId response (e.ReadId request)
                                | None -> response

                            do! sendGate.WaitAsync ct

                            try
                                do! transport.Send(responseCodec.Encode response, ct)
                            finally
                                sendGate.Release() |> ignore
                        with
                        | :? OperationCanceledException when ct.IsCancellationRequested -> ()
                        | ex -> report ServeFailureStage.ResponseSend ex
                    with
                    | :? OperationCanceledException when ct.IsCancellationRequested -> ()
                    | ex -> report ServeFailureStage.HandlerExecution ex
                }

            // Without correlation replies must retain request order, so that mode always has one worker.
            let workerCount =
                match echo with
                | Some _ -> options.MaxConcurrentHandlers
                | None -> 1

            let queueOptions =
                BoundedChannelOptions(
                    workerCount,
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleWriter = true,
                    SingleReader = (workerCount = 1)
                )

            let requests = Channel.CreateBounded<'Req>(queueOptions)

            let worker () : Task =
                task {
                    try
                        use e = requests.Reader.ReadAllAsync(ct).GetAsyncEnumerator()
                        let mutable go = true

                        while go do
                            let! moved = e.MoveNextAsync()

                            if moved then
                                do! handleOne e.Current
                            else
                                go <- false
                    with :? OperationCanceledException when ct.IsCancellationRequested ->
                        ()
                }

            let workers = Array.init workerCount (fun _ -> worker ())
            let mutable receiveError: exn option = None

            try
                use e = transport.Receive.GetAsyncEnumerator(ct)
                let mutable go = true

                while go do
                    let! moved = e.MoveNextAsync()

                    if moved then
                        let request = requestCodec.Decode e.Current
                        do! requests.Writer.WriteAsync(request, ct)
                    else
                        go <- false
            with
            | :? OperationCanceledException when ct.IsCancellationRequested -> ()
            | ex -> receiveError <- Some ex

            requests.Writer.TryComplete() |> ignore

            if receiveError.IsSome then
                shutdown.Cancel()

            do! Task.WhenAll workers

            match receiveError with
            | Some ex -> return raise ex
            | None -> ()
        }

    let serve
        (transport: ITransport)
        (requestCodec: IMessageCodec<'Req>)
        (responseCodec: IMessageCodec<'Resp>)
        (echo: ServerEcho<'Req, 'Resp> option)
        (handler: 'Req -> Task<'Resp>)
        : Task =
        serveWithOptions
            transport
            requestCodec
            responseCodec
            echo
            ServeOptions.defaults
            (fun request _ -> handler request)
