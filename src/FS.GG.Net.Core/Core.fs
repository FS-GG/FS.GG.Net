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

type Correlation<'Req, 'Resp> =
    | Sequential of idEcho: IdEcho<'Req, 'Resp> option
    | Multiplexed of idEcho: IdEcho<'Req, 'Resp>

type IMessageChannel<'Req, 'Resp> =
    inherit IAsyncDisposable
    abstract member State: ConnectionState
    abstract member Exchange: request: 'Req * ct: CancellationToken -> Task<'Resp>
    abstract member Incoming: IAsyncEnumerable<'Resp>

exception CorrelationMismatch of expected: uint64 * actual: uint64

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
        // Guarded by `gate` (single in-flight), so a plain increment is safe — no Interlocked needed.
        let mutable nextId = 0UL

        let receiveLoop () : Task =
            task {
                try
                    let e = transport.Receive.GetAsyncEnumerator(loopCts.Token)
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

                    incoming.Writer.TryComplete() |> ignore
                with ex ->
                    lock sync (fun () ->
                        match pending with
                        | Some tcs -> tcs.TrySetException ex |> ignore
                        | None -> ()

                        pending <- None)

                    incoming.Writer.TryComplete ex |> ignore
            }

        let loop = receiveLoop ()

        { new IMessageChannel<'Req, 'Resp> with
            member _.State = transport.State
            member _.Incoming = incoming.Reader.ReadAllAsync()

            member _.Exchange(request: 'Req, ct: CancellationToken) : Task<'Resp> =
                task {
                    do! gate.WaitAsync ct

                    try
                        let tcs =
                            TaskCompletionSource<'Resp>(TaskCreationOptions.RunContinuationsAsynchronously)

                        let stamped, expected =
                            match idEcho with
                            | Some echo ->
                                nextId <- nextId + 1UL
                                echo.Stamp request nextId, Some nextId
                            | None -> request, None

                        lock sync (fun () -> pending <- Some tcs)
                        use _reg = ct.Register(fun () -> tcs.TrySetCanceled ct |> ignore)

                        try
                            do! transport.Send(requestCodec.Encode stamped, ct)
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
                }

            member _.DisposeAsync() : ValueTask =
                loopCts.Cancel()
                incoming.Writer.TryComplete() |> ignore
                loop |> ignore
                gate.Dispose()
                transport.DisposeAsync() }

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
                    let e = transport.Receive.GetAsyncEnumerator(loopCts.Token)
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
                loop |> ignore
                transport.DisposeAsync() }

    let create
        (transport: ITransport)
        (requestCodec: IMessageCodec<'Req>)
        (responseCodec: IMessageCodec<'Resp>)
        (correlation: Correlation<'Req, 'Resp>)
        : IMessageChannel<'Req, 'Resp> =
        match correlation with
        | Sequential idEcho -> createSequential transport requestCodec responseCodec idEcho
        | Multiplexed idEcho -> createMultiplexed transport requestCodec responseCodec idEcho
