namespace FS.GG.Net.WebSocket

open System
open System.Buffers
open System.IO
open System.Net.WebSockets
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open FS.GG.Net.Core

type WebSocketOptions =
    { ConnectRetries: int
      ConnectBackoff: TimeSpan
      ReceiveBufferSize: int }

[<RequireQualifiedAccess>]
module WebSocketOptions =
    let defaults =
        { ConnectRetries = 40
          ConnectBackoff = TimeSpan.FromMilliseconds 250.0
          ReceiveBufferSize = 64 * 1024 }

/// A client WebSocket ITransport. A background loop reassembles continuation frames into complete
/// application messages and publishes them on an unbounded channel; the read buffer is pooled.
type private ClientWsTransport(ws: ClientWebSocket, options: WebSocketOptions) =
    let inbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>()
    let loopCts = new CancellationTokenSource()
    let mutable state = Connected

    let receiveLoop () : Task =
        task {
            let buffer = ArrayPool<byte>.Shared.Rent options.ReceiveBufferSize
            let acc = new MemoryStream()

            try
                try
                    let mutable go = true

                    while go do
                        let! result = ws.ReceiveAsync(Memory<byte>(buffer), loopCts.Token)

                        match result.MessageType with
                        | WebSocketMessageType.Close ->
                            go <- false
                            state <- Closing
                        | _ ->
                            acc.Write(buffer, 0, result.Count)

                            if result.EndOfMessage then
                                let msg = acc.ToArray()
                                acc.SetLength 0L
                                inbound.Writer.TryWrite(ReadOnlyMemory<byte> msg) |> ignore

                    inbound.Writer.TryComplete() |> ignore
                with
                | :? OperationCanceledException ->
                    // Dispose cancelled the loop — an orderly stop, not a fault.
                    inbound.Writer.TryComplete() |> ignore
                | ex ->
                    state <- Faulted ex
                    inbound.Writer.TryComplete ex |> ignore
            finally
                acc.Dispose()
                ArrayPool<byte>.Shared.Return buffer
        }

    let loop = receiveLoop ()

    interface ITransport with
        member _.State = state
        member _.Receive = inbound.Reader.ReadAllAsync()

        member _.Send(message: ReadOnlyMemory<byte>, ct: CancellationToken) : ValueTask =
            ws.SendAsync(message, WebSocketMessageType.Binary, true, ct)

        member _.DisposeAsync() : ValueTask =
            ValueTask(
                task {
                    state <- Closing
                    loopCts.Cancel()

                    try
                        if ws.State = WebSocketState.Open then
                            do! ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", CancellationToken.None)
                    with _ ->
                        ()

                    ws.Dispose()
                    do! loop
                    state <- Disconnected
                }
            )

[<RequireQualifiedAccess>]
module WebSocketTransport =
    let connectAsync (uri: Uri) (options: WebSocketOptions) (ct: CancellationToken) : Task<ITransport> =
        task {
            let mutable attempt = 0
            let mutable connected: ITransport option = None
            let mutable lastError: exn = Unchecked.defaultof<exn>

            while connected.IsNone && attempt < options.ConnectRetries do
                attempt <- attempt + 1
                let ws = new ClientWebSocket()

                try
                    do! ws.ConnectAsync(uri, ct)
                    connected <- Some(new ClientWsTransport(ws, options) :> ITransport)
                with ex ->
                    lastError <- ex
                    ws.Dispose()

                    if connected.IsNone && attempt < options.ConnectRetries then
                        do! Task.Delay(options.ConnectBackoff, ct)

            match connected with
            | Some transport -> return transport
            | None ->
                return
                    raise (
                        TimeoutException($"WebSocket connect to {uri} failed after {attempt} attempt(s).", lastError)
                    )
        }
