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
      ReceiveBufferSize: int
      InboundCapacity: int
      MaxMessageSize: int }

[<RequireQualifiedAccess>]
module WebSocketOptions =
    let defaults =
        { ConnectRetries = 40
          ConnectBackoff = TimeSpan.FromMilliseconds 250.0
          ReceiveBufferSize = 64 * 1024
          InboundCapacity = 16
          MaxMessageSize = 64 * 1024 * 1024 }

/// A WebSocket ITransport over any open socket (client-connected or server-accepted). A background
/// loop reassembles continuation frames into complete application messages and publishes them on an
/// bounded channel; when consumers fall behind, awaiting the channel write applies backpressure to
/// socket reads. The read buffer is pooled and assembled messages cannot exceed MaxMessageSize.
type private SocketTransport(ws: WebSocket, options: WebSocketOptions) =
    do
        if options.ReceiveBufferSize <= 0 then
            invalidArg (nameof options.ReceiveBufferSize) "ReceiveBufferSize must be positive."

        if options.InboundCapacity <= 0 then
            invalidArg (nameof options.InboundCapacity) "InboundCapacity must be positive."

        if options.MaxMessageSize <= 0 then
            invalidArg (nameof options.MaxMessageSize) "MaxMessageSize must be positive."

    let channelOptions = BoundedChannelOptions(options.InboundCapacity)

    do
        channelOptions.FullMode <- BoundedChannelFullMode.Wait
        channelOptions.SingleWriter <- true
        channelOptions.SingleReader <- false

    let inbound = Channel.CreateBounded<ReadOnlyMemory<byte>>(channelOptions)
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
                            let assembledSize = acc.Length + int64 result.Count

                            if assembledSize > int64 options.MaxMessageSize then
                                go <- false
                                state <- Closing
                                acc.SetLength 0L

                                if ws.State = WebSocketState.Open || ws.State = WebSocketState.CloseReceived then
                                    do!
                                        ws.CloseOutputAsync(
                                            WebSocketCloseStatus.MessageTooBig,
                                            $"message exceeds configured maximum of {options.MaxMessageSize} bytes",
                                            CancellationToken.None
                                        )
                            else
                                acc.Write(buffer, 0, result.Count)

                                if result.EndOfMessage then
                                    let msg = acc.ToArray()
                                    acc.SetLength 0L

                                    // FullMode.Wait plus an awaited write is the backpressure boundary:
                                    // the socket is not read again until the consumer frees channel space.
                                    do!
                                        inbound.Writer.WriteAsync(ReadOnlyMemory<byte> msg, loopCts.Token).AsTask()

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
    let ofSocket (socket: WebSocket) (options: WebSocketOptions) : ITransport =
        new SocketTransport(socket, options) :> ITransport

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
                    connected <- Some(ofSocket ws options)
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
