module FS.GG.Net.IntegrationTests.EchoServer

open System
open System.Net.WebSockets
open System.Threading
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Microsoft.AspNetCore.Hosting.Server
open Microsoft.AspNetCore.Hosting.Server.Features

type Running =
    { Uri: Uri
      Stop: unit -> Task }

type Pushing =
    { Uri: Uri
      PeerCloseStatus: Task<WebSocketCloseStatus option>
      Stop: unit -> Task }

/// Start an in-process WebSocket echo server on an ephemeral port. Each inbound message is
/// accumulated to EndOfMessage and echoed back whole as one binary message — so a payload larger
/// than the client's receive buffer round-trips through the client transport's fragment reassembly.
let start () : Task<Running> =
    task {
        let builder = WebApplication.CreateBuilder()
        builder.Logging.ClearProviders() |> ignore
        builder.WebHost.UseUrls("http://127.0.0.1:0") |> ignore
        let app = builder.Build()
        app.UseWebSockets() |> ignore

        app.Use(
            Func<HttpContext, RequestDelegate, Task>(fun ctx _next ->
                (task {
                    if ctx.WebSockets.IsWebSocketRequest then
                        use! ws = ctx.WebSockets.AcceptWebSocketAsync()
                        let buf = Array.zeroCreate<byte> 16384
                        use ms = new System.IO.MemoryStream()
                        let mutable go = true

                        while go && ws.State = WebSocketState.Open do
                            let! res = ws.ReceiveAsync(ArraySegment<byte>(buf), CancellationToken.None)

                            if res.MessageType = WebSocketMessageType.Close then
                                do! ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None)
                                go <- false
                            else
                                ms.Write(buf, 0, res.Count)

                                if res.EndOfMessage then
                                    let payload = ms.ToArray()
                                    ms.SetLength 0L

                                    do!
                                        ws.SendAsync(
                                            ArraySegment<byte>(payload),
                                            WebSocketMessageType.Binary,
                                            true,
                                            CancellationToken.None
                                        )
                    else
                        ctx.Response.StatusCode <- 400
                })
                :> Task)
        )
        |> ignore

        do! app.StartAsync()

        let addresses =
            app.Services
                .GetRequiredService<IServer>()
                .Features.Get<IServerAddressesFeature>()
                .Addresses

        let port = Uri(Seq.head addresses).Port
        let wsUri = Uri(sprintf "ws://127.0.0.1:%d/echo" port)
        return { Uri = wsUri; Stop = fun () -> app.StopAsync() }
    }

/// Start a server that sends one binary message immediately and records the close status returned by
/// the peer. This drives the client's inbound-size guard from the wire rather than through a fake.
let startPushing (payload: byte[]) : Task<Pushing> =
    task {
        let closeStatus =
            TaskCompletionSource<WebSocketCloseStatus option>(TaskCreationOptions.RunContinuationsAsynchronously)

        let builder = WebApplication.CreateBuilder()
        builder.Logging.ClearProviders() |> ignore
        builder.WebHost.UseUrls("http://127.0.0.1:0") |> ignore
        let app = builder.Build()
        app.UseWebSockets() |> ignore

        app.Use(
            Func<HttpContext, RequestDelegate, Task>(fun ctx _next ->
                (task {
                    if ctx.WebSockets.IsWebSocketRequest then
                        use! ws = ctx.WebSockets.AcceptWebSocketAsync()

                        do!
                            ws.SendAsync(
                                ArraySegment<byte>(payload),
                                WebSocketMessageType.Binary,
                                true,
                                CancellationToken.None
                            )

                        let! response =
                            ws.ReceiveAsync(ArraySegment<byte>(Array.zeroCreate<byte> 1), CancellationToken.None)

                        let observedStatus =
                            if response.CloseStatus.HasValue then
                                Some response.CloseStatus.Value
                            else
                                None

                        closeStatus.TrySetResult observedStatus |> ignore

                        if ws.State = WebSocketState.CloseReceived then
                            do!
                                ws.CloseOutputAsync(
                                    defaultArg observedStatus WebSocketCloseStatus.NormalClosure,
                                    "ack",
                                    CancellationToken.None
                                )
                    else
                        ctx.Response.StatusCode <- 400
                })
                :> Task)
        )
        |> ignore

        do! app.StartAsync()

        let addresses =
            app.Services
                .GetRequiredService<IServer>()
                .Features.Get<IServerAddressesFeature>()
                .Addresses

        let port = Uri(Seq.head addresses).Port

        return
            { Uri = Uri(sprintf "ws://127.0.0.1:%d/push" port)
              PeerCloseStatus = closeStatus.Task
              Stop = fun () -> app.StopAsync() }
    }
