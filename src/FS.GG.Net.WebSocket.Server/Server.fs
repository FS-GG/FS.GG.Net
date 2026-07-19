namespace FS.GG.Net.WebSocket.Server

open System
open System.Net
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Server.Kestrel.Core
open Microsoft.AspNetCore.Hosting.Server
open Microsoft.AspNetCore.Hosting.Server.Features
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open FS.GG.Net.Core
open FS.GG.Net.WebSocket

type ServerHandle =
    { Uri: Uri
      StopAsync: unit -> Task }

[<RequireQualifiedAccess>]
module WebSocketServer =
    let start
        (listenOn: Uri)
        (options: WebSocketOptions)
        (onConnection: ITransport -> Task)
        : Task<ServerHandle> =
        task {
            let host = listenOn.Host
            let port = if listenOn.Port < 0 then 0 else listenOn.Port
            let path = if String.IsNullOrEmpty listenOn.AbsolutePath then "/" else listenOn.AbsolutePath
            let ip = if host = "localhost" then IPAddress.Loopback else IPAddress.Parse host

            let builder = WebApplication.CreateBuilder()
            builder.Logging.ClearProviders() |> ignore
            builder.WebHost.ConfigureKestrel(fun (o: KestrelServerOptions) -> o.Listen(ip, port)) |> ignore
            let app = builder.Build()
            app.UseWebSockets() |> ignore

            app.Use(
                Func<HttpContext, RequestDelegate, Task>(fun ctx next ->
                    (task {
                        if ctx.WebSockets.IsWebSocketRequest && (path = "/" || ctx.Request.Path = PathString(path)) then
                            let! socket = ctx.WebSockets.AcceptWebSocketAsync()
                            let transport = WebSocketTransport.ofSocket socket options
                            do! onConnection transport
                            do! transport.DisposeAsync().AsTask()
                        else
                            do! next.Invoke ctx
                    })
                    :> Task)
            )
            |> ignore

            do! app.StartAsync()

            let bound =
                app.Services
                    .GetRequiredService<IServer>()
                    .Features.Get<IServerAddressesFeature>()
                    .Addresses
                |> Seq.head

            let boundUri = Uri bound
            let wsUri = Uri(sprintf "ws://%s:%d%s" boundUri.Host boundUri.Port path)
            return { Uri = wsUri; StopAsync = fun () -> app.StopAsync() }
        }
