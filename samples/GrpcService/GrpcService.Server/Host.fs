module GrpcService.Server.Host

open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Server.Kestrel.Core
open Microsoft.AspNetCore.Hosting.Server
open Microsoft.AspNetCore.Hosting.Server.Features
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open ProtoBuf.Grpc.Server
open FS.GG.Net.Protobuf

type Running =
    { Address: string
      Stop: unit -> Task }

/// Host the code-first service in-process on an ephemeral h2c (plaintext HTTP/2) port. Reused by the
/// integration test and runnable standalone. The F# record contracts are registered up front via
/// FS.GG.Net.Protobuf.Registration — the fsGRPCSkills "register before you serialize" gotcha, as API.
let start () : Task<Running> =
    task {
        Registration.records GrpcService.Contracts.Contract.recordTypes

        let builder = WebApplication.CreateBuilder()
        builder.Logging.ClearProviders() |> ignore

        builder.WebHost.ConfigureKestrel(fun (o: KestrelServerOptions) ->
            // Loopback on an ephemeral port; ListenLocalhost cannot do dynamic (port 0).
            o.Listen(System.Net.IPAddress.Loopback, 0, fun l -> l.Protocols <- HttpProtocols.Http2))
        |> ignore

        builder.Services.AddCodeFirstGrpc() |> ignore

        let app = builder.Build()
        app.MapGrpcService<GreeterService>() |> ignore
        do! app.StartAsync()

        let address =
            app.Services
                .GetRequiredService<IServer>()
                .Features.Get<IServerAddressesFeature>()
                .Addresses
            |> Seq.head

        return { Address = address; Stop = fun () -> app.StopAsync() }
    }
