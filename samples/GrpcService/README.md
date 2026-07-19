# Sample: gRPC service (code-first, for third-party clients)

A **runnable, CI-verified** code-first gRPC service on FS.GG.Net — the "others develop their own
client" story from ADR-0052. A service is defined once as F# records + an interface (no `.proto`),
hosted on ASP.NET Core, and consumed by a client that composes the FS.GG.Net pieces.

Unlike the SC2 sample, this one runs entirely in-process, so the integration test exercises the whole
path **in CI** — the real runtime coverage for `FS.GG.Net.Grpc`.

## Layout

| Path | What |
|---|---|
| `GrpcService.Contracts` | The wire contract: F# `[<ProtoContract>]` records + a `[<ServiceContract>]` `IGreeter` (unary `SayHello` + server-streaming `CountTo`). This is what a third party generates a client against — the *code-first* provenance of ADR-0052 §6. |
| `GrpcService.Server` | The service impl + an in-process `Host.start` (Kestrel + `AddCodeFirstGrpc` + the F# registration). Runnable standalone. |
| `GrpcService.Client` | The client — **pure FS.GG.Net composition**. |

## The point: FS.GG.Net stays thin

The client is the whole argument for keeping `FS.GG.Net.Grpc` a thin lifecycle bridge rather than a
gRPC reimplementation. It composes three existing pieces:

```fsharp
Registration.records Contract.recordTypes          // FS.GG.Net.Protobuf — the F# registration gotcha, as API
let channel = GrpcTransport.connect (Uri address)   // FS.GG.Net.Grpc     — the channel + ConnectionState
let greeter = channel.CreateGrpcService<IGreeter>() // protobuf-net.Grpc  — the typed proxy (not ours)
```

The **server** is ASP.NET Core + `protobuf-net.Grpc` — FS.GG.Net does not wrap it. And the things a
real service needs beyond the pipe — **auth (JWT/mTLS), TLS, streaming policy, deployment,
orchestration** — are deliberately *not here*: they belong to the application host, not the transport
component (see your `GEHost`/`PhysicsSandbox` for that layer).

## Running it

```bash
# Terminal 1 — the server (prints its ephemeral address):
dotnet run --project GrpcService.Server

# Terminal 2 — the client, given that address:
dotnet run --project GrpcService.Client -- http://localhost:<port>
```

```
SayHello -> Hello, World!
CountTo 3:
  tick 1
  tick 2
  tick 3
channel state via FS.GG.Net.Grpc: Connected
```

The sample server is plaintext HTTP/2 (h2c) for zero-config local runs; a real deployment terminates
TLS and adds auth in the host.
