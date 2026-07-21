# Getting started with FS.GG.Net

FS.GG.Net carries protobuf messages over a WebSocket or a gRPC channel, and nothing
above that: it knows no `.proto` and no game types — the message schemas live in the
application that consumes it. This guide installs the packages and then walks two
**real, runnable** exchanges end to end:

1. [A code-first gRPC exchange](#walkthrough-1-a-code-first-grpc-exchange) — runs
   entirely in-process, no external server, so you can run it from a clone in one terminal.
2. [The StarCraft II WebSocket handshake](#walkthrough-2-the-starcraft-ii-websocket-handshake)
   — a full game handshake against Blizzard's headless SC2 server, over raw protobuf on a WebSocket.

Both exist as buildable samples in this repo under [`samples/`](../samples); this guide
is the narrated path through them.

## Install

Every `FS.GG.*` package is **public on [nuget.org](https://www.nuget.org) and restores
with no credential** ([ADR-0039][adr-0039]) — no `--add-source`, no org feed, no token.
Start from the entry package and add the pieces your transport needs:

```sh
dotnet add package FS.GG.Net.Core
```

### The packages

`FS.GG.Net` ships as a coherent set. Take `FS.GG.Net.Core` plus whichever transport and
serialization pieces your app uses:

| Package | Add it when you need | Responsibility |
|---|---|---|
| **FS.GG.Net.Core** | always (the seams) | Pure, BCL-only seams: `ConnectionState`, `ITransport`, `IMessageCodec`, `IdEcho`, `Correlation`, `IMessageChannel`, and a working `Sequential` correlator. |
| **FS.GG.Net.WebSocket** | a client talking protobuf over a WebSocket | `ITransport` over `ClientWebSocket` (`connectAsync`, or `ofSocket` for any open socket) — fragment reassembly, pooled buffers, initial connect-retry. The SC2 substrate. |
| **FS.GG.Net.WebSocket.Server** | serving that protocol | A Kestrel acceptor (`WebSocketServer.start`) that hands each connection to a handler as an `ITransport`. Pair with `MessageChannel.serve`. |
| **FS.GG.Net.Protobuf** | encoding/decoding messages | `IMessageCodec` for Google.Protobuf (the reproducible raw path SC2-over-WS needs) and protobuf-net (code-first, owned schemas). Absorbs the F#/protobuf-net registration gotchas. |
| **FS.GG.Net.Grpc** | a gRPC channel | A thin lifecycle bridge over grpc-dotnet — projects a channel's connectivity onto `ConnectionState`. Does not re-abstract gRPC method dispatch. |
| **FS.GG.Net.Elmish** | driving it from an Elmish app | `Net.Cmd.exchange` / `Net.Sub.incoming` over an `IMessageChannel`. Depends on standard Elmish, never on FS.GG.UI. |

## Walkthrough 1: a code-first gRPC exchange

This is the "others develop their own client" story — a service defined **once** as F#
records plus an interface (no `.proto`), hosted on ASP.NET Core, and consumed by a client
that composes the FS.GG.Net pieces. It runs entirely in-process, which is why it is the
runtime coverage for `FS.GG.Net.Grpc` in CI. The full sample is
[`samples/GrpcService`](../samples/GrpcService).

Packages: `FS.GG.Net.Grpc`, `FS.GG.Net.Protobuf`.

### 1. Define the wire contract

The contract is plain F# — records tagged for protobuf-net, and a service interface. This
*is* the wire contract a third party generates a client against; there is no `.proto` to
vendor.

```fsharp
namespace GrpcService.Contracts

open System.Collections.Generic
open System.ServiceModel
open System.Threading.Tasks
open ProtoBuf
open ProtoBuf.Grpc

[<ProtoContract>]
type GreetRequest = { [<ProtoMember(1)>] Name: string }
[<ProtoContract>]
type GreetReply = { [<ProtoMember(1)>] Message: string }
[<ProtoContract>]
type CountRequest = { [<ProtoMember(1)>] To: int }
[<ProtoContract>]
type Tick = { [<ProtoMember(1)>] N: int }

[<ServiceContract>]
type IGreeter =
    [<OperationContract>]
    abstract SayHello: request: GreetRequest * context: CallContext -> ValueTask<GreetReply>
    [<OperationContract>]
    abstract CountTo: request: CountRequest * context: CallContext -> IAsyncEnumerable<Tick>

module Contract =
    let recordTypes: System.Type list =
        [ typeof<GreetRequest>; typeof<GreetReply>; typeof<CountRequest>; typeof<Tick> ]
```

> **F#/protobuf-net gotcha:** register each record before serializing (the next step does
> that), and prefer `array` over `list` and `Dictionary` over `Map` on the wire.

### 2. Compose the client

The client is pure FS.GG.Net composition — three existing pieces, no gRPC reimplementation:

```fsharp
open System
open ProtoBuf.Grpc
open ProtoBuf.Grpc.Client
open FS.GG.Net.Grpc
open FS.GG.Net.Protobuf
open GrpcService.Contracts

// The sample server is plaintext HTTP/2 (h2c) for zero-config local runs.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)

Registration.records Contract.recordTypes            // FS.GG.Net.Protobuf — the F# registration
let channel = GrpcTransport.connect (Uri "http://localhost:5000")  // FS.GG.Net.Grpc — channel + ConnectionState
let greeter = channel.CreateGrpcService<IGreeter>()  // protobuf-net.Grpc — the typed proxy (not ours)

let reply = greeter.SayHello({ Name = "World" }, CallContext.Default).Result
printfn "SayHello -> %s" reply.Message
printfn "channel state via FS.GG.Net.Grpc: %A" (GrpcTransport.stateOf channel)
```

### 3. Run it

From the sample, one terminal for the server and one for the client:

```bash
# Terminal 1 — the server (prints its ephemeral address):
dotnet run --project samples/GrpcService/GrpcService.Server

# Terminal 2 — the client, given that address:
dotnet run --project samples/GrpcService/GrpcService.Client -- http://localhost:<port>
```

Expected output:

```
SayHello -> Hello, World!
CountTo 3:
  tick 1
  tick 2
  tick 3
channel state via FS.GG.Net.Grpc: Connected
```

The server is ASP.NET Core + protobuf-net.Grpc — FS.GG.Net does not wrap it. Auth (JWT/mTLS),
TLS, streaming policy, and deployment are deliberately not here: they belong to the
application host, not the transport component.

## Walkthrough 2: the StarCraft II WebSocket handshake

This drives a full game handshake against Blizzard's headless SC2 server, entirely over
raw protobuf on a WebSocket:

```
Ping → CreateGame → JoinGame(raw) → Observation → Step → Observation → LeaveGame → Quit
```

Every step is a correlated protobuf `Request`/`Response` over a WebSocket —
`WebSocketTransport` + `Codec.google` + `MessageChannel` with `Sequential (Some idEcho)`.
SC2's `Request`/`Response` both carry `id` (proto field 97), so the id-verified correlator's
desync guard is live: a lost or misordered response surfaces as a `CorrelationMismatch`
instead of a silently stale observation. The full sample is
[`samples/Sc2Handshake`](../samples/Sc2Handshake).

Packages: `FS.GG.Net.WebSocket`, `FS.GG.Net.Protobuf` (Google.Protobuf codec).

### The shape of one exchange

`MessageChannel.create` wraps a transport with codecs and a correlation strategy; each
`Exchange` sends one request and awaits its correlated response:

```fsharp
open System
open System.Threading
open SC2APIProtocol   // message types generated by Grpc.Tools over the vendored s2clientprotocol .proto
open FS.GG.Net.Core
open FS.GG.Net.Protobuf
open FS.GG.Net.WebSocket

// SC2's id lives on Request/Response field 97 — an IdEcho makes it the correlator's desync guard.
let idEcho: IdEcho<Request, Response> =
    { Stamp = fun (r: Request) id -> r.Id <- uint32 id; r
      Read  = fun (r: Response) -> uint64 r.Id }

let run (uri: Uri) =
    task {
        let! transport = WebSocketTransport.connectAsync uri WebSocketOptions.defaults CancellationToken.None
        let channel =
            MessageChannel.create
                transport
                (Codec.google Request.Parser)
                (Codec.google Response.Parser)
                (Sequential (Some idEcho))

        let! pong = channel.Exchange(Request(Ping = RequestPing()), CancellationToken.None)
        printfn "ping -> %A" pong.Status
    }
```

### Running against a real SC2 server

There is no live SC2 in CI, so the sample **builds everywhere** but **runs** only against a
real install. It works with the same headless package the
[aiarena](https://github.com/aiarena/aiarena-docker-base) image wraps:

```bash
# 1. Get Blizzard's SC2 Linux headless package (the password IS the EULA acceptance).
wget http://blzdistsc2-a.akamaihd.net/Linux/SC2.4.10.zip
unzip -P iagreetotheeula SC2.4.10.zip -d ~/sc2/
ln -sfn ~/sc2/StarCraftII/Maps ~/sc2/StarCraftII/maps   # SC2 resolves maps under lowercase maps/ on Linux

# 2. Launch the headless server (opens ws://127.0.0.1:5000/sc2api).
cd ~/sc2/StarCraftII/Versions/Base75689
./SC2_x64 -listen 127.0.0.1 -port 5000 -dataDir ~/sc2/StarCraftII/ -tempDir /tmp/sc2/ &

# 3. Run the client (port, then a map path relative to Maps/).
dotnet run --project samples/Sc2Handshake/Sc2Handshake -- 5000 "Ladder2019Season1/CyberForestLE.SC2Map"
```

Expected tail:

```
observation   -> status=InGame   game_loop=0  raw_units=161  observation_payload=46225 bytes
step          -> status=InGame
observation2  -> status=InGame   game_loop advanced 0 -> 112
quit          -> status=Quit
OK — full game handshake over FS.GG.Net ... against a REAL SC2 server.
```

> **proto2 note:** SC2's proto is proto2 — an unset `optional` enum reads back as its
> *first* value, so check `HasError` (not an enum comparison) before trusting
> `ResponseCreateGame.Error`.

## Serving the other side

The `ITransport` seam is symmetric, so a server reuses it. `FS.GG.Net.WebSocket.Server`
accepts a connection and hands it over as an `ITransport`, and `MessageChannel.serve` is the
inverse of the client's `Exchange`: it reads inbound requests, runs a handler, and sends the
id-echoed response. What FS.GG.Net does **not** own is the rest of "server infrastructure" —
auth, TLS, deployment, orchestration — which belongs to the application host.

## Where to go next

- [`README.md`](../README.md) — the component overview and the two-tier seam.
- [ADR-0052](https://github.com/FS-GG/.github/blob/main/docs/adr/0052-onboard-fs-gg-net-transport-component.md) — why FS.GG.Net exists and the decisions behind it.
- [`docs/architecture.md`](https://github.com/FS-GG/.github/blob/main/docs/architecture.md) and the [platform vocabulary (ADR-0020)](https://github.com/FS-GG/.github/blob/main/docs/adr/0020-platform-workspace-component-vocabulary.md) — how the whole platform fits together.

[adr-0039]: https://github.com/FS-GG/.github/blob/main/docs/adr/0039-nuget-org-is-the-read-path.md
