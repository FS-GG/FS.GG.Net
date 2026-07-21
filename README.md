# FS.GG.Net

A render-independent, domain-neutral transport component for .NET apps that need to carry protobuf messages over a WebSocket or a gRPC channel.

## What it can do

- Carry **protobuf request/response exchanges** over a WebSocket, with a pluggable correlation strategy (single-in-flight lockstep, or id-multiplexed).
- Drive a **raw-protobuf-over-WebSocket** protocol such as StarCraft II's `sc2api` — with an id-echo desync guard that turns a lost or misordered reply into an explicit error instead of a stale result.
- Stand up **code-first gRPC** services and clients from plain F# records and an interface — no `.proto` to author or vendor.
- Serve the same typed exchange from the **server side** (a Kestrel WebSocket acceptor), reusing one symmetric `ITransport` seam on both ends.
- Model connection lifecycle uniformly as a single `ConnectionState`, and drive it from **Elmish** via a `Cmd`/`Sub` idiom.

## Acquire

Every `FS.GG.*` package is **public on [nuget.org](https://www.nuget.org) and
restores with no credential** ([ADR-0039][adr-0039]). Add the entry package:

```sh
dotnet add package FS.GG.Net.Core
```

`FS.GG.Net.Core` is the seams; add the pieces your transport needs alongside it —
`FS.GG.Net.WebSocket`, `FS.GG.Net.WebSocket.Server`, `FS.GG.Net.Protobuf`,
`FS.GG.Net.Grpc`, `FS.GG.Net.Elmish`. The full package map is in
[the packages reference](docs/getting-started.md#the-packages).

## Quick start

A code-first gRPC exchange — no `.proto`. Define the contract as F# records plus an
interface, then compose the client from three pieces (`FS.GG.Net.Grpc` opens the
channel, `FS.GG.Net.Protobuf` registers the records, protobuf-net.Grpc types the proxy):

```fsharp
open System
open System.ServiceModel
open System.Threading.Tasks
open ProtoBuf
open ProtoBuf.Grpc
open ProtoBuf.Grpc.Client
open FS.GG.Net.Grpc
open FS.GG.Net.Protobuf

// The wire contract — plain F# records + a service interface, no .proto.
[<ProtoContract>]
type GreetRequest = { [<ProtoMember(1)>] Name: string }
[<ProtoContract>]
type GreetReply = { [<ProtoMember(1)>] Message: string }

[<ServiceContract>]
type IGreeter =
    [<OperationContract>]
    abstract SayHello: request: GreetRequest * context: CallContext -> ValueTask<GreetReply>

// Register the records once, open a channel, get a typed client, call it.
Registration.records [ typeof<GreetRequest>; typeof<GreetReply> ]
let channel = GrpcTransport.connect (Uri "http://localhost:5000")
let greeter = channel.CreateGrpcService<IGreeter>()

let reply = greeter.SayHello({ Name = "World" }, CallContext.Default).Result
printfn "SayHello -> %s" reply.Message
printfn "channel state via FS.GG.Net.Grpc: %A" (GrpcTransport.stateOf channel)
```

Against the matching server you should see:

```
SayHello -> Hello, World!
channel state via FS.GG.Net.Grpc: Connected
```

The complete, runnable server + client (and the SC2-over-WebSocket exchange) are the
two walkthroughs in [the getting-started guide](docs/getting-started.md).

## Go deeper

- [`docs/getting-started.md`](docs/getting-started.md) — install, then run either exchange (code-first gRPC in-process, or the SC2 WebSocket handshake) end to end.
- Sibling bottom-layer components: [FS.GG.Game](https://github.com/FS-GG/FS.GG.Game) and [FS.GG.Audio](https://github.com/FS-GG/FS.GG.Audio).

## Where this sits

FS.GG.Net is a bottom-layer transport sibling to `FS.GG.Game` and `FS.GG.Audio` that reaches up to nothing — it knows no `.proto` and no game types. See the
[platform vocabulary (ADR-0020)](https://github.com/FS-GG/.github/blob/main/docs/adr/0020-platform-workspace-component-vocabulary.md)
and [`docs/architecture.md`](https://github.com/FS-GG/.github/blob/main/docs/architecture.md)
for how the whole platform fits together.

---

## The two-tier seam

The abstraction is two tiers, not "WebSocket vs gRPC as sibling transports":

- **`ITransport`** — an ordered duplex channel of *complete application messages*. WebSocket implements it (fragment reassembly is internal). gRPC does **not** — it owns its own framing and sits above this tier.
- **`IMessageChannel<'Req,'Resp>`** — a typed protobuf request/response exchange with a pluggable `Correlation` strategy. The piece with no off-the-shelf .NET equivalent, and the SC2 substrate.

The core unifies only the **`ConnectionState` lifecycle** and (via `FS.GG.Net.Elmish`) the **`Cmd`/`Sub` idiom** — deliberately small, so the version can stay stable. The per-package responsibilities are the table in [the getting-started guide](docs/getting-started.md#the-packages).

## Building & contributing

```bash
dotnet build          # net10.0, warnings-as-errors, locked/deterministic restore
dotnet test           # FS.GG.Net.Core.Tests (Expecto) — the Sequential correlator + desync guard
```

The `ITransport` seam is symmetric, so the server reuses it: `FS.GG.Net.WebSocket.Server`
accepts a connection and hands it over as an `ITransport`, and `MessageChannel.serve` is the
inverse of the client's `Exchange`. What FS.GG.Net does **not** own is the rest of "server
infrastructure" — auth, TLS, deployment, orchestration — which belongs to the application host,
not the transport component.

Decision record: **[ADR-0052](https://github.com/FS-GG/.github/blob/main/docs/adr/0052-onboard-fs-gg-net-transport-component.md)**.
The wire-contract registry dimension (vendored external `.proto`, owned `.proto`, code-first
surface) is tracked there and lands via the SDD-first schema-growth sequence
([SDD#589](https://github.com/FS-GG/FS.GG.SDD/issues/589)).

[adr-0039]: https://github.com/FS-GG/.github/blob/main/docs/adr/0039-nuget-org-is-the-read-path.md
