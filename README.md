# FS.GG.Net

A render-independent, domain-neutral **transport** component for the FS-GG platform — a bottom-layer sibling to `FS.GG.Game` and `FS.GG.Audio` that *reaches up to nothing*. It carries protobuf messages over a WebSocket or a gRPC channel, and knows no `.proto` and no game types: the message schemas live in the applications that consume it.

Decision record: **[ADR-0052](https://github.com/FS-GG/.github/blob/main/docs/adr/0052-onboard-fs-gg-net-transport-component.md)**.

## Why it exists

Two first uses share **protobuf serialization** but not transport:

| Use case | Transport | Correlation | Package path |
|---|---|---|---|
| Custom **StarCraft II** client ↔ SC2 headless server | raw protobuf over **WebSocket** (`ws://…/sc2api`) | `Sequential (Some idEcho)` — single in-flight, id-verified (`Request`/`Response` field 97) | `Core` + `WebSocket` + `Protobuf` |
| Custom **Beyond All Reason** client ↔ owned gRPC proxy | **gRPC** | native (grpc-dotnet) | `Core` + `Grpc` |

## The two-tier seam

The abstraction is two tiers, not "WebSocket vs gRPC as sibling transports":

- **`ITransport`** — an ordered duplex channel of *complete application messages*. WebSocket implements it (fragment reassembly is internal). gRPC does **not** — it owns its own framing and sits above this tier.
- **`IMessageChannel<'Req,'Resp>`** — a typed protobuf request/response exchange with a pluggable `Correlation` strategy. The piece with no off-the-shelf .NET equivalent, and the SC2 substrate.

The core unifies only the **`ConnectionState` lifecycle** and (via `FS.GG.Net.Elmish`) the **`Cmd`/`Sub` idiom** — deliberately small, so the version can stay stable.

## Packages (coherent set on `$(FsGgNetVersion)`)

| Package | Responsibility |
|---|---|
| **FS.GG.Net.Core** | Pure, BCL-only seams: `ConnectionState`, `ITransport`, `IMessageCodec`, `IdEcho`, `Correlation`, `IMessageChannel`, and a working `Sequential` correlator. |
| **FS.GG.Net.WebSocket** | `ITransport` over `ClientWebSocket` (client `connectAsync` + `ofSocket` for any open socket) — fragment reassembly, pooled buffers, initial connect-retry. The SC2 substrate. |
| **FS.GG.Net.WebSocket.Server** | Server-side transport: a Kestrel acceptor (`WebSocketServer.start`) that hands each connection to a handler as an `ITransport`. Pair with `MessageChannel.serve` to serve a protobuf-over-WS protocol. |
| **FS.GG.Net.Protobuf** | `IMessageCodec` for Google.Protobuf (`ToByteArray`/`MessageParser` — the reproducible raw path SC2-over-WS needs) and protobuf-net (code-first, owned schemas). Encodes the F#/protobuf-net registration gotchas absorbed from `fsGRPCSkills`. |
| **FS.GG.Net.Grpc** | A thin lifecycle bridge over grpc-dotnet — projects a channel's connectivity onto `ConnectionState`. Does **not** re-abstract gRPC method dispatch. The BAR substrate. |
| **FS.GG.Net.Elmish** | `Net.Cmd.exchange` / `Net.Sub.incoming` over an `IMessageChannel`. Depends on standard Elmish, never on FS.GG.UI. |

## Build & test

```bash
dotnet build          # net10.0, warnings-as-errors, locked/deterministic restore
dotnet test           # FS.GG.Net.Core.Tests (Expecto) — the Sequential correlator + desync guard
```

## Server side

The `ITransport` seam is symmetric, so the server reuses it: `FS.GG.Net.WebSocket.Server` accepts a
connection and hands it over as an `ITransport`, and `MessageChannel.serve` is the inverse of the
client's `Exchange` — it reads inbound requests, runs a handler, and sends the (id-echoed, via
`ServerEcho`) response. What FS.GG.Net does **not** own is the rest of "server infrastructure" — auth,
TLS, deployment, orchestration — which belongs to the application host, not the transport component.

## Still an org-level follow-up

- The **wire-contract registry dimension** (vendored external `.proto`, owned `.proto`, code-first surface) is tracked in ADR-0052; it lands via the SDD-first schema-growth sequence ([SDD#589](https://github.com/FS-GG/FS.GG.SDD/issues/589)).
