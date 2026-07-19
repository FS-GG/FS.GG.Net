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
| **FS.GG.Net.WebSocket** | Client-only `ITransport` over `ClientWebSocket` — fragment reassembly, pooled buffers, initial connect-retry. The SC2 substrate. |
| **FS.GG.Net.Protobuf** | `IMessageCodec` for Google.Protobuf (`ToByteArray`/`MessageParser` — the reproducible raw path SC2-over-WS needs) and protobuf-net (code-first, owned schemas). Encodes the F#/protobuf-net registration gotchas absorbed from `fsGRPCSkills`. |
| **FS.GG.Net.Grpc** | A thin lifecycle bridge over grpc-dotnet — projects a channel's connectivity onto `ConnectionState`. Does **not** re-abstract gRPC method dispatch. The BAR substrate. |
| **FS.GG.Net.Elmish** | `Net.Cmd.exchange` / `Net.Sub.incoming` over an `IMessageChannel`. Depends on standard Elmish, never on FS.GG.UI. |

## Build & test

```bash
dotnet build          # net10.0, warnings-as-errors, locked/deterministic restore
dotnet test           # FS.GG.Net.Core.Tests (Expecto) — the Sequential correlator + desync guard
```

## Not in v1 (deliberately)

- **No WebSocket server host** — both first uses are clients (the SC2 game *is* the server; the BAR proxy is a gRPC server). A `FS.GG.Net.WebSocket.Server` becomes an additive package when a real use case appears.
- The **wire-contract registry dimension** (vendored external `.proto`, owned `.proto`, code-first surface) is an org-level change tracked in ADR-0052; it lands via the SDD-first schema-growth sequence.
