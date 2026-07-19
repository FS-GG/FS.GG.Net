# Sample: SC2 handshake (the v1 vertical slice)

The vertical slice ADR-0052 names as the evidence the seams are right before freezing `0.1.0`:
`Core` + `WebSocket` + `Protobuf` driving a minimal StarCraft II handshake against a **real headless
server** — connect → `Ping` → `CreateGame`/`JoinGame` → step one `Observation`.

This sample is a **recipe**, not a compiled project: it needs the Blizzard-owned `s2clientprotocol`
`.proto` (vendored + generated into an app repo, per the schema-lives-in-the-app boundary), which is
intentionally *not* part of the stable `FS.GG.Net` core.

## Shape

```fsharp
open System
open System.Threading
open SC2APIProtocol            // generated from Blizzard/s2client-proto (Google.Protobuf / Grpc.Tools)
open FS.GG.Net.Core
open FS.GG.Net.Protobuf
open FS.GG.Net.WebSocket

// SC2's Request/Response both carry `id` (proto field 97) — the desync guard.
let idEcho : IdEcho<Request, Response> =
    { Stamp = fun (r: Request) id -> r.Id <- uint32 id; r     // Google.Protobuf types are mutable
      Read  = fun (r: Response) -> uint64 r.Id }

let run (port: int) = task {
    // The game boots its process, THEN listens — connect-retry handles the gap.
    let uri = Uri(sprintf "ws://127.0.0.1:%d/sc2api" port)
    let! transport = WebSocketTransport.connectAsync uri WebSocketOptions.defaults CancellationToken.None

    let channel =
        MessageChannel.create
            transport
            (Codec.google Request.Parser)      // reproducible raw-bytes path — no gRPC channel
            (Codec.google Response.Parser)
            (Sequential (Some idEcho))          // single in-flight, id-verified

    // 1. Ping
    let! pong = channel.Exchange(Request(Ping = RequestPing()), CancellationToken.None)
    printfn "SC2 %s (proto %d)" pong.Ping.GameVersion pong.Ping.DataVersion

    // 2. CreateGame / JoinGame / RequestObservation follow the same Exchange shape.
    //    Response.Status/Error are payload the app maps — the core never sees them.

    do! (channel :> IAsyncDisposable).DisposeAsync()
}
```

## Why it proves the design

- Exercises **the entire net-new stack** — WebSocket transport, protobuf codec, `Sequential`
  correlation — end to end, without touching gRPC.
- The mutable-`Id` `Stamp`/`Read` is exactly why SC2 uses **Google.Protobuf** (mutable C# interop
  types, rock-solid raw API) rather than FsGrpc for the external schema (ADR-0052 §4).
- `Response.Status`/`Error` staying in the app confirms the schema-lives-in-the-app boundary holds.
