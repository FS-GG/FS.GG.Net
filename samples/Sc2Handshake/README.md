# Sample: SC2 handshake

A **real, runnable** StarCraft II client built on FS.GG.Net — the worked example behind ADR-0052. It
drives a full game handshake against Blizzard's headless SC2 server entirely over the FS.GG.Net stack:

```
Ping → CreateGame → JoinGame(raw) → Observation → Step → Observation → LeaveGame → Quit
```

Every step is a correlated protobuf `Request`/`Response` over a WebSocket — `WebSocketTransport` +
`Codec.google` + `MessageChannel` with `Sequential (Some idEcho)` (SC2's `Request`/`Response` both
carry `id`, proto field 97, so the id-verified correlator's desync guard is live).

## Layout

| Path | What |
|---|---|
| `proto/s2clientprotocol/*.proto` | The vendored SC2 protocol (MIT, Blizzard — see `proto/NOTICE.md`) |
| `Sc2.Protocol/` | C# lib: `Grpc.Tools` generates `SC2APIProtocol.*` message types (no gRPC services — SC2 is raw protobuf over WS) |
| `Sc2Handshake/` | The F# client: the handshake above, in ~90 lines over FS.GG.Net |

It **builds anywhere** (the CI gate builds it) — but it can only **run** against a real SC2 install.

## Running it against a real SC2 server

There is no live SC2 in CI, so this is a local recipe. It works with the same headless package the
[aiarena](https://github.com/aiarena/aiarena-docker-base) image wraps:

```bash
# 1. Get Blizzard's SC2 Linux headless package (the password IS the EULA acceptance).
wget http://blzdistsc2-a.akamaihd.net/Linux/SC2.4.10.zip
unzip -P iagreetotheeula SC2.4.10.zip -d ~/sc2/
# SC2 resolves maps under a lowercase `maps/` on Linux:
ln -sfn ~/sc2/StarCraftII/Maps ~/sc2/StarCraftII/maps

# 2. Launch the headless server (it opens ws://127.0.0.1:5000/sc2api).
cd ~/sc2/StarCraftII/Versions/Base75689
./SC2_x64 -listen 127.0.0.1 -port 5000 -dataDir ~/sc2/StarCraftII/ -tempDir /tmp/sc2/ &

# 3. Run the client (port, then a map path relative to Maps/).
dotnet run --project Sc2Handshake -- 5000 "Ladder2019Season1/CyberForestLE.SC2Map"
```

Expected tail:

```
observation   -> status=InGame   game_loop=0  raw_units=161  observation_payload=46225 bytes
step          -> status=InGame
observation2  -> status=InGame   game_loop advanced 0 -> 112
quit          -> status=Quit
OK — full game handshake over FS.GG.Net ... against a REAL SC2 server.
```

## Notes for a fuller client

- SC2's proto is **proto2** — an unset `optional` enum reads back as its *first* value, so check
  `HasError` (not an enum comparison) before trusting `ResponseCreateGame.Error`.
- `Sequential (Some idEcho)` is right for SC2: it is strictly lockstep, so one request is in flight at
  a time, and the echoed `id` turns a lost/misordered response into a `CorrelationMismatch` instead of
  a silently stale observation.
