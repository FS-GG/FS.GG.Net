module Sc2Handshake.Program

open System
open System.Threading
open System.Threading.Tasks
open SC2APIProtocol
open FS.GG.Net.Core
open FS.GG.Net.Protobuf
open FS.GG.Net.WebSocket

// A full game handshake over FS.GG.Net against a REAL SC2 headless server:
// Ping -> CreateGame -> JoinGame(raw) -> Observation -> Step -> Observation -> Leave -> Quit.
// Every exchange is a correlated protobuf Request/Response over the WebSocket; the Observation
// payloads are large (raw units + map state), so this also exercises fragment reassembly and the
// Sequential id-verified correlator across a real multi-step conversation.

[<EntryPoint>]
let main argv =
    let port = if argv.Length > 0 then int argv[0] else 5000
    let mapPath = if argv.Length > 1 then argv[1] else "Ladder2019Season1/CyberForestLE.SC2Map"

    let run () =
        task {
            let uri = Uri(sprintf "ws://127.0.0.1:%d/sc2api" port)
            printfn "Connecting to %O ..." uri

            let opts =
                { WebSocketOptions.defaults with
                    ConnectRetries = 120
                    ConnectBackoff = TimeSpan.FromMilliseconds 500.0 }

            let! transport = WebSocketTransport.connectAsync uri opts CancellationToken.None

            let idEcho: IdEcho<Request, Response> =
                { Stamp =
                    fun (r: Request) id ->
                        r.Id <- uint32 id
                        r
                  Read = fun (r: Response) -> uint64 r.Id }

            let channel =
                MessageChannel.create
                    transport
                    (Codec.google Request.Parser)
                    (Codec.google Response.Parser)
                    (Sequential(Some idEcho))

            let exchange (label: string) (req: Request) : Task<Response> =
                task {
                    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 90.0)
                    let! (resp: Response) = channel.Exchange(req, cts.Token)
                    let errs = resp.Error |> Seq.toList

                    printfn
                        "  %-13s -> status=%A%s"
                        label
                        resp.Status
                        (if errs.IsEmpty then "" else sprintf "  ERROR=%A" errs)

                    return resp
                }

            printfn "── Ping"
            let! ping = exchange "ping" (Request(Ping = RequestPing()))
            printfn "    game_version=%s base_build=%d" ping.Ping.GameVersion ping.Ping.BaseBuild

            printfn "── CreateGame (%s)" mapPath
            let cg = RequestCreateGame(LocalMap = LocalMap(MapPath = mapPath))
            cg.PlayerSetup.Add(PlayerSetup(Type = PlayerType.Participant))
            cg.PlayerSetup.Add(PlayerSetup(Type = PlayerType.Computer, Race = Race.Random, Difficulty = Difficulty.VeryEasy))
            cg.Realtime <- false
            let! cgResp = exchange "create_game" (Request(CreateGame = cg))
            if cgResp.CreateGame.HasError then
                failwithf "CreateGame rejected: %A (%s)" cgResp.CreateGame.Error cgResp.CreateGame.ErrorDetails
            printfn "    created (no error)"

            printfn "── JoinGame (raw interface, Terran)"
            let jg = RequestJoinGame(Race = Race.Terran)
            jg.Options <- InterfaceOptions(Raw = true, Score = true)
            let! joined = exchange "join_game" (Request(JoinGame = jg))
            printfn "    player_id=%d" joined.JoinGame.PlayerId

            printfn "── Observation (raw)"
            let! obs1 = exchange "observation" (Request(Observation = RequestObservation()))
            let loop0 = obs1.Observation.Observation.GameLoop
            let units = obs1.Observation.Observation.RawData.Units.Count
            let payloadBytes = (obs1.Observation :> Google.Protobuf.IMessage).CalculateSize()
            printfn "    game_loop=%d  raw_units=%d  observation_payload=%d bytes (reassembled)" loop0 units payloadBytes

            printfn "── Step x112"
            let! _ = exchange "step" (Request(Step = RequestStep(Count = 112u)))
            let! obs2 = exchange "observation2" (Request(Observation = RequestObservation()))
            printfn "    game_loop advanced %d -> %d" loop0 obs2.Observation.Observation.GameLoop

            printfn "── LeaveGame + Quit"
            let! _ = exchange "leave_game" (Request(LeaveGame = RequestLeaveGame()))
            let! _ = exchange "quit" (Request(Quit = RequestQuit()))

            do! (channel :> IAsyncDisposable).DisposeAsync()

            printfn
                "OK — full game handshake over FS.GG.Net: created a game, joined with the raw interface, read a %d-byte observation, stepped the sim, and quit — all correlated protobuf over WebSocket against a REAL SC2 server."
                payloadBytes

            return 0
        }

    (run ()).GetAwaiter().GetResult()
