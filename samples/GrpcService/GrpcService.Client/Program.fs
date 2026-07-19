module GrpcService.Client.Program

open System
open ProtoBuf.Grpc
open ProtoBuf.Grpc.Client
open FS.GG.Net.Grpc
open FS.GG.Net.Protobuf
open GrpcService.Contracts

// A third-party client: FS.GG.Net.Grpc opens the channel and projects its lifecycle; FS.GG.Net.Protobuf
// registers the F# code-first contracts; protobuf-net.Grpc turns the channel into a typed IGreeter.

[<EntryPoint>]
let main argv =
    // The sample server is plaintext HTTP/2 (h2c); enable it for the client's handler.
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)
    let address = if argv.Length > 0 then argv[0] else "http://localhost:5000"

    Registration.records Contract.recordTypes
    let channel = GrpcTransport.connect (Uri address)
    let greeter = channel.CreateGrpcService<IGreeter>()

    let run () =
        task {
            let! reply = greeter.SayHello({ Name = "World" }, CallContext.Default)
            printfn "SayHello -> %s" reply.Message

            printfn "CountTo 3:"
            let e = greeter.CountTo({ To = 3 }, CallContext.Default).GetAsyncEnumerator()
            let mutable go = true

            while go do
                let! moved = e.MoveNextAsync()

                if moved then
                    printfn "  tick %d" e.Current.N
                else
                    go <- false

            printfn "channel state via FS.GG.Net.Grpc: %A" (GrpcTransport.stateOf channel)
            return 0
        }

    (run ()).GetAwaiter().GetResult()
