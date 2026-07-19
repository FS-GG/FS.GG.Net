module GrpcService.Server.Program

open System.Threading.Tasks

[<EntryPoint>]
let main _argv =
    let running = (GrpcService.Server.Host.start ()).GetAwaiter().GetResult()
    printfn "GrpcService server listening on %s (Ctrl+C to stop)" running.Address
    Task.Delay(-1).GetAwaiter().GetResult()
    0
