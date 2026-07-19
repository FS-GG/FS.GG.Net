namespace GrpcService.Server

open System.Collections.Generic
open System.Threading.Channels
open System.Threading.Tasks
open GrpcService.Contracts

/// The code-first service implementation.
type GreeterService() =
    interface IGreeter with
        member _.SayHello(request, _context) =
            ValueTask<GreetReply>({ Message = $"Hello, {request.Name}!" })

        member _.CountTo(request, _context) : IAsyncEnumerable<Tick> =
            // Server-streaming: emit 1..To. Produced into a channel and returned as its reader's
            // async stream — no extra dependency needed for the IAsyncEnumerable.
            let ch = Channel.CreateUnbounded<Tick>()

            for i in 1 .. request.To do
                ch.Writer.TryWrite { N = i } |> ignore

            ch.Writer.Complete()
            ch.Reader.ReadAllAsync()
