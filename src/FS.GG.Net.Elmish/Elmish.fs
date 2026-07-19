namespace FS.GG.Net.Elmish

open System
open System.Threading
open Elmish
open FS.GG.Net.Core

[<RequireQualifiedAccess>]
module Net =

    module Cmd =
        let exchange
            (channel: IMessageChannel<'Req, 'Resp>)
            (request: 'Req)
            (onResponse: 'Resp -> 'Msg)
            (onError: exn -> 'Msg)
            : Cmd<'Msg> =
            Cmd.OfTask.either (fun () -> channel.Exchange(request, CancellationToken.None)) () onResponse onError

    module Sub =
        let incoming (channel: IMessageChannel<'Req, 'Resp>) (subId: string) (map: 'Resp -> 'Msg) : Sub<'Msg> =
            let start (dispatch: Dispatch<'Msg>) : IDisposable =
                let cts = new CancellationTokenSource()

                let work =
                    task {
                        try
                            let e = channel.Incoming.GetAsyncEnumerator(cts.Token)
                            let mutable go = true

                            while go do
                                let! moved = e.MoveNextAsync()

                                if not moved then
                                    go <- false
                                else
                                    dispatch (map e.Current)
                        with _ ->
                            ()
                    }

                work |> ignore

                { new IDisposable with
                    member _.Dispose() =
                        cts.Cancel()
                        cts.Dispose() }

            [ [ subId ], start ]
