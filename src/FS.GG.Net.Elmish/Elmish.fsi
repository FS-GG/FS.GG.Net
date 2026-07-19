namespace FS.GG.Net.Elmish

open Elmish
open FS.GG.Net.Core

/// Public contract exposed by this FS.GG.Net.Elmish package.
/// Elmish Cmd/Sub bridge over an FS.GG.Net message channel — one idiom for both channel kinds
/// (mirrors FS.GG.Audio.Elmish).
[<RequireQualifiedAccess>]
module Net =

    module Cmd =
        /// Dispatch a request over `channel`; map the correlated response to a message, or a failure
        /// (transport fault, `CorrelationMismatch`, cancellation) via `onError`.
        val exchange:
            channel: IMessageChannel<'Req, 'Resp> ->
            request: 'Req ->
            onResponse: ('Resp -> 'Msg) ->
            onError: (exn -> 'Msg) ->
                Cmd<'Msg>

    module Sub =
        /// Subscribe to the channel's unsolicited incoming messages under `subId`, mapping each to a
        /// message. Disposing the subscription stops the read loop.
        val incoming:
            channel: IMessageChannel<'Req, 'Resp> -> subId: string -> map: ('Resp -> 'Msg) -> Sub<'Msg>
