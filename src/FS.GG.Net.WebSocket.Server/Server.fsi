namespace FS.GG.Net.WebSocket.Server

open System
open System.Threading.Tasks
open FS.GG.Net.Core
open FS.GG.Net.WebSocket

/// Public contract type exposed by this FS.GG.Net.WebSocket.Server package.
/// A running WebSocket server.
type ServerHandle =
    { /// The actual bound `ws://` address — resolves an ephemeral `:0` port to the real one.
      Uri: Uri
      /// Stop the server.
      StopAsync: unit -> Task }

/// Public contract exposed by this FS.GG.Net.WebSocket.Server package.
[<RequireQualifiedAccess>]
module WebSocketServer =
    /// Host a WebSocket server on `listenOn` (e.g. `ws://127.0.0.1:5000/sc2api`; port `0` picks an
    /// ephemeral port). Each accepted connection is wrapped as an `ITransport` (reusing
    /// FS.GG.Net.WebSocket's fragment reassembly + pooling) and handed to `onConnection`; the
    /// connection stays open until that task returns. Pair it with `MessageChannel.serve` to serve a
    /// protobuf-over-WebSocket protocol.
    val start:
        listenOn: Uri -> options: WebSocketOptions -> onConnection: (ITransport -> Task) -> Task<ServerHandle>
