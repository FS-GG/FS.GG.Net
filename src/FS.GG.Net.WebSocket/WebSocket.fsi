namespace FS.GG.Net.WebSocket

open System
open System.Net.WebSockets
open System.Threading
open System.Threading.Tasks
open FS.GG.Net.Core

/// Public contract type exposed by this FS.GG.Net.WebSocket package.
/// Options for the client WebSocket transport.
type WebSocketOptions =
    { /// Attempts to establish the INITIAL connection before giving up. The SC2 game boots its
      /// process and only then starts listening, so the first connect must be retried.
      ConnectRetries: int
      /// Delay between initial connect attempts.
      ConnectBackoff: TimeSpan
      /// Size of each pooled receive buffer read; a message larger than this is reassembled across
      /// reads (SC2 raw observations are multi-MB), so this is a read granularity, not a max size. }
      ReceiveBufferSize: int }

/// Public contract exposed by this FS.GG.Net.WebSocket package.
[<RequireQualifiedAccess>]
module WebSocketOptions =
    /// Defaults tuned for a local SC2 headless server: 40 retries x 250ms (~10s to come up),
    /// 64 KiB read granularity.
    val defaults: WebSocketOptions

/// Public contract exposed by this FS.GG.Net.WebSocket package.
[<RequireQualifiedAccess>]
module WebSocketTransport =
    /// Wrap an already-open WebSocket — client-connected or server-accepted — as an ITransport,
    /// reusing the same fragment reassembly and pooled receive buffer. The server package accepts a
    /// connection and hands the socket here.
    val ofSocket: socket: WebSocket -> options: WebSocketOptions -> ITransport

    /// Connect a client WebSocket transport to `uri` (e.g. ws://127.0.0.1:5000/sc2api), retrying the
    /// initial connect per `options`. The returned transport's `Receive` yields COMPLETE application
    /// messages (continuation frames reassembled); `Send` writes one binary message. Disposing closes
    /// the socket.
    val connectAsync: uri: Uri -> options: WebSocketOptions -> ct: CancellationToken -> Task<ITransport>
