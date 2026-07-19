namespace FS.GG.Net.Core

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// Public contract type exposed by this FS.GG.Net.Core package.
/// The lifecycle state of a transport or channel. `Faulted` carries the terminal error.
type ConnectionState =
    | Disconnected
    | Connecting
    | Connected
    | Closing
    | Faulted of error: exn

/// Public contract type exposed by this FS.GG.Net.Core package.
/// Tier-1 seam: an ordered duplex channel of COMPLETE application messages. The unit is a whole
/// message, not a wire frame — a WebSocket implementation reassembles protocol-level continuation
/// frames internally before yielding one. gRPC does NOT implement this; it owns its own framing and
/// method dispatch and sits ABOVE this tier (see FS.GG.Net.Grpc). Disposing the transport closes it.
type ITransport =
    inherit IAsyncDisposable
    /// The current lifecycle state.
    abstract member State : ConnectionState
    /// Send one complete application message. Honours cancellation.
    abstract member Send : message: ReadOnlyMemory<byte> * ct: CancellationToken -> ValueTask
    /// The stream of complete inbound application messages, in arrival order.
    abstract member Receive : IAsyncEnumerable<ReadOnlyMemory<byte>>

/// Public contract type exposed by this FS.GG.Net.Core package.
/// Turns a typed message to and from the bytes a transport carries. One implementation per protobuf
/// stack (Google.Protobuf, protobuf-net) lives in FS.GG.Net.Protobuf — the core stays serializer-agnostic.
type IMessageCodec<'T> =
    abstract member Encode : value: 'T -> ReadOnlyMemory<byte>
    abstract member Decode : bytes: ReadOnlyMemory<byte> -> 'T

/// Public contract type exposed by this FS.GG.Net.Core package.
/// An optional id-echo the correlator uses to bind a response to its request. SC2 exposes one:
/// `Request.id` and `Response.id` are both proto field 97. When present, a `Sequential` channel
/// stamps a monotonic id and asserts the response echoes it — a cheap desync guard.
type IdEcho<'Req, 'Resp> =
    { /// Return `request` carrying the given correlation id.
      Stamp: 'Req -> uint64 -> 'Req
      /// Read the echoed correlation id off a response.
      Read: 'Resp -> uint64 }

/// Public contract type exposed by this FS.GG.Net.Core package.
/// How responses are matched to requests on a message channel. The two axes — matching (by order vs
/// by id) and concurrency (single-in-flight vs pipelined) — collapse to the two sane combinations.
type Correlation<'Req, 'Resp> =
    /// One request in flight; the next response is matched by arrival order. With an `IdEcho` the id
    /// is verified as a desync guard; `None` fits raw protobuf-over-WebSocket protocols that carry no
    /// id field. StarCraft II uses `Sequential (Some ...)`.
    | Sequential of idEcho: IdEcho<'Req, 'Resp> option
    /// Many requests may be in flight; each response is matched by its echoed id (required).
    | Multiplexed of idEcho: IdEcho<'Req, 'Resp>

/// Public contract type exposed by this FS.GG.Net.Core package.
/// Tier-2 seam: a typed protobuf request/response exchange over a transport, with a pluggable
/// correlation strategy. This is the piece with no off-the-shelf .NET equivalent, and the SC2
/// substrate — SC2 fits `IMessageChannel<Request, Response>` exactly, one oneof-envelope each way.
type IMessageChannel<'Req, 'Resp> =
    inherit IAsyncDisposable
    /// The current lifecycle state (delegated to the underlying transport).
    abstract member State: ConnectionState
    /// Send a request and await its correlated response. Honours cancellation.
    abstract member Exchange: request: 'Req * ct: CancellationToken -> Task<'Resp>
    /// Unsolicited, server-initiated messages, if the protocol has any (empty for SC2).
    abstract member Incoming: IAsyncEnumerable<'Resp>

/// Public contract exposed by this FS.GG.Net.Core package.
/// Raised when a response's echoed correlation id does not match the outstanding request — a
/// detected desync rather than silently returning a stale response to the caller.
exception CorrelationMismatch of expected: uint64 * actual: uint64

/// Public contract exposed by this FS.GG.Net.Core package.
[<RequireQualifiedAccess>]
module MessageChannel =
    /// Build a message channel over `transport`, using the given codecs and correlation strategy.
    /// The channel owns the transport's receive loop; disposing the channel disposes the transport.
    /// v1 implements `Sequential`; `Multiplexed` is reserved (raises until implemented — ADR-0052).
    val create:
        transport: ITransport ->
        requestCodec: IMessageCodec<'Req> ->
        responseCodec: IMessageCodec<'Resp> ->
        correlation: Correlation<'Req, 'Resp> ->
            IMessageChannel<'Req, 'Resp>
