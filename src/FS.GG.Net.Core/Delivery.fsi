namespace FS.GG.Net.Core

/// Opaque reconnect identity issued by the owning server session.
type ReconnectTicket =
    {
        SessionId: string
        ClientId: string
        Token: string
    }

/// Bounds retained delivery and reconnect lifetime.
type DeliveryConfig =
    {
        MaxQueuedDeltas: int
        ReconnectLifetimeMilliseconds: uint64
    }

[<RequireQualifiedAccess>]
type DeliveryMessage<'delta, 'snapshot> =
    | Delta of revision: uint64 * value: 'delta
    | Snapshot of revision: uint64 * value: 'snapshot

[<RequireQualifiedAccess>]
type DeliveryStatus =
    | Connected
    | Disconnected of expiresAtMilliseconds: uint64
    | Disposed

type DeliveryState<'delta, 'snapshot> =
    private | DeliveryState of
        DeliveryConfig *
        ReconnectTicket *
        DeliveryStatus *
        uint64 *
        uint64 *
        DeliveryMessage<'delta, 'snapshot> list

[<RequireQualifiedAccess>]
type DeliveryIssue =
    | InvalidMaximumQueue of int
    | MissingSessionId
    | MissingClientId
    | MissingToken
    | WrongSession of expected: string * actual: string
    | WrongClient of expected: string * actual: string
    | TokenMismatch
    | ReconnectExpired of expiresAtMilliseconds: uint64 * nowMilliseconds: uint64
    | RevisionNotMonotonic of current: uint64 * candidate: uint64
    | AcknowledgementAhead of current: uint64 * candidate: uint64
    | Disposed

[<RequireQualifiedAccess>]
type DeliveryEffect<'delta, 'snapshot> =
    | Enqueued of DeliveryMessage<'delta, 'snapshot>
    | QueueCoalesced of DeliveryMessage<'delta, 'snapshot>
    | Reconnected of DeliveryMessage<'delta, 'snapshot> list
    | QueueAcknowledged of throughRevision: uint64
    | DisconnectedUntil of expiresAtMilliseconds: uint64
    | Disposed

[<RequireQualifiedAccess>]
module Delivery =
    val create:
        config: DeliveryConfig ->
        ticket: ReconnectTicket ->
        initialRevision: uint64 ->
            Result<DeliveryState<'delta, 'snapshot>, DeliveryIssue list>

    val publish:
        revision: uint64 ->
        delta: 'delta ->
        snapshot: 'snapshot ->
        state: DeliveryState<'delta, 'snapshot> ->
            Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue>

    val acknowledge:
        revision: uint64 ->
        state: DeliveryState<'delta, 'snapshot> ->
            Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue>

    val disconnect:
        nowMilliseconds: uint64 ->
        state: DeliveryState<'delta, 'snapshot> ->
            Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue>

    val reconnect:
        ticket: ReconnectTicket ->
        nowMilliseconds: uint64 ->
        state: DeliveryState<'delta, 'snapshot> ->
            Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue>

    val dispose:
        state: DeliveryState<'delta, 'snapshot> -> DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>

    val status: state: DeliveryState<'delta, 'snapshot> -> DeliveryStatus
    val currentRevision: state: DeliveryState<'delta, 'snapshot> -> uint64
    val acknowledgedRevision: state: DeliveryState<'delta, 'snapshot> -> uint64
    val pending: state: DeliveryState<'delta, 'snapshot> -> DeliveryMessage<'delta, 'snapshot> list
