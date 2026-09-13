namespace FS.GG.Net.Core

open System

type ReconnectTicket =
    { SessionId: string
      ClientId: string
      Token: string }

type DeliveryConfig =
    { MaxQueuedDeltas: int
      ReconnectLifetimeMilliseconds: uint64 }

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
    private
    | DeliveryState of DeliveryConfig * ReconnectTicket * DeliveryStatus * uint64 * uint64 * DeliveryMessage<'delta, 'snapshot> list

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
    let private missing value = String.IsNullOrWhiteSpace value
    let private messageRevision = function
        | DeliveryMessage.Delta(value, _) | DeliveryMessage.Snapshot(value, _) -> value

    let create config ticket initialRevision =
        let issues =
            [ if config.MaxQueuedDeltas < 1 then DeliveryIssue.InvalidMaximumQueue config.MaxQueuedDeltas
              if missing ticket.SessionId then DeliveryIssue.MissingSessionId
              if missing ticket.ClientId then DeliveryIssue.MissingClientId
              if missing ticket.Token then DeliveryIssue.MissingToken ]
        if issues.IsEmpty then
            Ok(DeliveryState(config, ticket, DeliveryStatus.Connected, initialRevision, initialRevision, []))
        else Error issues

    let publish revision delta snapshot state =
        let (DeliveryState(config, ticket, status, current, acknowledged, pending)) = state
        if status = DeliveryStatus.Disposed then Error DeliveryIssue.Disposed
        elif revision <= current then Error(DeliveryIssue.RevisionNotMonotonic(current, revision))
        else
            let deltaMessage = DeliveryMessage.Delta(revision, delta)
            let nextPending, effect =
                if pending.Length < config.MaxQueuedDeltas then
                    pending @ [ deltaMessage ], DeliveryEffect.Enqueued deltaMessage
                else
                    let snapshotMessage = DeliveryMessage.Snapshot(revision, snapshot)
                    [ snapshotMessage ], DeliveryEffect.QueueCoalesced snapshotMessage
            Ok(DeliveryState(config, ticket, status, revision, acknowledged, nextPending), effect)

    let acknowledge revision (state: DeliveryState<'delta, 'snapshot>) : Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue> =
        let (DeliveryState(config, ticket, status, current, acknowledged, pending)) = state
        if status = DeliveryStatus.Disposed then Error DeliveryIssue.Disposed
        elif revision > current then Error(DeliveryIssue.AcknowledgementAhead(current, revision))
        else
            let retained = pending |> List.filter (messageRevision >> (<) revision)
            let nextAcknowledged = max acknowledged revision
            Ok(DeliveryState(config, ticket, status, current, nextAcknowledged, retained), DeliveryEffect.QueueAcknowledged nextAcknowledged)

    let disconnect nowMilliseconds (state: DeliveryState<'delta, 'snapshot>) : Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue> =
        let (DeliveryState(config, ticket, status, current, acknowledged, pending)) = state
        if status = DeliveryStatus.Disposed then Error DeliveryIssue.Disposed
        else
            let expires = nowMilliseconds + config.ReconnectLifetimeMilliseconds
            Ok(DeliveryState(config, ticket, DeliveryStatus.Disconnected expires, current, acknowledged, pending), DeliveryEffect.DisconnectedUntil expires)

    let reconnect ticket nowMilliseconds (state: DeliveryState<'delta, 'snapshot>) : Result<DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot>, DeliveryIssue> =
        let (DeliveryState(config, expected, status, current, acknowledged, pending)) = state
        if status = DeliveryStatus.Disposed then Error DeliveryIssue.Disposed
        elif ticket.SessionId <> expected.SessionId then Error(DeliveryIssue.WrongSession(expected.SessionId, ticket.SessionId))
        elif ticket.ClientId <> expected.ClientId then Error(DeliveryIssue.WrongClient(expected.ClientId, ticket.ClientId))
        elif ticket.Token <> expected.Token then Error DeliveryIssue.TokenMismatch
        else
            match status with
            | DeliveryStatus.Disconnected expires when nowMilliseconds > expires -> Error(DeliveryIssue.ReconnectExpired(expires, nowMilliseconds))
            | _ -> Ok(DeliveryState(config, expected, DeliveryStatus.Connected, current, acknowledged, pending), DeliveryEffect.Reconnected pending)

    let dispose (DeliveryState(config, ticket, _, current, acknowledged, _): DeliveryState<'delta, 'snapshot>) : DeliveryState<'delta, 'snapshot> * DeliveryEffect<'delta, 'snapshot> =
        DeliveryState(config, ticket, DeliveryStatus.Disposed, current, acknowledged, []), DeliveryEffect.Disposed

    let status (DeliveryState(_, _, value, _, _, _)) = value
    let currentRevision (DeliveryState(_, _, _, value, _, _)) = value
    let acknowledgedRevision (DeliveryState(_, _, _, _, value, _)) = value
    let pending (DeliveryState(_, _, _, _, _, value)) = value
