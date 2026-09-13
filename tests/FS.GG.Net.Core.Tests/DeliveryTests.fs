namespace FS.GG.Net.Core.Tests

open Expecto
open FS.GG.Net.Core

module DeliveryTests =
    let private ticket = {SessionId="arena";ClientId="a";Token="secret"}
    let private create maxQueue : DeliveryState<string,string> =
        Delivery.create {MaxQueuedDeltas=maxQueue;ReconnectLifetimeMilliseconds=50UL} ticket 0UL
        |> Result.defaultWith (fun issue -> failtestf "%A" issue)
    let private success = function Ok value -> value | Error issue -> failtestf "%A" issue

    [<Tests>]
    let tests = testList "bounded delivery and reconnect" [
        testCase "delayed acknowledgement cannot discard a newer delta" <| fun _ ->
            let one,_=create 3 |> Delivery.publish 1UL "d1" "s1" |> success
            let two,_=one |> Delivery.publish 2UL "d2" "s2" |> success
            let acknowledged,_=two |> Delivery.acknowledge 1UL |> success
            Expect.equal (Delivery.pending acknowledged) [DeliveryMessage.Delta(2UL,"d2")] "only the acknowledged prefix is removed"
            Expect.equal (Delivery.acknowledge 3UL acknowledged) (Error(DeliveryIssue.AcknowledgementAhead(2UL,3UL))) "client acknowledgement cannot lead authority"

        testCase "queue saturation coalesces to the current full snapshot" <| fun _ ->
            let one,_=create 2 |> Delivery.publish 1UL "d1" "s1" |> success
            let two,_=one |> Delivery.publish 2UL "d2" "s2" |> success
            let three,effect=two |> Delivery.publish 3UL "d3" "s3" |> success
            Expect.equal (Delivery.pending three) [DeliveryMessage.Snapshot(3UL,"s3")] "bounded history becomes one authoritative snapshot"
            Expect.equal effect (DeliveryEffect.QueueCoalesced(DeliveryMessage.Snapshot(3UL,"s3"))) "backpressure is observable"

        testCase "reconnect validates identity token and expiry" <| fun _ ->
            let disconnected,_=create 2 |> Delivery.disconnect 100UL |> success
            let connected,effect=disconnected |> Delivery.reconnect ticket 149UL |> success
            Expect.equal (Delivery.status connected) DeliveryStatus.Connected "valid reconnect resumes delivery"
            Expect.equal effect (DeliveryEffect.Reconnected []) "the retained suffix is explicit"
            Expect.equal (disconnected |> Delivery.reconnect {ticket with Token="wrong"} 120UL) (Error DeliveryIssue.TokenMismatch) "wrong token is refused"
            Expect.equal (disconnected |> Delivery.reconnect ticket 151UL) (Error(DeliveryIssue.ReconnectExpired(150UL,151UL))) "expired reconnect is refused"

        testCase "dispose clears pending work and makes later operations inert" <| fun _ ->
            let queued,_=create 2 |> Delivery.publish 1UL "d1" "s1" |> success
            let disposed,effect=Delivery.dispose queued
            Expect.equal effect DeliveryEffect.Disposed "disposal is visible"
            Expect.equal (Delivery.pending disposed) [] "pending work is released"
            Expect.equal (disposed |> Delivery.publish 2UL "d2" "s2") (Error DeliveryIssue.Disposed) "disposed delivery refuses publication"
            let again,effectAgain=Delivery.dispose disposed
            Expect.equal again disposed "disposal is idempotent"
            Expect.equal effectAgain DeliveryEffect.Disposed "idempotent disposal remains observable"
    ]
