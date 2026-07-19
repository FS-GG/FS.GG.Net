module FS.GG.Net.IntegrationTests.Tests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Expecto
open Google.Protobuf.WellKnownTypes
open ProtoBuf
open FS.GG.Net.Core
open FS.GG.Net.Protobuf
open FS.GG.Net.WebSocket
open FS.GG.Net.Grpc
open FS.GG.Net.Elmish

let private unwrap (ex: exn) =
    match ex with
    | :? AggregateException as agg -> agg.Flatten().InnerException
    | _ -> ex

/// Read one message off an ITransport's / channel's inbound stream.
let private firstOf (source: Collections.Generic.IAsyncEnumerable<'a>) : Task<'a> =
    task {
        let e = source.GetAsyncEnumerator(CancellationToken.None)
        let! _ = e.MoveNextAsync()
        return e.Current
    }

// A protobuf-net (code-first) F# record exercising the documented gotchas: option, array, Dictionary.
[<ProtoContract>]
type Sample =
    { [<ProtoMember(1)>] Name: string
      [<ProtoMember(2)>] Tags: string[]
      [<ProtoMember(3)>] Note: string option
      [<ProtoMember(4)>] Meta: Dictionary<string, string> }

// A fake channel for the Elmish bridge test.
let private fakeChannel (reply: 'Resp) : IMessageChannel<'Req, 'Resp> =
    let empty = Channel.CreateUnbounded<'Resp>()
    empty.Writer.Complete()

    { new IMessageChannel<'Req, 'Resp> with
        member _.State = Connected
        member _.Exchange(_req, _ct) = Task.FromResult reply
        member _.Incoming = empty.Reader.ReadAllAsync()
        member _.DisposeAsync() = ValueTask() }

type Msg =
    | Got of StringValue
    | Failed of exn

[<Tests>]
let tests =
    testList
        "FS.GG.Net integration"
        [ testCaseAsync "WebSocket transport round-trips a small message"
          <| async {
              let! server = EchoServer.start () |> Async.AwaitTask
              let! transport =
                  WebSocketTransport.connectAsync server.Uri WebSocketOptions.defaults CancellationToken.None
                  |> Async.AwaitTask

              do!
                  transport.Send(ReadOnlyMemory(Text.Encoding.UTF8.GetBytes "hello"), CancellationToken.None).AsTask()
                  |> Async.AwaitTask

              let! echoed = firstOf transport.Receive |> Async.AwaitTask
              Expect.equal (Text.Encoding.UTF8.GetString(echoed.ToArray())) "hello" "echoed bytes match"
              do! transport.DisposeAsync().AsTask() |> Async.AwaitTask
              do! server.Stop () |> Async.AwaitTask
          }

          testCaseAsync "WebSocket reassembles a message larger than the receive buffer"
          <| async {
              let! server = EchoServer.start () |> Async.AwaitTask
              let opts = { WebSocketOptions.defaults with ReceiveBufferSize = 8 * 1024 }

              let! transport =
                  WebSocketTransport.connectAsync server.Uri opts CancellationToken.None
                  |> Async.AwaitTask

              let big = Array.init (256 * 1024) (fun i -> byte (i % 251))
              do! transport.Send(ReadOnlyMemory big, CancellationToken.None).AsTask() |> Async.AwaitTask
              let! got = firstOf transport.Receive |> Async.AwaitTask
              Expect.equal (got.ToArray()) big "large payload reassembled intact across frames"
              do! transport.DisposeAsync().AsTask() |> Async.AwaitTask
              do! server.Stop () |> Async.AwaitTask
          }

          testCaseAsync "end-to-end: protobuf over a real WebSocket via IMessageChannel"
          <| async {
              // The closest proxy to the SC2 vertical slice without a live game: real WS transport +
              // real Google.Protobuf codec + the Sequential Core channel, request/response over a socket.
              let! server = EchoServer.start () |> Async.AwaitTask

              let! transport =
                  WebSocketTransport.connectAsync server.Uri WebSocketOptions.defaults CancellationToken.None
                  |> Async.AwaitTask

              let codec = Codec.google StringValue.Parser
              let channel = MessageChannel.create transport codec codec (Sequential None)
              let! resp = channel.Exchange(StringValue(Value = "ping"), CancellationToken.None) |> Async.AwaitTask
              Expect.equal resp.Value "ping" "protobuf request/response round-trips over the socket"
              do! channel.DisposeAsync().AsTask() |> Async.AwaitTask
              do! server.Stop () |> Async.AwaitTask
          }

          testCase "Codec.google round-trips a protobuf message"
          <| fun () ->
              let codec = Codec.google StringValue.Parser
              let back = codec.Decode(codec.Encode(StringValue(Value = "hi")))
              Expect.equal back.Value "hi" "google codec round-trip"

          testCase "Codec.protobufNet round-trips an F# record (option/array/Dictionary)"
          <| fun () ->
              let codec = Codec.protobufNet<Sample> ()
              let meta = Dictionary<string, string>()
              meta["k"] <- "v"

              let value =
                  { Name = "n"
                    Tags = [| "a"; "b" |]
                    Note = Some "note"
                    Meta = meta }

              let back = codec.Decode(codec.Encode value)
              Expect.equal back.Name "n" "name"
              Expect.equal back.Tags [| "a"; "b" |] "tags array (list would not serialize)"
              Expect.equal back.Note (Some "note") "option (needs protobuf-net-fsharp)"
              Expect.equal back.Meta.Count 1 "dict count (Map would not serialize)"
              Expect.equal back.Meta["k"] "v" "dict value"

          testCase "Grpc.stateOf maps a fresh channel to Disconnected"
          <| fun () ->
              let channel = GrpcTransport.connect (Uri "https://localhost:5001")
              Expect.equal (GrpcTransport.stateOf channel) Disconnected "a fresh gRPC channel is idle/disconnected"

          testCaseAsync "WebSocket connect-retry gives up with a TimeoutException on a dead port"
          <| async {
              let opts =
                  { WebSocketOptions.defaults with
                      ConnectRetries = 2
                      ConnectBackoff = TimeSpan.FromMilliseconds 20.0 }

              let! result =
                  WebSocketTransport.connectAsync (Uri "ws://127.0.0.1:1/nope") opts CancellationToken.None
                  |> Async.AwaitTask
                  |> Async.Catch

              match result with
              | Choice2Of2 ex when (unwrap ex :? TimeoutException) -> ()
              | Choice2Of2 ex -> failtestf "expected TimeoutException, got %O" ex
              | Choice1Of2 _ -> failtest "expected connect to fail on a dead port"
          }

          testCaseAsync "Elmish Cmd.exchange dispatches the correlated response"
          <| async {
              let channel = fakeChannel (StringValue(Value = "pong"))
              let cmd = Net.Cmd.exchange channel (StringValue(Value = "ping")) Msg.Got Msg.Failed
              let tcs = TaskCompletionSource<Msg>(TaskCreationOptions.RunContinuationsAsynchronously)

              for effect in cmd do
                  effect (fun m -> tcs.TrySetResult m |> ignore)

              let! got = tcs.Task |> Async.AwaitTask

              match got with
              | Msg.Got sv -> Expect.equal sv.Value "pong" "Cmd dispatched the response"
              | Msg.Failed e -> failtestf "unexpected error: %O" e
          } ]
