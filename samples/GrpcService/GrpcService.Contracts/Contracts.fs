namespace GrpcService.Contracts

open System.Collections.Generic
open System.ServiceModel
open System.Threading.Tasks
open ProtoBuf
open ProtoBuf.Grpc

// Code-first gRPC contract (protobuf-net.Grpc): F# records + a service interface, no .proto.
// This IS the wire contract a third party generates/consumes a client against — the owned/code-first
// provenance of ADR-0052 §6. Reminder from fsGRPCSkills: register each record before serializing
// (FS.GG.Net.Protobuf.Registration does that); use `array` not `list`, `Dictionary` not `Map`.

[<ProtoContract>]
type GreetRequest = { [<ProtoMember(1)>] Name: string }

[<ProtoContract>]
type GreetReply = { [<ProtoMember(1)>] Message: string }

[<ProtoContract>]
type CountRequest = { [<ProtoMember(1)>] To: int }

[<ProtoContract>]
type Tick = { [<ProtoMember(1)>] N: int }

/// The service third-party clients target. `SayHello` is unary; `CountTo` is server-streaming.
[<ServiceContract>]
type IGreeter =
    [<OperationContract>]
    abstract SayHello: request: GreetRequest * context: CallContext -> ValueTask<GreetReply>

    [<OperationContract>]
    abstract CountTo: request: CountRequest * context: CallContext -> IAsyncEnumerable<Tick>

/// The record types that must be registered with protobuf-net-fsharp on both server and client.
module Contract =
    let recordTypes: System.Type list =
        [ typeof<GreetRequest>; typeof<GreetReply>; typeof<CountRequest>; typeof<Tick> ]
