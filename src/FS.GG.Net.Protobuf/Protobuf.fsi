namespace FS.GG.Net.Protobuf

open System
open Google.Protobuf
open FS.GG.Net.Core

/// Public contract exposed by this FS.GG.Net.Protobuf package.
/// The protobuf-net (code-first) F# registration gotchas from fsGRPCSkills, turned into API: register
/// each F# record type once, before serializing, or `None`/records fail at runtime.
[<RequireQualifiedAccess>]
module Registration =
    /// Register an F# record type with the shared protobuf-net model. Idempotent.
    val record: recordType: Type -> unit
    /// Register several F# record types at once.
    val records: recordTypes: Type list -> unit

/// Public contract exposed by this FS.GG.Net.Protobuf package.
[<RequireQualifiedAccess>]
module Codec =
    /// A codec for a Google.Protobuf generated message type (contract-first: FsGrpc or Grpc.Tools
    /// output). Uses `ToByteArray` / the message `parser` — the reproducible raw-bytes path that
    /// SC2-over-WebSocket needs. Pass the generated type's static `Parser`, e.g. `Codec.google Response.Parser`.
    val google<'T when 'T :> IMessage<'T>> : parser: MessageParser<'T> -> IMessageCodec<'T>

    /// A codec for a protobuf-net (code-first) F# contract type. Registers the record with
    /// protobuf-net-fsharp on construction so `option`/records serialize correctly. Reminder from the
    /// absorbed skills: use `array` not `list`, and `Dictionary<K,V>` not `Map<K,V>`, for repeated/map fields.
    val protobufNet<'T> : unit -> IMessageCodec<'T>
