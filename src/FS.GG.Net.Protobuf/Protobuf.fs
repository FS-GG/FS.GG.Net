namespace FS.GG.Net.Protobuf

open System
open System.IO
open Google.Protobuf
open ProtoBuf.FSharp
open FS.GG.Net.Core

[<RequireQualifiedAccess>]
module Registration =
    // Register each type at most once per process. Re-registering a type whose serializer is already
    // generated throws ("cannot be changed once a serializer has been generated"); protobuf-net's own
    // IsDefined is the wrong guard (it is true for types it could auto-handle, so it would skip the
    // essential F# record registration). Track what WE have registered instead.
    let private registered = System.Collections.Concurrent.ConcurrentDictionary<Type, bool>()

    let record (recordType: Type) : unit =
        if registered.TryAdd(recordType, true) then
            Serialiser.registerRecordRuntimeTypeIntoModel recordType Serialiser.defaultModel
            |> ignore

    let records (recordTypes: Type list) : unit = recordTypes |> List.iter record

[<RequireQualifiedAccess>]
module Codec =
    let google<'T when 'T :> IMessage<'T>> (parser: MessageParser<'T>) : IMessageCodec<'T> =
        { new IMessageCodec<'T> with
            member _.Encode(value: 'T) : ReadOnlyMemory<byte> =
                // MessageExtensions.ToByteArray — the reproducible raw path (no gRPC channel involved).
                ReadOnlyMemory<byte>(MessageExtensions.ToByteArray value)

            member _.Decode(bytes: ReadOnlyMemory<byte>) : 'T = parser.ParseFrom(bytes.ToArray()) }

    let protobufNet<'T> () : IMessageCodec<'T> =
        let model = Serialiser.defaultModel
        Registration.record typeof<'T>

        { new IMessageCodec<'T> with
            member _.Encode(value: 'T) : ReadOnlyMemory<byte> =
                use ms = new MemoryStream()
                model.Serialize(ms, box value)
                ReadOnlyMemory<byte>(ms.ToArray())

            member _.Decode(bytes: ReadOnlyMemory<byte>) : 'T =
                use ms = new MemoryStream(bytes.ToArray())
                model.Deserialize(typeof<'T>, ms) :?> 'T }
