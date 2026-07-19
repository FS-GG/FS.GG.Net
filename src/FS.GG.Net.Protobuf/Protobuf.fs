namespace FS.GG.Net.Protobuf

open System
open System.IO
open Google.Protobuf
open ProtoBuf.FSharp
open FS.GG.Net.Core

[<RequireQualifiedAccess>]
module Registration =
    let record (recordType: Type) : unit =
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
        Serialiser.registerRecordRuntimeTypeIntoModel typeof<'T> model |> ignore

        { new IMessageCodec<'T> with
            member _.Encode(value: 'T) : ReadOnlyMemory<byte> =
                use ms = new MemoryStream()
                model.Serialize(ms, box value)
                ReadOnlyMemory<byte>(ms.ToArray())

            member _.Decode(bytes: ReadOnlyMemory<byte>) : 'T =
                use ms = new MemoryStream(bytes.ToArray())
                model.Deserialize(typeof<'T>, ms) :?> 'T }
