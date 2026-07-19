namespace FS.GG.Net.Grpc

open System
open Grpc.Net.Client
open FS.GG.Net.Core

/// Public contract exposed by this FS.GG.Net.Grpc package.
/// A thin lifecycle bridge over grpc-dotnet. The generated client / protobuf-net.Grpc service proxy
/// still owns method dispatch and streaming — this only unifies connection lifecycle with the rest
/// of FS.GG.Net.
[<RequireQualifiedAccess>]
module GrpcTransport =
    /// Open a gRPC channel to `address` (e.g. https://localhost:5001). The caller builds its typed
    /// client/proxy over the returned channel as usual.
    val connect: address: Uri -> GrpcChannel

    /// Project a gRPC channel's connectivity onto the component's ConnectionState.
    val stateOf: channel: GrpcChannel -> ConnectionState
