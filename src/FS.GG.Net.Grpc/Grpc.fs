namespace FS.GG.Net.Grpc

open System
open Grpc.Core
open Grpc.Net.Client
open FS.GG.Net.Core

[<RequireQualifiedAccess>]
module GrpcTransport =
    let connect (address: Uri) : GrpcChannel = GrpcChannel.ForAddress address

    let stateOf (channel: GrpcChannel) : ConnectionState =
        match channel.State with
        | ConnectivityState.Idle -> Disconnected
        | ConnectivityState.Connecting -> Connecting
        | ConnectivityState.Ready -> Connected
        | ConnectivityState.TransientFailure -> Faulted(Exception "gRPC channel is in transient failure")
        | ConnectivityState.Shutdown -> Closing
        | other -> Faulted(Exception $"unknown gRPC connectivity state: {other}")
