# Comprehensive code and architecture review

- Repository: `FS-GG/FS.GG.Net`
- Reviewed revision: `f4dc35518820f125378029dbb221c9da79453589`
- Review completed: 2026-07-25 19:46:15 UTC (21:46:15 CEST)
- Scope: transport and message-channel abstractions, WebSocket/gRPC adapters, server/Elmish integration, tests, packaging, and current GitHub checks

## Executive assessment

Net has a good two-level abstraction: byte/message transports are distinct from request/reply channels, and WebSocket plus gRPC adapters share the higher-level model. All 15 Release tests and all current GitHub checks pass. The most serious defect is lifecycle handling in the sequential channel: clean receive-loop completion or disposal can leave an in-flight `Exchange` awaiting forever. Disposal can also race with semaphore release.

Overall risk: **high** for shutdown/disconnection paths, **medium** otherwise. The core model is promising, but lifecycle completion, resource limits, and server concurrency require hardening before exposure to untrusted or unreliable peers.

## Architecture

`ITransport` represents the underlying message movement, while `IMessageChannel` adds sequential or multiplexed request/reply semantics. Protocol adapters sit below the channel layer, and server/Elmish integrations sit above it. The multiplexed implementation already centralizes pending-request failure; the sequential implementation should share equivalent lifecycle semantics.

## Evidence

| Check | Result |
|---|---|
| Core tests, Release | 5 passed |
| Integration tests, Release | 10 passed |
| Total | 15 passed, 0 failed |
| Current-revision GitHub checks | 3 succeeded, 0 failed |
| Static lifecycle review | Sequential completion/disposal does not settle pending exchange |

The integration tests include real WebSocket/gRPC paths, but the suite is small and does not yet cover hostile peers, disconnect races, slow consumers, or sustained load.

## Findings

### 1. Critical — sequential exchanges can wait forever after clean disconnect or disposal

In `Core.fs`, the sequential receive loop completes `Incoming` on normal end-of-stream but does not complete/fault the current `pending` exchange. `DisposeAsync` cancels the loop and completes incoming delivery, but likewise does not settle the pending reply.

An `Exchange` using `CancellationToken.None` can therefore remain incomplete forever after a clean remote close or local disposal. The multiplexed channel already has `faultAll`, demonstrating the intended lifecycle model.

Recommendation: centralize termination so normal EOF, cancellation, faults, and disposal atomically settle every pending operation with a typed channel-closed exception. Await receive-loop termination during disposal. Add deterministic tests for EOF-before-reply and dispose-during-exchange.

### 2. High — sequential disposal can race with `SemaphoreSlim.Release`

`DisposeAsync` disposes the sequential gate immediately. An in-flight `Exchange` may later execute its `finally` block and call `gate.Release()`, producing `ObjectDisposedException` and masking the original close reason.

Recommendation: stop admission, cancel/fault pending work, await all in-flight ownership/receive-loop completion, and only then dispose synchronization primitives. Make disposal idempotent.

### 3. High — WebSocket receive buffering is unbounded

`WebSocket.fs` uses an unbounded inbound channel and reassembles each message in a `MemoryStream` without a maximum message size. A fast peer, fragmented oversized message, or slow consumer can drive unbounded memory growth.

Recommendation: use a bounded channel with a documented backpressure/overflow policy and enforce a configurable maximum assembled-message size before allocation grows. Close with an appropriate WebSocket status when exceeded.

### 4. Medium — echo-server handlers have unbounded concurrency and suppress diagnostics

The echo server starts handlers without a concurrency limit or linked cancellation, prunes completed tasks only when later messages arrive, and catches/discards all handler errors.

Recommendation: use a bounded worker/semaphore model, link handlers to server cancellation, observe tasks promptly, and expose structured error diagnostics. Drain handlers during shutdown.

### 5. Low — async enumerator lifetime is not consistently disposed

Core and Elmish subscription paths obtain async enumerators without consistently disposing them. This can retain transport/subscription resources across cancellation.

Recommendation: wrap enumerators in `use`/`use!`-equivalent lifetime management and add cancellation/disposal tests.

## Strengths

- Transport and request/reply abstractions have a clear dependency boundary.
- WebSocket and gRPC share a coherent higher-level channel contract.
- Multiplexed pending-request failure provides a good model for sequential repair.
- All current Release tests and live checks are green.
- Packaging is split by capability rather than forcing every transport on every consumer.

## Recommended order

1. Make channel termination settle all pending exchanges and await loop shutdown.
2. Fix disposal ordering and add race-focused tests.
3. Bound WebSocket message size and inbound buffering.
4. Bound, cancel, and observe server handlers.
5. Dispose async enumerators and expand disconnect/load integration coverage.
