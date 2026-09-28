# C3-NET-01 — Ordinary V2 receiver adoption

Status: activation source prepared for the 0.1.5 CLI. Dedicated custody is enrolled. Installed
operation and its post-merge settlement proof remain pending until this change merges.

FS.GG.Net is the fixed C3 source repository (`FS-GG/FS.GG.Net`, repository ID
`1305845505`) under the code-owned `net-v1` profile. This change adds only repository-owned
receiver source. It changes no repository setting, environment, secret, branch protection, required
check, generated workspace content, or protected effect.

## Prepared source

- The receiver workflow is bound to protected-main pushes. Its read-only, secret-free preflight
  produces an exact-run receipt. The bounded credential job runs only when that receipt admits
  activation, rechecks current Authority, verifies the pinned public package archive, and uses
  only the dedicated `ordinary-v2` secrets. Both checkouts persist no credential.
- The source pattern comes from Rendering receiver commit
  `8f4acd853566ea287abd15655c1aa5c4d3ceb403`; the observer retains its repaired Audio bytes,
  and the qualifier replaces only the code-owned source profile. Their SHA-256 digests are
  `6e63f6f724fb267e77ef02f41b4c58a833e0dc5b08c003f07fd16c89e4f7fd55` and
  `ff38a1b5748aaec8ff23561ae7c0c3eb67fb5232b9a4a43071dff22182dbac32` respectively.
- The selected settlement checks are `Build + test (locked restore)` and
  `contract-coherence / coherence`. The four separate native required gates are those two checks,
  `kit / coordination-kit`, and `materialize / receiver-validate`. Every check is bound to
  GitHub Actions App `15368` and its exact workflow ID,
  path, event, PR head, run, job, suite, and attempt.
- Both selected checks come from Net's existing gate workflow `316245439` at
  `.github/workflows/gate.yml`; the other producer workflows are coordination coherence
  `316890379` and kit materialization `316890380`. Net does not produce or require
  `routine-eligibility`, and this receiver does not add that check.
- The shared policy ID remains `v2-ci-i1-ordinary-settlement-v1`; the shared Authority anchor retains
  App `5064713`, installation `164553252`, repository `FS-GG/FS.GG.Coordination.Authority`
  (`1351660651`), `contents:write`, metadata read, and the existing writer/integrity ruleset pins.
- Read-only API observation at `2026-09-28T15:00:36Z`, against Net main
  `dfc04d994e955842a7e16597f7093ed14f1b5251`, found the `ordinary-v2` environment as ID
  `22920188172`, restricted to the single `main` branch policy ID `61287584`, with no reviewers.
  Protected custody bridge run `36419999006` succeeded and the environment now reads back the exact
  three dedicated ordinary-v2 secret names.
- The immutable public `v0.1.5` release package pin is SHA-256
  `3567a92825917a7d537f6c5c545d3a7947bc35edd666fc3a1898de3bf97267c9`, from reviewed
  Coordination source and exact tag `1268908d2d5a38d30a764c927f3e0591e53138aa`. The public
  release asset matches the prepared archive byte for byte. Its readback records an identical
  GitHub Packages archive and an identical nuget.org payload after NuGet signing; anonymous
  public-only install and invocation passed before this activation source was pushed.
- Net already pins .NET SDK `10.0.401` in the repository's tracked `global.json`. The credential
  job uses that pin after receipt and Authority verification.

## Installation boundary

This activation source was held locally until one reviewed change verified all of:

1. an immutable published Coordination CLI supports the exact `net-v1` source profile and its
   served package SHA-256 is pinned;
2. Net identity, exact current required-check population, producer mappings, and shared
   Authority binding are freshly read back.

This candidate changes policy status, installed state, package evidence, observer guard, and the
bounded credential job together. It imports no V1 admission or receiver state.

## Activation handoff (prepared on 2026-09-28)

This activation candidate becomes one reviewed PR based on protected `main` only after the
public `FS.GG.Coordination.Cli` 0.1.5 release asset has its SHA-256 verified and the published
package implements `net-v1`. The package pin must identify that exact digest and source commit;
a local build or an intended release version is insufficient.

Refresh Net's repository ID, protected-main head, required check names and App IDs, workflow IDs and
paths, and the `ordinary-v2` environment branch policy and three dedicated secret *names* from the
native API. Re-read the shared Authority binding against its current source. On 2026-09-28 the Net
readback matched repository ID `1305845505`, four required GitHub Actions App `15368` contexts,
workflow IDs `316245439`, `316890379`, `316890380`, environment ID `22920188172`, its sole `main`
branch policy ID `61287584`, and exactly the three names recorded above. These observations are a
baseline, not permission to use stale values at activation.

The candidate enables the secret-free preflight, passes its receipt digest and activation result
to the `ordinary-v2` credential job, checks the public CLI archive SHA-256 before local-only
installation, and rechecks the same-run receipt and current protected policy, workflow, and anchor
before a settlement attempt. The policy and source tests bind the installed state, package pin,
receipt fence, exact secret inventory, and absence of request or manual trigger paths together.

The clean source gate for this repository is the two Python receiver test files followed by
`dotnet restore FS.GG.Net.slnx --locked-mode`, `dotnet build FS.GG.Net.slnx -c Debug --no-restore`,
and `dotnet test FS.GG.Net.slnx -c Debug --no-build --no-restore`. If the shared NuGet cache raises
`NU1403` for `FSharp.Core 10.1.401`, use a fresh task-specific `NUGET_PACKAGES` directory; the
isolated locked restore, build, and both test assemblies passed at this handoff. Preserve all four
native required checks, merge the exact green PR head, read back the merged Authority, and verify
the ordinary `AlreadyComplete` rerun behavior before recording installed operation.
