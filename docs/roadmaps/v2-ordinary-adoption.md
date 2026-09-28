# C3-NET-01 — Ordinary V2 receiver adoption

Status: source prepared and disabled. Dedicated custody is enrolled; CLI release, installation, and
activation remain pending.

FS.GG.Net is the fixed C3 source repository (`FS-GG/FS.GG.Net`, repository ID
`1305845505`) under the code-owned `net-v1` profile. This change adds only repository-owned
receiver source. It changes no repository setting, environment, secret, branch protection, required
check, generated workspace content, or protected effect.

## Prepared source

- The receiver workflow is bound to protected-main pushes, but its only job has an unconditional
  false guard. It uses read-only GitHub permissions, persists no checkout credential, and contains
  no credential job, environment binding, secret reference, package download, or settlement command.
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
- Read-only API observation at `2026-09-28T12:22:05Z`, against Net main
  `2fe9c00976b5d01c36fbd587c21135b650cf43b9`, found the `ordinary-v2` environment as ID
  `22920188172`, restricted to the single `main` branch policy ID `61287584`, with no reviewers.
  Protected custody bridge run `36419999006` succeeded and the environment now reads back the exact
  three dedicated ordinary-v2 secret names.
- No immutable published CLI release with `net-v1` support is selected. Version and package
  SHA-256 remain null, and policy explicitly refuses activation until a served package digest is
  independently verified.
- Net already pins .NET SDK `10.0.401` in the repository's tracked `global.json`. This
  receiver leaves that pin unchanged and invokes no .NET setup while disabled.

## Installation boundary

Do not enable the preflight or add a credential job until one reviewed source change verifies all of:

1. an immutable published Coordination CLI supports the exact `net-v1` source profile and its
   served package SHA-256 is pinned;
2. Net identity, exact current required-check population, producer mappings, and shared
   Authority binding are freshly read back.

The later activation must change policy status, installed state, package evidence, observer guard,
and the bounded credential job together. This disabled source cannot settle work and imports no V1
admission or receiver state.

## Activation handoff (prepared on 2026-09-28)

The next source change is one reviewed activation PR based on protected `main`. Before editing it,
read the public `FS.GG.Coordination.Cli` 0.1.5 release asset from the served feed, verify its SHA-256,
and confirm that the published package implements `net-v1`. Record that exact digest and source commit
in `packagePin`; a local build or an intended release version is insufficient.

Refresh Net's repository ID, protected-main head, required check names and App IDs, workflow IDs and
paths, and the `ordinary-v2` environment branch policy and three dedicated secret *names* from the
native API. Re-read the shared Authority binding against its current source. On 2026-09-28 the Net
readback matched repository ID `1305845505`, four required GitHub Actions App `15368` contexts,
workflow IDs `316245439`, `316890379`, `316890380`, environment ID `22920188172`, its sole `main`
branch policy ID `61287584`, and exactly the three names recorded above. These observations are a
baseline, not permission to use stale values at activation.

In that PR, enable the secret-free preflight, pass its receipt digest and activation result to a
bounded `ordinary-v2` credential job, and use the installed public CLI archive only after checking
its served SHA-256. The credential job must recheck the same-run receipt and current protected
policy, workflow, and anchor before reading the dedicated secrets and executing one settlement
attempt. Set policy status to `installed`, `credentialJob.installed` to true, and pin the verified
package in the same source change. Update the source tests so an enabled workflow is checked for
the receipt fence, local-only package install, exact secret inventory, and absence of request or
manual trigger paths.

The clean source gate for this repository is the two Python receiver test files followed by
`dotnet restore FS.GG.Net.slnx --locked-mode`, `dotnet build FS.GG.Net.slnx -c Debug --no-restore`,
and `dotnet test FS.GG.Net.slnx -c Debug --no-build --no-restore`. If the shared NuGet cache raises
`NU1403` for `FSharp.Core 10.1.401`, use a fresh task-specific `NUGET_PACKAGES` directory; the
isolated locked restore, build, and both test assemblies passed at this handoff. Preserve all four
native required checks, merge the exact green PR head, read back the merged Authority, and verify
the ordinary `AlreadyComplete` rerun behavior before recording installed operation.
