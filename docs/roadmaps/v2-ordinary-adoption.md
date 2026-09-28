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
