#!/usr/bin/env bash
# The RUNTIME SKILL-ROOT SET this repo declares — asserted against ADR-0011's two roots, and
# demonstrated to be able to fire.
#
# WHY THIS EXISTS (FS-GG/.github#1721, ADR-0067 §9 phase 4). This repo used to commit its skills
# TWICE: the FS.GG.Kit materialize wrote byte-identical copies into `.claude/skills` AND
# `.agents/skills`, and `coordination-coherence` graded both sets against the pin. Phase 4 retired the
# second copy: `.agents/skills` is now a VIEW root (ADR-0065 §A root's three dispositions) whose
# content `scripts/skill-view generate` resolves from `.claude/skills` at checkout. The union of
# `<FsggKitSkillRoots>` and `<FsggKitViewSkillRoots>` is the runtime root set, and it did not change.
#
# WHAT THE RETIREMENT GAVE UP, WHICH IS THE ONLY REASON THIS FILE IS HERE. Before it, a change that
# dropped `.agents/skills` from this repo's runtime contract would have been caught by
# `coordination-coherence`: the root was materialized into, so removing it produced missing files
# against the pin. Now it is not materialized into, and the two gates that could notice both go QUIET
# instead of red. Both halves were MEASURED on this repo's own tree before this file was written:
#
#   * `coordination-sync --check --against-pin` derives its roots from `<FsggKitSkillRoots>` alone
#     (FS-GG/.github `scripts/coordination-sync`, `skill_roots()`). With `<FsggKitViewSkillRoots>`
#     emptied and `.agents` deleted outright, it reported
#     "OK — all 28 materialized file(s) match the FS.GG.Kit 0.15.0 this tree pins". Green.
#   * `FsggKitCheckSkillView` is SCOPED TO VIEW ROOTS and says so out loud when there are none —
#     "no view skill roots declared (FsggKitViewSkillRoots is empty) — nothing to assert" — which is
#     a message, not a failure. Green.
#
# So emptying `<FsggKitViewSkillRoots>` would remove `.agents/skills` from this repo's runtime
# contract with EVERY gate green, and the only observable consequence would be that Codex resolves
# zero skills here and exits 0 saying nothing (ADR-0067 §8's measured silent class). That is exactly
# the trade ADR-0067 §8 forbids — "a rewrite that removes the loud failure and adds the quiet one is
# worse than no rewrite" — so the retirement ships the replacement alarm in the same change. This is
# it.
#
# THIS CHECK IS NOT A REQUIRED STATUS CHECK, AND SAYING SO IS PART OF THE RECORD. FS-GG/FS.GG.Net's
# `main` carries no branch protection and no rulesets at all (FS-GG/.github#1714, a human decision
# that is not this change's to pre-empt), so NOTHING on this repo is enforced at merge time — this
# alarm, `gate`, and `contract-coherence` alike. It fails loudly in CI, which is what ADR-0067 §8
# asks of it; it cannot block a merge, which is #1714's subject and not this file's.
#
# IT GRADES THE DECLARATION, NOT MSBUILD'S EVALUATION, and that is deliberate rather than lazy. The
# faithful alternative is `dotnet msbuild -getProperty:` on the receiver project, which needs a
# RESTORE of the pinned FS.GG.Kit — a network round-trip added to a check that grades a two-line fact
# this repo authors in its own tree. It would also introduce a second source of truth for the
# package's defaults: a property this repo does NOT declare evaluates to the package's default, so a
# text reader would have to restate `.claude/skills;.agents/skills` to interpret an absence, and a
# restated default is the invented-location bug one file over. Requiring BOTH properties to be
# declared EXPLICITLY removes the question: an absence is a RED, not a guess.
#
# Fails CLOSED throughout: an unreadable project, a missing property, a multi-line declaration this
# reader cannot parse, and a union that is not ADR-0011's two are each `bad`. "I could not look" is
# never "looked, and fine" (FS-GG/.github#266).

set -uo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

PASS=0
FAIL=0
ok()  { PASS=$((PASS + 1)); printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL + 1)); printf '  \033[31m✗\033[0m %s\n' "$1"; }

# ADR-0011 Decision 1 as amended by ADR-0067 §5 and executed by FS-GG/.github#1636: `.codex/skills` is
# retired, and the runtime root set is these two. SORTED, so the comparison is set equality and not an
# accident of which property each root is declared in — moving a root between the two properties is a
# legal disposition change (ADR-0065) and must NOT red this.
FSGG_RUNTIME_ROOTS_EXPECTED='.agents/skills .claude/skills'

# The receiver project is where both properties live. Named once here rather than restated below.
FSGG_RECEIVER_PROJ="${FSGG_RECEIVER_PROJ:-$REPO_ROOT/.config/kit/FS.GG.Kit.receiver.proj}"

# msbuild_property <file> <name>
# Echo the text of a single-line `<name>value</name>` element; echo nothing and return 1 when the
# element is absent, empty, or not on one line. Deliberately NOT an XML parser: the one thing this
# needs to distinguish is "declared with a value" from "anything else", and every "anything else"
# lands on the same red. A declaration this cannot read is a declaration a reviewer should reformat.
msbuild_property() {
  local file="$1" name="$2" value
  [[ -r "$file" ]] || return 1
  value="$(sed -n "s|^[[:space:]]*<${name}>\(.*\)</${name}>[[:space:]]*$|\1|p" "$file" | head -1)"
  [[ -n "$value" ]] || return 1
  printf '%s' "$value"
}

# runtime_root_union <file>
# Echo the sorted, space-separated union of <FsggKitSkillRoots> and <FsggKitViewSkillRoots>. Returns 1
# with nothing on stdout when either property is not declared — an undeclared property is the failure
# this alarm exists for, so it must not be silently treated as an empty contribution.
runtime_root_union() {
  local file="$1" live view
  live="$(msbuild_property "$file" FsggKitSkillRoots)"     || return 1
  view="$(msbuild_property "$file" FsggKitViewSkillRoots)" || return 1
  printf '%s;%s' "$live" "$view" | tr ';' '\n' \
    | sed 's|[[:space:]]||g; s|/*$||' | grep -v '^$' | sort -u | paste -sd' ' -
}

# assert_runtime_roots <lane>
assert_runtime_roots() {
  local lane="$1" union
  if ! union="$(runtime_root_union "$FSGG_RECEIVER_PROJ")"; then
    bad "$lane: cannot read the runtime root set from $FSGG_RECEIVER_PROJ — both <FsggKitSkillRoots> and <FsggKitViewSkillRoots> must be declared, each on ONE line. ADR-0067 §9 phase 4 made this repo's second runtime root a generated VIEW, and no other gate can see it leave the contract (see this file's header)."
    return
  fi
  if [[ "$union" == "$FSGG_RUNTIME_ROOTS_EXPECTED" ]]; then
    ok "$lane: runtime skill roots are ADR-0011's two ($union) — the union of <FsggKitSkillRoots> and <FsggKitViewSkillRoots>"
  else
    bad "$lane: this repo's runtime skill roots are '$union', not '$FSGG_RUNTIME_ROOTS_EXPECTED'. A root that leaves this union leaves the runtime contract, and BOTH kit gates stay green while it does: coordination-coherence looks only at <FsggKitSkillRoots>, and FsggKitCheckSkillView reports 'nothing to assert' for an empty <FsggKitViewSkillRoots>. Codex would then resolve zero skills here and exit 0 saying nothing (ADR-0067 §8). If the root set is genuinely meant to change, that is an ADR-0065 §Retiring a root contract migration — amend the record and this constant in the same change."
  fi
}

# assert_runtime_roots_can_fire <lane>
# "Demonstrated, not asserted" — a gate that never fires and a gate that always passes are
# indistinguishable from outside (FS-GG/.github#1611 category D). Entirely offline, entirely local:
# five fixture projects in a temp dir plus one path that does not exist, driving the ASSERTION rather
# than only the predicate, with the counters snapshotted and restored. Driving the assertion is the
# part that matters — a demo that exercises only the predicate survives a mutation of the `bad` arm.
assert_runtime_roots_can_fire() {
  local lane="$1" tmp saved_pass saved_fail proj
  tmp="$(mktemp -d)"
  saved_pass="$PASS" saved_fail="$FAIL"

  local ok_cases=0 fired=0

  # (1) the shape this repo ships: both declared, union is the two roots -> PASS
  proj="$tmp/good.proj"
  printf '<Project>\n  <FsggKitSkillRoots>.claude/skills</FsggKitSkillRoots>\n  <FsggKitViewSkillRoots>.agents/skills</FsggKitViewSkillRoots>\n</Project>\n' > "$proj"
  PASS=0 FAIL=0; FSGG_RECEIVER_PROJ="$proj" assert_runtime_roots "$lane" >/dev/null
  [[ "$FAIL" -eq 0 && "$PASS" -eq 1 ]] && ok_cases=$((ok_cases + 1))

  # (2) the disposition swap: same union, roots declared the other way round -> PASS. This is a legal
  #     ADR-0065 move and reddening it would make the alarm an obstacle to the contract it protects.
  proj="$tmp/swapped.proj"
  printf '<Project>\n  <FsggKitSkillRoots>.agents/skills</FsggKitSkillRoots>\n  <FsggKitViewSkillRoots>.claude/skills</FsggKitViewSkillRoots>\n</Project>\n' > "$proj"
  PASS=0 FAIL=0; FSGG_RECEIVER_PROJ="$proj" assert_runtime_roots "$lane" >/dev/null
  [[ "$FAIL" -eq 0 && "$PASS" -eq 1 ]] && ok_cases=$((ok_cases + 1))

  # (3) THE REGRESSION THIS FILE EXISTS FOR: the view root emptied. Every kit gate is green on that
  #     tree — measured on this repo, see the header. This must not be.
  proj="$tmp/emptied.proj"
  printf '<Project>\n  <FsggKitSkillRoots>.claude/skills</FsggKitSkillRoots>\n  <FsggKitViewSkillRoots></FsggKitViewSkillRoots>\n</Project>\n' > "$proj"
  PASS=0 FAIL=0; FSGG_RECEIVER_PROJ="$proj" assert_runtime_roots "$lane" >/dev/null
  [[ "$FAIL" -eq 1 ]] && fired=$((fired + 1))

  # (4) the property deleted outright -> RED. An absent property must never read as an empty
  #     contribution to the union, which would make the deletion the very thing it silently allows.
  proj="$tmp/deleted.proj"
  printf '<Project>\n  <FsggKitSkillRoots>.claude/skills</FsggKitSkillRoots>\n</Project>\n' > "$proj"
  PASS=0 FAIL=0; FSGG_RECEIVER_PROJ="$proj" assert_runtime_roots "$lane" >/dev/null
  [[ "$FAIL" -eq 1 ]] && fired=$((fired + 1))

  # (5) a THIRD root added without a contract migration -> RED. The alarm is set equality, not a
  #     minimum: ADR-0065 governs adding a root exactly as it governs removing one.
  proj="$tmp/extra.proj"
  printf '<Project>\n  <FsggKitSkillRoots>.claude/skills;.codex/skills</FsggKitSkillRoots>\n  <FsggKitViewSkillRoots>.agents/skills</FsggKitViewSkillRoots>\n</Project>\n' > "$proj"
  PASS=0 FAIL=0; FSGG_RECEIVER_PROJ="$proj" assert_runtime_roots "$lane" >/dev/null
  [[ "$FAIL" -eq 1 ]] && fired=$((fired + 1))

  # (6) an unreadable project -> RED. "I could not look" is never "looked, and fine".
  PASS=0 FAIL=0; FSGG_RECEIVER_PROJ="$tmp/does-not-exist.proj" assert_runtime_roots "$lane" >/dev/null
  [[ "$FAIL" -eq 1 ]] && fired=$((fired + 1))

  PASS="$saved_pass" FAIL="$saved_fail"
  rm -rf "$tmp"

  if [[ "$ok_cases" -eq 2 && "$fired" -eq 4 ]]; then
    ok "$lane: the runtime-root alarm can fire — 4 of 4 regressions RED (emptied view root, deleted property, extra root, unreadable project) and 2 of 2 legal shapes GREEN"
  else
    bad "$lane: the runtime-root alarm is NOT demonstrably live — $ok_cases/2 legal shapes passed and $fired/4 regressions fired. A gate that cannot fire is not a gate (FS-GG/.github#1611 category D)."
  fi
}

# assert_view_root_is_not_tracked <lane>
# The other half of the retirement, and the one git can silently undo. A view root that gets committed
# again is a committed symlink — which ADR-0067 §6 rejects on measurement, because it checks out as a
# small regular TEXT FILE under `git -c core.symlinks=false` (the git-for-Windows default without
# Developer Mode) and both runtimes then exit 0 with zero skills and no diagnostic.
assert_view_root_is_not_tracked() {
  local lane="$1" view tracked
  view="$(msbuild_property "$FSGG_RECEIVER_PROJ" FsggKitViewSkillRoots)" || {
    bad "$lane: cannot read <FsggKitViewSkillRoots> from $FSGG_RECEIVER_PROJ, so 'is the view root untracked?' has no subject."
    return
  }
  view="${view//;/ }"
  local root
  for root in $view; do
    if ! tracked="$(git -C "$REPO_ROOT" ls-files -- "$root" 2>/dev/null)"; then
      bad "$lane: could not ask git whether '$root' is tracked. That is not an answer (FS-GG/.github#266)."
      return
    fi
    if [[ -n "$tracked" ]]; then
      bad "$lane: '$root' is a VIEW root but git TRACKS $(printf '%s\n' "$tracked" | grep -c .) path(s) under it. Either the retirement was reverted (fine — then move the root back to <FsggKitSkillRoots>) or a generated view was committed. A COMMITTED symlink degrades to a plain text file under 'git -c core.symlinks=false' and both runtimes then load zero skills and exit 0 (ADR-0067 §6)."
      return
    fi
  done
  ok "$lane: every view root is untracked ($view) — the view stays generated, never committed"
}

lane="skill-view-roots"
printf '%s\n' "composition: $lane (FS.GG.Net)"
assert_runtime_roots "$lane"
assert_view_root_is_not_tracked "$lane"
assert_runtime_roots_can_fire "$lane"

printf '\n%s\n' "composition: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]] || exit 1
