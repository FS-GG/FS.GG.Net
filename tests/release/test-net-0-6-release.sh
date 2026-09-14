#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
packages=""

while (($#)); do
  case "$1" in
    --root) root="$(cd "$2" && pwd)"; shift 2 ;;
    --packages) packages="$(cd "$2" && pwd)"; shift 2 ;;
    --source-only) shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

version="$(sed -n 's:.*<FsGgNetVersion>\([^<]*\)</FsGgNetVersion>.*:\1:p' "$root/Directory.Packages.props")"
[[ "$version" == "0.6.0" ]] || {
  echo "release scalar must be 0.6.0, observed '$version'" >&2
  exit 1
}

python3 - "$root/.github/workflows/release.yml" <<'PY'
import pathlib, sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
required = {
    "verification dependency": "needs: [verify]",
    "repository commit binding": '-p:RepositoryCommit="$GITHUB_SHA"',
    "API compatibility gate": "-p:EnablePackageValidation=true",
    "0.5.0 API baseline": "-p:PackageValidationBaselineVersion=0.5.0",
    "custody checksums": "sha256sum -- *.nupkg",
    "retained custody artifact": "net-release-custody-",
    "authenticated recovery": "Authenticate failed release custody",
    "dual-feed payload readback": "Read back and compare both feed payloads",
    "org feed": "https://nuget.pkg.github.com/FS-GG/index.json",
    "public feed": "https://api.nuget.org/v3/index.json",
}
for label, token in required.items():
    if token not in text:
        raise SystemExit(f"release workflow is missing {label}: {token}")
if text.count("dotnet pack FS.GG.Net.slnx") != 1:
    raise SystemExit("release workflow must prepare the coherent set exactly once")
push = 'dotnet nuget push "artifacts/packages/*.nupkg"'
pushes = [index for index in range(len(text)) if text.startswith(push, index)]
if len(pushes) != 2:
    raise SystemExit("release workflow must contain exactly two package pushes")
retained = text.index("Retain original coherent package bytes before publication")
if retained >= pushes[0] or pushes[0] >= pushes[1]:
    raise SystemExit("custody must precede ordered org/public feed pushes")
if "dotnet pack" in text[pushes[0]:pushes[1]]:
    raise SystemExit("release workflow must not re-pack between feed pushes")
PY

[[ -z "$packages" ]] && exit 0

mapfile -t nupkgs < <(find "$packages" -maxdepth 1 -type f -name '*.nupkg' ! -name '*.symbols.nupkg' -printf '%f\n' | sort)
expected=(
  FS.GG.Net.Core.0.6.0.nupkg
  FS.GG.Net.Elmish.0.6.0.nupkg
  FS.GG.Net.Grpc.0.6.0.nupkg
  FS.GG.Net.Protobuf.0.6.0.nupkg
  FS.GG.Net.WebSocket.0.6.0.nupkg
  FS.GG.Net.WebSocket.Server.0.6.0.nupkg
)
[[ "${nupkgs[*]}" == "${expected[*]}" ]] || {
  echo "expected exactly the coherent six-package 0.6.0 set" >&2
  printf 'observed: %s\n' "${nupkgs[*]:-<none>}" >&2
  exit 1
}

expected_commit="$(git -C "$root" rev-parse HEAD)"
for package in "${nupkgs[@]}"; do
  nuspec="$(unzip -Z1 "$packages/$package" | sed -n '/\.nuspec$/p')"
  metadata="$(unzip -p "$packages/$package" "$nuspec")"
  grep -q '<version>0.6.0</version>' <<<"$metadata" || {
    echo "$package does not declare version 0.6.0" >&2; exit 1;
  }
  grep -q "commit=\"$expected_commit\"" <<<"$metadata" || {
    echo "$package does not bind repository commit $expected_commit" >&2; exit 1;
  }
  if unzip -Z1 "$packages/$package" | grep -q '^fable/'; then
    echo "$package unexpectedly claims an unqualified Fable source view" >&2
    exit 1
  fi
done

for package in FS.GG.Net.Elmish FS.GG.Net.Grpc FS.GG.Net.Protobuf FS.GG.Net.WebSocket FS.GG.Net.WebSocket.Server; do
  archive="$packages/$package.0.6.0.nupkg"
  nuspec="$(unzip -Z1 "$archive" | sed -n '/\.nuspec$/p')"
  unzip -p "$archive" "$nuspec" | grep -q '<dependency id="FS.GG.Net.Core" version="0.6.0"' || {
    echo "$package does not pin FS.GG.Net.Core exactly to 0.6.0" >&2
    exit 1
  }
done
