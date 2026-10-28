#!/usr/bin/env bash
# Regenerate third_party/com_github_libp2p_go_libp2p_pubsub-admission.patch from the vendored
# fork, and verify that the four patches reproduce the fork from pristine v0.17.0.
#
# Bazel builds the *patched external module*, not third_party/go-libp2p-pubsub (go.mod replace
# only covers `go test`), so a fork change that is not in a patch compiles under go test and is
# missing under Bazel. Run this after every fork edit. Recipe, from deps.bzl:
#   base  = pristine v0.17.0 + cloneof + phaseforward + partialinterest (their BUILD hunks skipped)
#   body  = diff -ruN -x BUILD.bazel -x sign.go -x '*.orig' base fork
# The patch opens with a BUILD.bazel hunk that adds the fork-only source files to srcs, carried
# verbatim from the installed patch: go_repository runs gazelle before it applies patches, so a
# file a patch creates is compiled only if the patch also lists it. If you add a new .go file
# to the fork, extend that hunk by hand (it is the first block of the patch) and re-run.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
TP=$REPO/third_party
PATCH=$TP/com_github_libp2p_go_libp2p_pubsub-admission.patch
VENDORED=$TP/go-libp2p-pubsub
PRISTINE=$(cd "$REPO" && go env GOMODCACHE)/github.com/libp2p/go-libp2p-pubsub@v0.17.0
PRE=(
  "$TP/com_github_libp2p_go_libp2p_pubsub-cloneof.patch"
  "$TP/com_github_libp2p_go_libp2p_pubsub-phaseforward.patch"
  "$TP/com_github_libp2p_go_libp2p_pubsub-partialinterest.patch"
)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
[ -d "$PRISTINE" ] || { echo "pristine module not in the module cache: $PRISTINE"; exit 1; }

mkbase() { # $1 = destination: pristine plus the three earlier patches, BUILD hunks skipped
  cp -r "$PRISTINE" "$1"; chmod -R u+w "$1"
  for p in "${PRE[@]}"; do ( cd "$1" && patch -p1 -N -f < "$p" >/dev/null 2>&1 || true ); done
  find "$1" -name '*.orig' -delete
}

# The leading BUILD.bazel hunk of the installed patch, carried verbatim.
awk 'NR==1 && !/^diff --git a\/BUILD.bazel/ {exit 1} /^diff -ruN / {exit} {print}' "$PATCH" > "$WORK/build.hunk" \
  || { echo "the installed patch does not open with a BUILD.bazel hunk"; exit 1; }

mkbase "$WORK/base"
cp -r "$VENDORED" "$WORK/fork"
( cd "$WORK" && diff -ruN -x BUILD.bazel -x sign.go -x '*.orig' base fork > body.patch || true )
cat "$WORK/build.hunk" "$WORK/body.patch" > "$WORK/new.patch"

# Verify before installing: the new patch on a fresh base must reproduce the fork exactly.
mkbase "$WORK/verify"
( cd "$WORK/verify" && patch -p1 -N -f < "$WORK/new.patch" >/dev/null 2>&1 || true )
find "$WORK/verify" -name '*.orig' -delete
if ! diff -ruN -x BUILD.bazel -x sign.go "$WORK/verify" "$VENDORED" > "$WORK/verify.diff"; then
  echo "REGEN FAILED: the patch does not reproduce the vendored tree"; head -40 "$WORK/verify.diff"; exit 1
fi
cp "$WORK/new.patch" "$PATCH"
echo "admission patch regenerated and round-trip verified against pristine v0.17.0"
echo "now: bazel build @com_github_libp2p_go_libp2p_pubsub//... (and the targets that depend on it)"
