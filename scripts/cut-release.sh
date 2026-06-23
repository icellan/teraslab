#!/usr/bin/env bash
# Cut a new release by creating and pushing the version tags: the server tag
# vX.Y.Z AND the matching Go client tag client/go/vX.Y.Z.
#
# Usage:
#   scripts/cut-release.sh v0.4.0            # tag current origin/main HEAD
#   scripts/cut-release.sh v0.4.0 -m "..."   # custom annotation message
#   scripts/cut-release.sh v0.4.0 --dry-run  # show what would happen, do nothing
#
# How releases work here: the GitHub Actions workflow `.github/workflows/release.yml`
# triggers on any pushed tag matching `v*`. It runs `cargo test --all`, builds
# server binaries for 4 platforms, builds+pushes a multi-arch Docker image to
# ghcr.io/icellan/teraslab:<version> and :latest, and creates a GitHub Release
# with generated notes + the binaries. The version is taken from the tag name
# (leading `v` stripped) — NOT from Cargo.toml, which is not kept in sync.
#
# The Go client (client/go/, module github.com/icellan/teraslab/client/go) is a
# submodule: Go resolves its versions from tags PREFIXED with the subdirectory,
# so a plain vX.Y.Z tag does not publish the client — `go get .../client/go@vX.Y.Z`
# needs a client/go/vX.Y.Z tag. This script cuts both at the same commit so every
# server release ships a matching Go client version (the client/go/* tag does not
# match the workflow's `v*` filter, so it triggers no extra CI).
#
# Convention: three-part semver with a leading v, e.g. v0.4.0 (NOT v0.4).
#
# Safety: refuses unless on the default branch, the tree is clean, local HEAD
# matches origin/<branch> (so you tag exactly what's published), and the tag
# does not already exist locally or on the remote. Tagging is the irreversible
# publish trigger — a pushed tag that CI consumes cannot be cleanly recalled.
set -euo pipefail

REMOTE="origin"
DEFAULT_BRANCH="main"

die() { printf 'cut-release: %s\n' "$*" >&2; exit 1; }

VERSION=""
MESSAGE=""
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    -m|--message) MESSAGE="${2:-}"; shift 2 ;;
    --dry-run)    DRY_RUN=1; shift ;;
    -h|--help)    sed -n '2,28p' "$0"; exit 0 ;;
    v*)           VERSION="$1"; shift ;;
    *)            die "unexpected argument: $1 (version must look like v0.4.0)" ;;
  esac
done

[ -n "$VERSION" ] || die "version required, e.g. scripts/cut-release.sh v0.4.0"

# Enforce vMAJOR.MINOR.PATCH (optionally with -rc/-beta suffix).
if ! printf '%s' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'; then
  die "version '$VERSION' is not v<major>.<minor>.<patch> (use v0.4.0, not v0.4)"
fi

# Go-module tag for the client submodule (see header). Cut alongside $VERSION.
CLIENT_TAG="client/go/$VERSION"

# Must be on the default branch.
branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$branch" = "$DEFAULT_BRANCH" ] || die "not on $DEFAULT_BRANCH (on '$branch'); releases are cut from $DEFAULT_BRANCH"

# Clean working tree.
git diff-index --quiet HEAD -- 2>/dev/null || die "working tree is dirty; commit or stash first"

# Neither tag may already exist locally or remotely.
for t in "$VERSION" "$CLIENT_TAG"; do
  git rev-parse -q --verify "refs/tags/$t" >/dev/null 2>&1 && die "tag $t already exists locally"
  if git ls-remote --exit-code --tags "$REMOTE" "refs/tags/$t" >/dev/null 2>&1; then
    die "tag $t already exists on $REMOTE"
  fi
done

# HEAD must match the remote branch so the tag points at published code.
git fetch -q "$REMOTE" "$DEFAULT_BRANCH"
local_head="$(git rev-parse HEAD)"
remote_head="$(git rev-parse "$REMOTE/$DEFAULT_BRANCH")"
[ "$local_head" = "$remote_head" ] || die "HEAD ($local_head) != $REMOTE/$DEFAULT_BRANCH ($remote_head); push or pull first so the tag matches the remote"

# Default annotation: subject + commit count since the previous tag.
if [ -z "$MESSAGE" ]; then
  prev="$(git describe --tags --abbrev=0 2>/dev/null || true)"
  if [ -n "$prev" ]; then
    n="$(git rev-list --count "$prev..HEAD")"
    MESSAGE="$VERSION ($n commits since $prev)"
  else
    MESSAGE="$VERSION"
  fi
fi

echo "Release:  $VERSION  (+ Go client tag $CLIENT_TAG)"
echo "Commit:   $(git rev-parse --short HEAD) — $(git log -1 --format=%s)"
echo "Message:  $MESSAGE"
echo "Trigger:  push $VERSION -> .github/workflows/release.yml (test, build x4, docker push, GitHub Release)"
echo "          $CLIENT_TAG publishes the Go client (Go reads the tag directly; no workflow)"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "[dry-run] would: git tag -a $VERSION -m '...' && git tag -a $CLIENT_TAG -m '...' && git push $REMOTE $VERSION $CLIENT_TAG"
  exit 0
fi

git tag -a "$VERSION" -m "$MESSAGE" HEAD
git tag -a "$CLIENT_TAG" -m "$MESSAGE" HEAD
git push "$REMOTE" "$VERSION" "$CLIENT_TAG"

echo
echo "Pushed $VERSION + $CLIENT_TAG. Watch the release build:"
echo "  gh run watch \$(gh run list --workflow=release.yml -L1 --json databaseId -q '.[0].databaseId')"
echo "If a run fails on billing/limits, fix Settings -> Billing & plans, then: gh run rerun <run-id>"
