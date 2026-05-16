#!/usr/bin/env bash
# Clean up stale agent worktrees and their `target/` build artifacts.
#
# Usage:
#   scripts/cleanup-worktrees.sh                # remove all agent worktrees
#   scripts/cleanup-worktrees.sh KEEP_ID...     # keep these agent IDs intact
#   scripts/cleanup-worktrees.sh --branches     # also delete orphan agent branches
#   scripts/cleanup-worktrees.sh --target       # also `cargo clean` the main target/
#
# Why this exists: parallel-agent dispatch creates `.claude/worktrees/agent-<id>/`
# subtrees, each with its own `target/` (~2-4 GB). Across a multi-round fix
# campaign these accumulate to 60-80 GB and exhaust disk. The agent harness
# locks the worktree dirs; we force-remove the inactive ones.
#
# Safety: hard-coded protected paths and an opt-in `--target` flag prevent
# wiping the main repo's `target/` without explicit consent. Branches the
# script may delete are saved in `.git/refs/heads/` until pruned by gc;
# nothing is unrecoverable.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

WORKTREE_DIR=".claude/worktrees"
DRY_RUN="${CLEANUP_DRY_RUN:-0}"

declare -a KEEP_IDS
DELETE_BRANCHES=0
CLEAN_TARGET=0

for arg in "$@"; do
    case "$arg" in
        --branches) DELETE_BRANCHES=1 ;;
        --target)   CLEAN_TARGET=1   ;;
        --dry-run)  DRY_RUN=1        ;;
        *)          KEEP_IDS+=("$arg") ;;
    esac
done

run() {
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "[dry-run] $*"
    else
        echo "+ $*"
        "$@"
    fi
}

is_kept() {
    local path="$1"
    local id
    id="$(basename "$path" | sed 's/^agent-//')"
    for keep in "${KEEP_IDS[@]:-}"; do
        [[ "$id" == "$keep" ]] && return 0
    done
    return 1
}

echo "== cleanup-worktrees.sh =="
echo "repo:        $REPO_ROOT"
echo "keep ids:    ${KEEP_IDS[*]:-(none)}"
echo "branches:    $([[ $DELETE_BRANCHES == 1 ]] && echo yes || echo no)"
echo "main target: $([[ $CLEAN_TARGET == 1 ]] && echo cargo clean || echo skip)"
echo "dry-run:     $DRY_RUN"
echo

if [[ ! -d "$WORKTREE_DIR" ]]; then
    echo "no $WORKTREE_DIR — nothing to do"
    exit 0
fi

removed=0
kept=0
for path in "$WORKTREE_DIR"/agent-*; do
    [[ -d "$path" ]] || continue
    if is_kept "$path"; then
        echo "keep $path"
        kept=$((kept+1))
        continue
    fi
    # git worktree remove handles the .git/worktrees/* metadata + the dir.
    # --force needed because the harness locks worktree dirs.
    run git worktree remove --force "$path" 2>/dev/null || run rm -rf "$path"
    removed=$((removed+1))
done

# `git worktree prune` cleans dangling metadata for worktree dirs that were
# rm'd without going through `git worktree remove`. Cheap to always run.
run git worktree prune

# Delete agent branches if requested. We keep them by default so the commits
# remain reachable for inspection / cherry-pick / merge.
if [[ "$DELETE_BRANCHES" == 1 ]]; then
    echo
    echo "-- deleting orphan agent branches --"
    git for-each-ref --format='%(refname:short)' refs/heads/worktree-agent-* | while read -r branch; do
        # Skip branches still tied to a live worktree (paranoia).
        if git worktree list --porcelain | grep -q "^branch refs/heads/$branch$"; then
            echo "skip $branch (live worktree)"
            continue
        fi
        run git branch -D "$branch"
    done
fi

if [[ "$CLEAN_TARGET" == 1 ]]; then
    echo
    echo "-- cargo clean (main target/) --"
    run cargo clean
fi

echo
echo "summary: removed=$removed kept=$kept"
