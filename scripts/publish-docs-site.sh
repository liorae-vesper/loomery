#!/usr/bin/env sh
# SPDX-License-Identifier: MPL-2.0
#
# Publishes the documentation site to a branch, so a static host can serve it from
# that branch — the `gh-pages` pattern, for hosts that deploy from a branch instead
# of an upload.
#
#   sh scripts/publish-docs-site.sh                    # build, commit, push
#   sh scripts/publish-docs-site.sh --no-push          # build and commit locally
#   sh scripts/publish-docs-site.sh --branch site      # a different branch
#
# The working tree is never touched. The built files are indexed into a temporary
# index, written as a tree object, and committed onto the deploy branch with
# plumbing, so the branch you happen to have checked out — and any uncommitted work
# in it — is irrelevant. That matters here: two sessions work in this repository at
# once, in more than one worktree.
#
# The deploy branch is **append-only**: every publish adds one commit whose tree is
# the whole site, so the push is always a fast-forward and no published history is
# rewritten. Git stores identical files once, so a publish costs what changed
# between two builds. Nothing else is ever committed to that branch.
#
# Set `base` in `docs/.vitepress/config.mts` to the path the site is served from
# (a repository subpath needs e.g. `base: '/loomery/'`); the build is what the
# branch carries, so this script cannot correct it afterwards.
set -eu

BRANCH=gh-pages
PUSH=1
while [ $# -gt 0 ]; do
    case "$1" in
        --branch) BRANCH=${2:?--branch needs a name}; shift 2 ;;
        --no-push) PUSH=0; shift ;;
        -h|--help) sed -n '3,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "usage: $0 [--branch <name>] [--no-push]" >&2; exit 2 ;;
    esac
done

cd "$(dirname "$0")/.."
DIST=docs/.vitepress/dist
REMOTE=origin

# A branch checked out in a worktree cannot be moved under that worktree's feet:
# the ref would advance and its index would describe the previous tree.
if git worktree list --porcelain | grep -qx "branch refs/heads/$BRANCH"; then
    echo "publish-docs-site: '$BRANCH' is checked out in a worktree; refusing" >&2
    exit 1
fi

if [ ! -d docs/node_modules ]; then
    echo "publish-docs-site: installing the site toolchain"
    npm ci --prefix docs --no-audit --no-fund
fi
echo "publish-docs-site: building"
npm --prefix docs run build
[ -f "$DIST/index.html" ] || {
    echo "publish-docs-site: no build output in $DIST" >&2
    exit 1
}

# GitHub Pages serves a branch through Jekyll unless told not to. Other hosts
# ignore an empty file.
: >"$DIST/.nojekyll"

INDEX=$(mktemp)
trap 'rm -f "$INDEX"' EXIT
rm -f "$INDEX"
GIT_INDEX_FILE=$INDEX git --work-tree="$DIST" add -A -f -- .
TREE=$(GIT_INDEX_FILE=$INDEX git write-tree)

# Build on top of the remote branch when it exists, so publishing from a second
# machine (or a fresh clone) extends the deploy log instead of fighting it.
git fetch --quiet "$REMOTE" "$BRANCH" 2>/dev/null || true
PARENT=
if git rev-parse --verify --quiet "refs/remotes/$REMOTE/$BRANCH" >/dev/null; then
    PARENT=$(git rev-parse "refs/remotes/$REMOTE/$BRANCH")
elif git rev-parse --verify --quiet "refs/heads/$BRANCH" >/dev/null; then
    PARENT=$(git rev-parse "refs/heads/$BRANCH")
fi

MESSAGE="docs(site): publish $(git rev-parse --short HEAD)"
if [ -n "$PARENT" ]; then
    COMMIT=$(git commit-tree "$TREE" -p "$PARENT" -m "$MESSAGE")
else
    COMMIT=$(git commit-tree "$TREE" -m "$MESSAGE")
fi
git update-ref "refs/heads/$BRANCH" "$COMMIT"

FILES=$(git ls-tree -r --name-only "$BRANCH" | wc -l | tr -d ' ')
echo "publish-docs-site: $BRANCH at $(git rev-parse --short "$BRANCH"), $FILES files, $MESSAGE"

if [ "$PUSH" -eq 1 ]; then
    git push "$REMOTE" "refs/heads/$BRANCH:refs/heads/$BRANCH"
    echo "publish-docs-site: pushed to $REMOTE/$BRANCH — point the host at that branch"
else
    echo "publish-docs-site: not pushed (--no-push); push with:"
    echo "  git push $REMOTE refs/heads/$BRANCH:refs/heads/$BRANCH"
fi
