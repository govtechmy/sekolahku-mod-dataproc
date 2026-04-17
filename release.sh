#!/bin/bash

# ─────────────────────────────────────────
# Release Script
# ─────────────────────────────────────────
# Check / update the version below before running:
# 8 April 2025
VERSION="v1.2.1"   # <── change this version before running
# ─────────────────────────────────────────

set -euo pipefail

RELEASE_BRANCH="release/${VERSION}"

error() {
  echo "ERROR: $1" >&2
  exit 1
}

echo "Starting release process for ${VERSION}..."

git checkout develop        || error "Failed to checkout 'develop'"
git pull origin develop     || error "Failed to pull 'develop' from origin"

git checkout main           || error "Failed to checkout 'main'"
git pull origin main        || error "Failed to pull 'main' from origin"

git merge develop           || error "Failed to merge 'develop' into 'main'"

git checkout -B "${RELEASE_BRANCH}"         || error "Failed to create branch '${RELEASE_BRANCH}'"
git push origin "${RELEASE_BRANCH}"         || error "Failed to push branch '${RELEASE_BRANCH}' to origin"

git tag -a "${VERSION}" -m "release ${VERSION}"   || error "Failed to create tag '${VERSION}'"
git push origin "${VERSION}"                       || error "Failed to push tag '${VERSION}' to origin"

echo "Release ${VERSION} completed successfully."
echo "Branch: ${RELEASE_BRANCH}"
echo "Tag:    ${VERSION}"
