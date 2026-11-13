#!/usr/bin/env bash
set -euo pipefail

tag="${1:?release tag is required}"

if gh release view "$tag" >/dev/null 2>&1; then
  echo "GitHub Release $tag already exists; nothing to publish."
  exit 0
fi

if gh release create "$tag" --verify-tag --generate-notes --title "$tag"; then
  echo "GitHub Release $tag published."
  exit 0
fi

# Another publisher may have created the release between the two operations.
if gh release view "$tag" >/dev/null 2>&1; then
  echo "GitHub Release $tag was created concurrently."
  exit 0
fi

echo "Failed to publish or verify GitHub Release $tag." >&2
exit 1
