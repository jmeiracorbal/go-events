#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
cat > "$dir/gh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1 $2" >> "$GH_TEST_LOG"
case "$GH_TEST_CASE" in
  existing)
    [[ "$2" == view ]]
    ;;
  missing)
    [[ "$2" == create ]]
    ;;
  concurrent)
    if [[ "$2" == view ]]; then
      count="$(grep -c '^release view$' "$GH_TEST_LOG")"
      [[ "$count" -ge 2 ]]
    else
      exit 1
    fi
    ;;
  denied)
    exit 1
    ;;
  *) exit 2 ;;
esac
MOCK
chmod +x "$dir/gh"
export GH_TEST_LOG="$dir/commands.log" PATH="$dir:$PATH"
assert_calls() {
  local actual
  actual="$(cat "$GH_TEST_LOG")"
  if [[ "$actual" != "$1" ]]; then
    printf 'unexpected gh calls\nactual:\n%s\nexpected:\n%s\n' "$actual" "$1" >&2
    exit 1
  fi
}
export GH_TEST_CASE=existing
: > "$GH_TEST_LOG"
bash "$root/.github/scripts/publish-release.sh" v0.1.0
assert_calls 'release view'
export GH_TEST_CASE=missing
: > "$GH_TEST_LOG"
bash "$root/.github/scripts/publish-release.sh" v0.1.0
assert_calls $'release view\nrelease create'
export GH_TEST_CASE=concurrent
: > "$GH_TEST_LOG"
bash "$root/.github/scripts/publish-release.sh" v0.1.0
assert_calls $'release view\nrelease create\nrelease view'
export GH_TEST_CASE=denied
: > "$GH_TEST_LOG"
if bash "$root/.github/scripts/publish-release.sh" v0.1.0; then
  echo 'A failed publication must fail the workflow.' >&2
  exit 1
fi
assert_calls $'release view\nrelease create\nrelease view'
echo 'All release idempotency cases passed.'
