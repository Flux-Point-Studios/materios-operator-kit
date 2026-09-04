#!/usr/bin/env bash
# Optional self-declared operator identity on install.sh's faucet drip.
#
# The gateway records operator_label / contact / cardano_pool_id only on the
# INSERT that creates a registration, and this installer's drip is what creates
# it. So the two properties that matter are:
#
#   1. An operator who declares nothing sends a request byte-identical to the
#      pre-identity installer's — the anonymous path must not move.
#   2. The prompt never blocks a non-interactive install. Every documented
#      invocation is `curl … | bash`, so stdin is the pipe; the questions are
#      read from /dev/tty, and where there is no controlling terminal (CI,
#      cron, `docker run -d`) they are skipped entirely.
#
# The JSON builder and the prompt block are extracted from install.sh and run
# for real, so this exercises the shipped code rather than a copy of it.
#
# Run:
#   bash tests/test_faucet_identity_prompt.sh
#   INSTALL_SH=/path/to/other/install.sh bash tests/test_faucet_identity_prompt.sh
#
# Exits 0 on success, non-zero on any assertion failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_SH="${INSTALL_SH:-$REPO_ROOT/install.sh}"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

SS58="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
# What install.sh sent before identity capture existed. Every anonymous drip
# must still produce exactly this.
PRE_IDENTITY_BODY="{\"address\": \"${SS58}\"}"

# ── 0. Syntax ──────────────────────────────────────────────────────────────
bash -n "$INSTALL_SH" || fail "install.sh is not valid bash"
pass "install.sh parses"

# ── Extract the real body builder out of install.sh ────────────────────────
extract_fn() {
  sed -n "/^$1() {/,/^}/p" "$INSTALL_SH"
}
for fn in json_escape faucet_drip_body; do
  extract_fn "$fn" > "$WORK/$fn.sh"
  [ -s "$WORK/$fn.sh" ] || fail "install.sh does not define $fn()"
done

# shellcheck disable=SC1090
build_body() {
  (
    set -euo pipefail
    SS58="$SS58"
    OPERATOR_LABEL="${1-}"
    OPERATOR_CONTACT="${2-}"
    CARDANO_POOL_ID="${3-}"
    . "$WORK/json_escape.sh"
    . "$WORK/faucet_drip_body.sh"
    faucet_drip_body
  )
}

# ── 1. The anonymous drip is byte-identical to the pre-identity request ────
BODY=$(build_body "" "" "")
[ "$BODY" = "$PRE_IDENTITY_BODY" ] \
  || fail "anonymous body changed: got '$BODY', want '$PRE_IDENTITY_BODY'"
pass "anonymous drip body is byte-identical to the pre-identity installer's"

# ── 2. Declared fields ride along under the gateway's key names ────────────
BODY=$(build_body "OnlyBlocks" "ops@example.org" "pool1abc")
python3 - "$BODY" <<'PY' || fail "declared body is not the expected JSON"
import json, sys
b = json.loads(sys.argv[1])
assert b["operator_label"] == "OnlyBlocks", b
assert b["contact"] == "ops@example.org", b
assert b["cardano_pool_id"] == "pool1abc", b
assert set(b) == {"address", "operator_label", "contact", "cardano_pool_id"}, b
PY
pass "all three declared fields are forwarded under the gateway's key names"

# ── 3. Each field can be declared on its own ───────────────────────────────
for i in 1 2 3; do
  case $i in
    1) BODY=$(build_body "OnlyBlocks" "" ""); KEY=operator_label ;;
    2) BODY=$(build_body "" "ops@example.org" ""); KEY=contact ;;
    3) BODY=$(build_body "" "" "pool1abc"); KEY=cardano_pool_id ;;
  esac
  KEY="$KEY" python3 - "$BODY" <<'PY' || fail "single-field body wrong: $BODY"
import json, os, sys
b = json.loads(sys.argv[1])
assert set(b) == {"address", os.environ["KEY"]}, b
PY
done
pass "a single declared field is sent alone, the others omitted"

# ── 4. Quotes and backslashes cannot break the JSON ────────────────────────
# The gateway rejects these characters and names the field it rejected; that
# only works if the request parses as JSON in the first place.
BODY=$(build_body 'He said "hi"' 'back\slash' "")
python3 - "$BODY" <<'PY' || fail "quoted/escaped values did not survive as JSON"
import json, sys
b = json.loads(sys.argv[1])
assert b["operator_label"] == 'He said "hi"', b
assert b["contact"] == "back\\slash", b
PY
pass "embedded quotes and backslashes round-trip as valid JSON"

# ── Extract the real prompt block ──────────────────────────────────────────
sed -n '/^if \[ -z "\$INVITE_TOKEN" \] &&/,/^fi$/p' "$INSTALL_SH" > "$WORK/prompt.sh"
[ -s "$WORK/prompt.sh" ] || fail "install.sh has no identity prompt block"
grep -q '/dev/tty' "$WORK/prompt.sh" || fail "prompt block does not read from /dev/tty"

cat > "$WORK/harness.sh" <<'HARNESS'
set -euo pipefail
BOLD="" RESET=""
INVITE_TOKEN="" OPERATOR_LABEL="" OPERATOR_CONTACT="" CARDANO_POOL_ID=""
. "$PROMPT_BLOCK"
echo "RESULT|${OPERATOR_LABEL}|${OPERATOR_CONTACT}|${CARDANO_POOL_ID}"
HARNESS

# ── 5. No controlling terminal: skip the questions, never block ────────────
# setsid detaches from any controlling terminal, which is the CI / cron /
# `docker run -d` case. A `read` on stdin here would hang until the timeout.
OUT=$(PROMPT_BLOCK="$WORK/prompt.sh" timeout 10 setsid bash "$WORK/harness.sh" < /dev/null 2>&1) \
  || fail "prompt block blocked or failed with no controlling terminal: $OUT"
echo "$OUT" | grep -q "^RESULT|||$" || fail "expected empty identity with no tty, got: $OUT"
echo "$OUT" | grep -qi "your name or organisation" \
  && fail "asked a question with no terminal to answer it"
pass "no controlling terminal — questions skipped, identity stays empty, no hang"

# ── 6. Piped stdin with a terminal present: still asks, and gets answers ───
# This is the shipped `curl … | bash` shape: stdin is a pipe, /dev/tty is the
# operator's terminal. `script` supplies a pty so it can be exercised for real.
if command -v script >/dev/null 2>&1; then
  ANSWERS=$'OnlyBlocks\nops@example.org\npool1abc\n'
  OUT=$(printf '%s' "$ANSWERS" \
    | PROMPT_BLOCK="$WORK/prompt.sh" timeout 10 script -qec "bash $WORK/harness.sh < /dev/null" /dev/null 2>&1) \
    || fail "prompt block failed under a pty: $OUT"
  echo "$OUT" | grep -q "RESULT|OnlyBlocks|ops@example.org|pool1abc" \
    || fail "answers typed at the terminal were not captured: $OUT"
  echo "$OUT" | grep -qi "contact you about becoming a Materios validator" \
    || fail "prompt does not say what the answers are used for"
  pass "terminal present, stdin piped — questions asked and answers captured"
else
  echo "SKIP: script(1) unavailable, cannot allocate a pty"
fi

# ── 7. Already-supplied identity is not re-asked ───────────────────────────
OUT=$(PROMPT_BLOCK="$WORK/prompt.sh" OPERATOR_LABEL=x timeout 10 bash -c '
  set -euo pipefail
  BOLD="" RESET=""
  INVITE_TOKEN="" OPERATOR_CONTACT="" CARDANO_POOL_ID=""
  . "$PROMPT_BLOCK"
  echo "RESULT|${OPERATOR_LABEL}|${OPERATOR_CONTACT}|${CARDANO_POOL_ID}"
' < /dev/null 2>&1) || fail "prompt block failed with identity pre-set: $OUT"
echo "$OUT" | grep -q "^RESULT|x||$" || fail "pre-set identity was disturbed: $OUT"
pass "identity supplied by flag or env is not re-asked"

# ── 8. An invite-token install never drips, so it is never asked ───────────
OUT=$(PROMPT_BLOCK="$WORK/prompt.sh" INVITE_TOKEN=tok timeout 10 bash -c '
  set -euo pipefail
  BOLD="" RESET=""
  OPERATOR_LABEL="" OPERATOR_CONTACT="" CARDANO_POOL_ID=""
  . "$PROMPT_BLOCK"
  echo "RESULT|${OPERATOR_LABEL}|${OPERATOR_CONTACT}|${CARDANO_POOL_ID}"
' < /dev/null 2>&1) || fail "prompt block failed with an invite token: $OUT"
echo "$OUT" | grep -q "^RESULT|||$" || fail "invite-token install was asked to declare: $OUT"
pass "invite-token install is not asked (that path does not drip)"

# ── 9. Non-interactive overrides are documented and parsed ─────────────────
for v in MATERIOS_OPERATOR_LABEL MATERIOS_OPERATOR_CONTACT MATERIOS_CARDANO_POOL_ID; do
  grep -q "$v" "$INSTALL_SH" || fail "install.sh does not honour \$$v"
done
for f in --operator-label --contact --pool-id; do
  grep -q -- "$f)" "$INSTALL_SH" || fail "install.sh does not parse $f"
done
HELP_OUT=$(bash "$INSTALL_SH" --help)
for f in --operator-label --contact --pool-id; do
  echo "$HELP_OUT" | grep -q -- "$f" || fail "--help does not document $f"
done
echo "$HELP_OUT" | grep -qi "optional" || fail "--help does not mark the fields optional"
pass "env-var overrides and flags parsed and documented for automated installs"

# ── 10. A refused declaration must not leave the operator unfunded ─────────
grep -q 'IDENTITY_DECLARED' "$INSTALL_SH" || fail "install.sh does not track whether identity was declared"
grep -q 'Re-requesting without them so your node is still funded' "$INSTALL_SH" \
  || fail "install.sh has no anonymous retry when the gateway refuses the details"
pass "gateway refusal of the optional details falls back to an anonymous drip"

echo ""
echo "All $(grep -c '^pass ' "$0") faucet-identity checks passed."
