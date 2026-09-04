#!/usr/bin/env bash
# Optional self-declared operator identity on install.sh's faucet drip.
#
# The gateway records operator_label / contact / cardano_pool_id only on the
# INSERT that creates a registration, and this installer's drip is what creates
# it. So the three properties that matter are:
#
#   1. An operator who declares nothing sends a request byte-identical to the
#      pre-identity installer's — the anonymous path must not move.
#   2. The prompt never blocks a non-interactive install, and never suspends a
#      backgrounded one. Every documented invocation is `curl … | bash`, so
#      stdin is the pipe; the questions are read from /dev/tty, which only
#      works when this process group also owns that terminal.
#   3. The one identity-bearing drip is spent on an anonymous retry ONLY when
#      the gateway said a declared field was the problem. There is no update
#      path, so a retry stripped for any other reason burns it forever.
#
# The JSON builder, the prompt block and the drip block are extracted from
# install.sh and run for real, so this exercises the shipped code rather than a
# copy of it.
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
PASSES=0
pass() { PASSES=$((PASSES + 1)); echo "PASS: $*"; }

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
for fn in json_escape faucet_drip_body faucet_field faucet_drip_attempt faucet_refused_field; do
  extract_fn "$fn" > "$WORK/$fn.sh"
  [ -s "$WORK/$fn.sh" ] || fail "install.sh does not define $fn()"
done

# shellcheck disable=SC1090
build_body() {
  (
    set -euo pipefail
    SS58="$SS58"
    CONTACT_NAME="${1-}"
    CONTACT_HANDLE="${2-}"
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

# ── Extract the real prompt block, its terminal guard included ─────────────
sed -n '/^tty_is_ours() {/,/^fi$/p' "$INSTALL_SH" > "$WORK/prompt.sh"
[ -s "$WORK/prompt.sh" ] || fail "install.sh has no identity prompt block"
grep -q 'INVITE_TOKEN' "$WORK/prompt.sh" || fail "extracted block is not the prompt block"
grep -q '/dev/tty' "$WORK/prompt.sh" || fail "prompt block does not read from /dev/tty"

cat > "$WORK/harness.sh" <<'HARNESS'
set -euo pipefail
BOLD="" RESET=""
INVITE_TOKEN="" CONTACT_NAME="" CONTACT_HANDLE="" CARDANO_POOL_ID=""
. "$PROMPT_BLOCK"
echo "RESULT|${CONTACT_NAME}|${CONTACT_HANDLE}|${CARDANO_POOL_ID}"
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

# ── 5b. Backgrounded WITH a controlling terminal: skip, never suspend ──────
# Opening /dev/tty succeeds for a background process group — it is the read(2)
# that raises SIGTTIN, whose default disposition STOPS the process. So a probe
# that only opens /dev/tty passes here and then the install suspends on the
# first question, a hang the pre-identity installer could not produce.
# `bash -m` turns on job control so the harness really does land in its own
# process group; `cmd &` in a non-job-control shell would not move it.
if command -v script >/dev/null 2>&1; then
  cat > "$WORK/background.sh" <<'BG'
set -m
bash "$HARNESS" < /dev/null &
JOB=$!
wait "$JOB"
RC=$?
# 128+21 (SIGTTIN) means it was stopped, not finished.
echo "WAIT_RC=$RC"
kill -CONT "$JOB" 2>/dev/null || true
kill -KILL "$JOB" 2>/dev/null || true
BG
  OUT=$(PROMPT_BLOCK="$WORK/prompt.sh" HARNESS="$WORK/harness.sh" timeout 15 \
    script -qec "bash $WORK/background.sh" /dev/null < /dev/null 2>&1) \
    || fail "backgrounded prompt block did not finish: $OUT"
  echo "$OUT" | grep -q "WAIT_RC=0" \
    || fail "backgrounded install did not exit cleanly (SIGTTIN stop?): $OUT"
  echo "$OUT" | grep -q "RESULT|||" \
    || fail "backgrounded install never reached the drip: $OUT"
  echo "$OUT" | grep -qi "your name or organisation" \
    && fail "asked a question from a background process group — that read stops the install"
  pass "backgrounded with a terminal — questions skipped, no SIGTTIN suspension"
else
  echo "SKIP: script(1) unavailable, cannot allocate a pty"
fi

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
OUT=$(PROMPT_BLOCK="$WORK/prompt.sh" CONTACT_NAME=x timeout 10 bash -c '
  set -euo pipefail
  BOLD="" RESET=""
  INVITE_TOKEN="" CONTACT_HANDLE="" CARDANO_POOL_ID=""
  . "$PROMPT_BLOCK"
  echo "RESULT|${CONTACT_NAME}|${CONTACT_HANDLE}|${CARDANO_POOL_ID}"
' < /dev/null 2>&1) || fail "prompt block failed with identity pre-set: $OUT"
echo "$OUT" | grep -q "^RESULT|x||$" || fail "pre-set identity was disturbed: $OUT"
pass "identity supplied by flag or env is not re-asked"

# ── 8. An invite-token install never drips, so it is never asked ───────────
OUT=$(PROMPT_BLOCK="$WORK/prompt.sh" INVITE_TOKEN=tok timeout 10 bash -c '
  set -euo pipefail
  BOLD="" RESET=""
  CONTACT_NAME="" CONTACT_HANDLE="" CARDANO_POOL_ID=""
  . "$PROMPT_BLOCK"
  echo "RESULT|${CONTACT_NAME}|${CONTACT_HANDLE}|${CARDANO_POOL_ID}"
' < /dev/null 2>&1) || fail "prompt block failed with an invite token: $OUT"
echo "$OUT" | grep -q "^RESULT|||$" || fail "invite-token install was asked to declare: $OUT"
pass "invite-token install is not asked (that path does not drip)"

# ── 9. Non-interactive overrides are documented and parsed ─────────────────
for v in MATERIOS_CONTACT_NAME MATERIOS_CONTACT MATERIOS_CARDANO_POOL_ID; do
  grep -q "$v" "$INSTALL_SH" || fail "install.sh does not honour \$$v"
done
for f in --contact-name --contact --pool-id; do
  grep -q -- "$f)" "$INSTALL_SH" || fail "install.sh does not parse $f"
done
HELP_OUT=$(bash "$INSTALL_SH" --help)
for f in --contact-name --contact --pool-id; do
  echo "$HELP_OUT" | grep -q -- "$f" || fail "--help does not document $f"
done
echo "$HELP_OUT" | grep -qi "optional" || fail "--help does not mark the fields optional"
pass "env-var overrides and flags parsed and documented for automated installs"

# ── 9b. --operator-label belongs to bootstrap-validator.sh, not here ───────
# bootstrap-validator.sh has taken --operator-label since before this feature
# (it becomes the systemd Description and the node's --name) and the published
# SPO docs tell operators to pass it. install.sh must not quietly give the same
# spelling a second, different meaning.
grep -q -- '--operator-label)[[:space:]]*CONTACT_NAME=' "$INSTALL_SH" \
  && fail "install.sh silently aliases --operator-label to the contact name"
OUT=$(bash "$INSTALL_SH" --operator-label OnlyBlocks 2>&1) && OUT_RC=0 || OUT_RC=$?
[ "$OUT_RC" -ne 0 ] || fail "install.sh accepted --operator-label instead of refusing it"
echo "$OUT" | grep -q -- "--contact-name" \
  || fail "refusal does not point at install.sh's own flag: $OUT"
echo "$OUT" | grep -q "bootstrap-validator.sh" \
  || fail "refusal does not explain whose flag --operator-label is: $OUT"
# Trailing --operator-label with no value must still get the explanation, not a
# `set -u` unbound-variable trace.
OUT=$(bash "$INSTALL_SH" --operator-label 2>&1) && OUT_RC=0 || OUT_RC=$?
[ "$OUT_RC" -ne 0 ] || fail "install.sh accepted a valueless --operator-label"
echo "$OUT" | grep -q -- "--contact-name" \
  || fail "valueless --operator-label produced a shell error, not the explanation: $OUT"
pass "--operator-label is refused with both meanings named, not silently reused"

# ── 10. Only a 400 that names a declared field is an identity rejection ────
# Identity is written on INSERT and there is no update path, so re-sending the
# drip stripped spends the address's only chance to be recorded. The bodies
# below are the gateway's real ones (src/operator_identity.ts and the
# /faucet/drip route in src/routes/faucet.ts).
# shellcheck disable=SC1090
refused_field() {
  (
    set -euo pipefail
    . "$WORK/faucet_refused_field.sh"
    faucet_refused_field "$1" "$2"
  )
}

assert_refused() {
  local want=$1 status=$2 body=$3 got
  got=$(refused_field "$status" "$body")
  [ "$got" = "$want" ] \
    || fail "status $status body '$body': expected refused-field '$want', got '$got'"
}

# Rejections of a declared field — these, and only these, may be retried.
assert_refused operator_label 400 \
  '{"error":"operator_label must be at most 64 characters"}'
assert_refused operator_label 400 \
  '{"error":"operator_label must not contain angle brackets, ampersands, quotes or control characters"}'
assert_refused operator_label 400 '{"error":"operator_label must be a string"}'
assert_refused contact 400 '{"error":"contact must be at most 128 characters"}'
assert_refused contact 400 '{"error":"contact must be a string"}'
assert_refused cardano_pool_id 400 \
  "{\"error\":\"cardano_pool_id must be a Cardano pool id: bech32 'pool1' + 51 characters, or the 56-character hex pool hash\"}"
assert_refused cardano_pool_id 400 '{"error":"cardano_pool_id must be at most 64 characters"}'

# Everything else. Each of these once triggered an anonymous re-drip.
assert_refused "" 400 '{"error":"Invalid SS58 address"}'
assert_refused "" 429 '{"error":"Faucet cooldown active for this IP","retry_after_seconds":3600}'
assert_refused "" 503 '{"error":"Faucet not connected to chain: websocket closed"}'
assert_refused "" 503 '{"error":"Faucet balance too low"}'
assert_refused "" 500 '{"error":"Faucet error: 1010: Invalid Transaction"}'
assert_refused "" 500 \
  '{"error":"Drip succeeded but operator registration failed: disk I/O error. Retry — ledger rolled back.","tx_hash":"0xabc"}'
assert_refused "" 409 '{"error":"Address already received a drip","dripped_at":1}'
assert_refused "" 000 ''
assert_refused "" 000 'curl: (28) Operation timed out'
assert_refused "" 502 '<html><body>502 Bad Gateway</body></html>'
assert_refused "" 200 '{"success":true,"identity_status":"recorded"}'
# A 400 that merely mentions a field name somewhere is not the gateway naming it.
assert_refused "" 400 '{"error":"Invalid SS58 address (operator_label was fine)"}'
# Nothing behind the gateway is required to phrase its errors carefully. A
# proxy, WAF or load balancer sitting in front of it can return anything, and
# "contact …" is an ordinary way for one to start a sentence — which is why the
# status has to gate the match and not the wording alone.
assert_refused "" 503 '{"error":"contact your administrator, the service is unavailable"}'
assert_refused "" 502 '{"error":"operator_label service temporarily unavailable"}'
assert_refused "" 429 '{"error":"contact support to raise your rate limit"}'
pass "only a 400 whose error names a declared field counts as an identity rejection"

# ── 11. End-to-end: the drip block against a stub gateway ─────────────────
# The real curl, the real status codes, the real block out of install.sh.
sed -n '/^# Request faucet drip/,/^fi$/p' "$INSTALL_SH" > "$WORK/drip.sh"
[ -s "$WORK/drip.sh" ] || fail "install.sh has no faucet drip block"
extract_fn faucet_drip_request > "$WORK/faucet_drip_request.sh"
[ -s "$WORK/faucet_drip_request.sh" ] || fail "install.sh does not define faucet_drip_request()"
grep -q 'http_code' "$WORK/faucet_drip_request.sh" \
  || fail "the drip request does not capture the HTTP status"

cat > "$WORK/stub_gateway.py" <<'PY'
import json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

SCRIPT = json.load(open(os.environ["RESPONSES"]))
REQUESTS = os.environ["REQUESTS"]


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        with open(REQUESTS, "a") as fh:
            fh.write(body.decode() + "\n")
        reply = SCRIPT[min(len(open(REQUESTS).readlines()) - 1, len(SCRIPT) - 1)]
        payload = reply["body"].encode()
        self.send_response(reply["status"])
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_):
        pass


srv = HTTPServer(("127.0.0.1", 0), Handler)
with open(os.environ["PORTFILE"], "w") as fh:
    fh.write(str(srv.server_address[1]))
srv.serve_forever()
PY

# Runs the shipped drip block against the stub and reports what it did.
run_drip() {
  local responses=$1 name=$2 handle=$3 pool=$4 declared=false
  [ -n "${name}${handle}${pool}" ] && declared=true
  local dir="$WORK/e2e-$RANDOM"
  mkdir -p "$dir"
  printf '%s' "$responses" > "$dir/responses.json"
  : > "$dir/requests.log"
  RESPONSES="$dir/responses.json" REQUESTS="$dir/requests.log" PORTFILE="$dir/port" \
    python3 "$WORK/stub_gateway.py" &
  local srv=$!
  local waited=0
  while [ ! -s "$dir/port" ]; do
    sleep 0.1
    waited=$((waited + 1))
    [ "$waited" -lt 50 ] || { kill "$srv" 2>/dev/null || true; fail "stub gateway did not start"; }
  done
  local port
  port=$(cat "$dir/port")

  SS58="$SS58" GATEWAY_URL="http://127.0.0.1:$port" \
  CONTACT_NAME="$name" CONTACT_HANDLE="$handle" CARDANO_POOL_ID="$pool" \
  IDENTITY_DECLARED="$declared" WORK="$WORK" \
  timeout 60 bash -c '
    set -euo pipefail
    info() { echo "[info] $*"; }
    ok()   { echo "[ok] $*"; }
    warn() { echo "[warn] $*"; }
    API_KEY=""
    . "$WORK/json_escape.sh"
    . "$WORK/faucet_drip_body.sh"
    . "$WORK/faucet_field.sh"
    . "$WORK/faucet_drip_request.sh"
    . "$WORK/faucet_drip_attempt.sh"
    . "$WORK/faucet_refused_field.sh"
    . "$WORK/drip.sh"
  ' > "$dir/out.txt" 2>&1 || true

  kill "$srv" 2>/dev/null || true
  wait "$srv" 2>/dev/null || true
  DRIP_DIR="$dir"
}

drip_requests() { wc -l < "$DRIP_DIR/requests.log" | tr -d ' '; }
drip_request()  { sed -n "${1}p" "$DRIP_DIR/requests.log"; }
drip_output()   { cat "$DRIP_DIR/out.txt"; }

# A refused field is dropped, the rest of the declaration is kept, and the
# retry succeeds.
run_drip '[{"status":400,"body":"{\"error\":\"cardano_pool_id must be at most 64 characters\"}"},
           {"status":200,"body":"{\"success\":true,\"identity_status\":\"recorded\"}"}]' \
  "OnlyBlocks" "ops@example.org" "not-a-pool-id"
[ "$(drip_requests)" = "2" ] || fail "refused field: expected 2 requests, got $(drip_requests) — $(drip_output)"
python3 - "$(drip_request 2)" <<'PY' || fail "retry did not keep the accepted fields"
import json, sys
b = json.loads(sys.argv[1])
assert "cardano_pool_id" not in b, b
assert b["operator_label"] == "OnlyBlocks", b
assert b["contact"] == "ops@example.org", b
PY
drip_output | grep -q "cardano_pool_id" \
  || fail "operator was not told which field was refused: $(drip_output)"
pass "a refused field is named, dropped, and the rest of the declaration survives"

# ── 12. The retry loop terminates against a responder that never relents ───
# Nothing obliges the far end to stop naming a field once we stop sending it —
# a stale cache, a WAF or a rolled-back gateway can repeat the same 400
# forever. The loop only makes progress when a pass empties a field the request
# was actually carrying, so this must stop at 2 requests: the declaration, then
# one retry without the pool id, whose refusal names a field no longer present.
run_drip '[{"status":400,"body":"{\"error\":\"cardano_pool_id must be at most 64 characters\"}"}]' \
  "OnlyBlocks" "ops@example.org" "pool1abc"
[ "$(drip_requests)" = "2" ] \
  || fail "unrelenting refusal: expected 2 requests, got $(drip_requests) — the loop does not terminate"
python3 - "$(drip_request 2)" <<'PY' || fail "the one retry did not keep the accepted fields"
import json, sys
b = json.loads(sys.argv[1])
assert "cardano_pool_id" not in b, b
assert b["operator_label"] == "OnlyBlocks", b
assert b["contact"] == "ops@example.org", b
PY
drip_output | grep -qi "re-run" \
  || fail "operator was not told what to do after the loop gave up: $(drip_output)"
pass "a responder that keeps naming a dropped field stops the loop instead of spinning"

# Everything that is not the gateway refusing a field must leave the one
# identity-bearing INSERT unspent.
for scenario in \
  '429|{"error":"Faucet cooldown active for this IP","retry_after_seconds":3600}' \
  '503|{"error":"Faucet not connected to chain: connection refused"}' \
  '503|{"error":"Faucet balance too low"}' \
  '500|{"error":"Faucet error: 1010: Invalid Transaction"}' \
  '400|{"error":"Invalid SS58 address"}' \
  '503|{"error":"contact your administrator, the service is unavailable"}' \
  '502|<html>502 Bad Gateway</html>' ; do
  status=${scenario%%|*}
  body=${scenario#*|}
  run_drip "[{\"status\":$status,\"body\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$body")}]" \
    "OnlyBlocks" "ops@example.org" "pool1abc"
  [ "$(drip_requests)" = "1" ] \
    || fail "HTTP $status: re-dripped anonymously ($(drip_requests) requests) — $(drip_output)"
  drip_output | grep -qi "re-run this installer" \
    || fail "HTTP $status: operator was not told they can retry: $(drip_output)"
done
pass "a cooldown, an unhealthy chain, a failed transfer or a bad gateway never burns the declaration"

# A dead endpoint (connection refused) is the same class: no second attempt.
DEAD_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
OUT=$(SS58="$SS58" GATEWAY_URL="http://127.0.0.1:$DEAD_PORT" \
  CONTACT_NAME="OnlyBlocks" CONTACT_HANDLE="" CARDANO_POOL_ID="" \
  IDENTITY_DECLARED=true WORK="$WORK" timeout 60 bash -c '
    set -euo pipefail
    info() { echo "[info] $*"; }
    ok()   { echo "[ok] $*"; }
    warn() { echo "[warn] $*"; }
    API_KEY=""
    . "$WORK/json_escape.sh"
    . "$WORK/faucet_drip_body.sh"
    . "$WORK/faucet_field.sh"
    . "$WORK/faucet_drip_request.sh"
    . "$WORK/faucet_drip_attempt.sh"
    . "$WORK/faucet_refused_field.sh"
    . "$WORK/drip.sh"
  ' 2>&1) || fail "drip block aborted the install on a network failure: $OUT"
echo "$OUT" | grep -qi "re-run this installer" \
  || fail "network failure did not tell the operator to retry: $OUT"
pass "an unreachable gateway leaves the declaration unspent and says to re-run"

# The anonymous install must be untouched by all of this.
run_drip '[{"status":200,"body":"{\"success\":true,\"identity_status\":\"not_declared\"}"}]' "" "" ""
[ "$(drip_requests)" = "1" ] || fail "anonymous drip made $(drip_requests) requests"
[ "$(drip_request 1)" = "$PRE_IDENTITY_BODY" ] \
  || fail "anonymous drip body changed: $(drip_request 1)"
pass "an install that declares nothing sends exactly one pre-identity request"

echo ""
echo "All $PASSES faucet-identity checks passed."
