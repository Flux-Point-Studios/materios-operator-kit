#!/usr/bin/env bash
#
# Materios Attestor Watchdog
#
# Checks daemon health every 60s and alerts via Discord webhook,
# email (sendmail/msmtp), or stdout when issues are detected.
#
# Setup:
#   1. Set ALERT_METHOD and the relevant config below
#   2. chmod +x watchdog.sh
#   3. Run alongside your daemon:  ./watchdog.sh &
#      Or add as a service in docker-compose.yml (see README)
#
# Checks:
#   - Daemon container running
#   - Health endpoint reachable
#   - RPC connected
#   - Block finality gap < threshold
#   - Last poll not stale (daemon actually processing)

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

# How to alert: "discord", "email", or "stdout"
ALERT_METHOD="${ALERT_METHOD:-stdout}"

# Discord webhook URL (create one in Server Settings → Integrations → Webhooks)
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"

# Email config (requires sendmail, msmtp, or similar MTA on the host)
ALERT_EMAIL="${ALERT_EMAIL:-}"
ALERT_FROM="${ALERT_FROM:-materios-watchdog@localhost}"

# Daemon health endpoint (default: local docker port mapping)
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/status}"

# Check interval in seconds
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"

# Thresholds
MAX_FINALITY_GAP="${MAX_FINALITY_GAP:-10}"
MAX_POLL_AGE_SECONDS="${MAX_POLL_AGE_SECONDS:-120}"

# Suppress repeated alerts for the same issue (seconds)
ALERT_COOLDOWN="${ALERT_COOLDOWN:-300}"

# Operator label (shown in alerts)
OPERATOR_LABEL="${OPERATOR_LABEL:-my-attestor}"

# ─── Internal state ─────────────────────────────────────────────────────────

_last_alert_time=0
_last_alert_msg=""
_consecutive_failures=0

# ─── Alert functions ─────────────────────────────────────────────────────────

send_alert() {
    local level="$1"  # WARN, CRIT, OK
    local message="$2"
    local now
    now=$(date +%s)

    # Cooldown: don't repeat same alert within window (except OK = recovery)
    if [[ "$level" != "OK" && "$message" == "$_last_alert_msg" ]]; then
        local elapsed=$(( now - _last_alert_time ))
        if (( elapsed < ALERT_COOLDOWN )); then
            return
        fi
    fi

    _last_alert_time=$now
    _last_alert_msg="$message"

    local timestamp
    timestamp=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
    local full_msg="[$level] $OPERATOR_LABEL — $message ($timestamp)"

    case "$ALERT_METHOD" in
        discord)
            send_discord "$level" "$message" "$timestamp"
            ;;
        email)
            send_email "$level" "$full_msg"
            ;;
        stdout|*)
            echo "$full_msg"
            ;;
    esac
}

send_discord() {
    local level="$1"
    local message="$2"
    local timestamp="$3"

    if [[ -z "$DISCORD_WEBHOOK_URL" ]]; then
        echo "[watchdog] DISCORD_WEBHOOK_URL not set, falling back to stdout"
        echo "[$level] $OPERATOR_LABEL — $message ($timestamp)"
        return
    fi

    local color
    case "$level" in
        CRIT) color=15158332 ;;  # red
        WARN) color=16776960 ;;  # yellow
        OK)   color=3066993  ;;  # green
    esac

    local payload
    payload=$(cat <<ENDJSON
{
  "embeds": [{
    "title": "Materios Attestor: $level",
    "description": "$message",
    "color": $color,
    "footer": {"text": "$OPERATOR_LABEL · $timestamp"}
  }]
}
ENDJSON
)

    curl -s -o /dev/null -X POST "$DISCORD_WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "$payload" 2>/dev/null || true
}

send_email() {
    local level="$1"
    local body="$2"

    if [[ -z "$ALERT_EMAIL" ]]; then
        echo "[watchdog] ALERT_EMAIL not set, falling back to stdout"
        echo "$body"
        return
    fi

    {
        echo "From: $ALERT_FROM"
        echo "To: $ALERT_EMAIL"
        echo "Subject: [Materios $level] $OPERATOR_LABEL"
        echo ""
        echo "$body"
    } | sendmail -t 2>/dev/null || \
    echo "$body" | mail -s "[Materios $level] $OPERATOR_LABEL" "$ALERT_EMAIL" 2>/dev/null || \
    echo "[watchdog] Failed to send email. Message: $body"
}

# ─── Health check ────────────────────────────────────────────────────────────

check_health() {
    # 1. Check if daemon container is running
    if command -v docker &>/dev/null; then
        local container_running
        container_running=$(docker compose ps --format json 2>/dev/null | grep -c '"running"' || echo "0")
        if [[ "$container_running" == "0" ]]; then
            send_alert "CRIT" "Daemon container is NOT running. Run: docker compose up -d"
            _consecutive_failures=$(( _consecutive_failures + 1 ))
            return 1
        fi
    fi

    # 2. Query health endpoint
    local response
    response=$(curl -s --max-time 5 "$HEALTH_URL" 2>/dev/null) || {
        send_alert "CRIT" "Health endpoint unreachable at $HEALTH_URL"
        _consecutive_failures=$(( _consecutive_failures + 1 ))
        return 1
    }

    # 3. Parse response (works with basic tools, no jq required)
    local connected best_block finality_gap last_poll pending certs

    connected=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('connected', False))" 2>/dev/null || echo "false")
    best_block=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('bestBlock', 0))" 2>/dev/null || echo "0")
    finality_gap=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('finalityGap', -1))" 2>/dev/null || echo "-1")
    last_poll=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d.get('lastPollTimestamp', 0)))" 2>/dev/null || echo "0")
    pending=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('pendingReceipts', 0))" 2>/dev/null || echo "0")
    certs=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('certsSubmitted', 0))" 2>/dev/null || echo "0")

    local issues=()

    # 4. Check RPC connection
    if [[ "$connected" != "True" && "$connected" != "true" ]]; then
        issues+=("RPC disconnected — daemon cannot reach Materios chain")
    fi

    # 5. Check finality gap
    if (( finality_gap > MAX_FINALITY_GAP )); then
        issues+=("Finality gap is $finality_gap (threshold: $MAX_FINALITY_GAP)")
    fi

    # 6. Check poll freshness
    if (( last_poll > 0 )); then
        local now
        now=$(date +%s)
        local poll_age=$(( now - last_poll ))
        if (( poll_age > MAX_POLL_AGE_SECONDS )); then
            issues+=("Last poll was ${poll_age}s ago (threshold: ${MAX_POLL_AGE_SECONDS}s) — daemon may be stuck")
        fi
    fi

    # 7. Report
    if (( ${#issues[@]} > 0 )); then
        local msg
        msg=$(printf '%s; ' "${issues[@]}")
        msg="${msg%; }"
        send_alert "WARN" "$msg [block=$best_block gap=$finality_gap pending=$pending certs=$certs]"
        _consecutive_failures=$(( _consecutive_failures + 1 ))
        return 1
    fi

    # All good — send recovery alert if we were previously failing
    if (( _consecutive_failures > 0 )); then
        send_alert "OK" "Daemon recovered — block=$best_block gap=$finality_gap certs=$certs"
    fi
    _consecutive_failures=0
    return 0
}

# ─── Main loop ───────────────────────────────────────────────────────────────

echo "[watchdog] Starting Materios attestor watchdog"
echo "[watchdog] Health URL: $HEALTH_URL"
echo "[watchdog] Alert method: $ALERT_METHOD"
echo "[watchdog] Check interval: ${CHECK_INTERVAL}s"
echo "[watchdog] Operator: $OPERATOR_LABEL"

while true; do
    check_health || true
    sleep "$CHECK_INTERVAL"
done
