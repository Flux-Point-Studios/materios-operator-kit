#!/usr/bin/env bash
# Materios Operator — One-Command Update
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/update.sh | bash
#
# What this does:
#   1. Detects your install (validator or attestor)
#   2. Pulls latest Docker images
#   3. Checks for chain fork (genesis mismatch)
#   4. If fork detected: downloads new chain spec, wipes chain data, keeps keystore
#   5. Restarts all services
#   6. Regenerates session keys if needed (validators only)
#
set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────────────
GATEWAY_URL="https://materios.fluxpointstudios.com/blobs"
CHAIN_SPEC_URL="https://raw.githubusercontent.com/Flux-Point-Studios/materios/main/chain-spec/chain-spec-raw.json"

# ── Colors ───────────────────────────────────────────────────────────────────
if [ -t 1 ]; then
  BOLD="\033[1m" GREEN="\033[32m" RED="\033[31m" YELLOW="\033[33m" CYAN="\033[36m" RESET="\033[0m"
else
  BOLD="" GREEN="" RED="" YELLOW="" CYAN="" RESET=""
fi
info()  { echo -e "${CYAN}[materios]${RESET} $*"; }
ok()    { echo -e "${GREEN}[OK]${RESET} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${RESET} $*"; }
fail()  { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

# ── Detect install ───────────────────────────────────────────────────────────
OPERATOR_DIR=""
MODE=""
if [ -f "$HOME/materios-operator/docker-compose.yml" ]; then
  OPERATOR_DIR="$HOME/materios-operator"
  if grep -q "materios-node" "$OPERATOR_DIR/docker-compose.yml" 2>/dev/null; then
    MODE="validator"
  else
    MODE="attestor"
  fi
elif [ -f "$HOME/materios-attestor/docker-compose.yml" ]; then
  OPERATOR_DIR="$HOME/materios-attestor"
  MODE="attestor"
else
  fail "No Materios installation found at ~/materios-operator or ~/materios-attestor"
fi

echo ""
echo -e "${BOLD}  Materios Operator Update${RESET}"
echo "  ────────────────────────"
echo ""
info "Install: ${OPERATOR_DIR} (${MODE})"

cd "$OPERATOR_DIR"

# ── Step 1: Pull latest images ───────────────────────────────────────────────
info "Pulling latest images..."
docker compose pull 2>&1 | tail -3
ok "Images updated"

# ── Step 2: Check for chain fork ─────────────────────────────────────────────
NEEDS_RESET=false
NETWORK_GENESIS=""

# Try /chain-info endpoint first
CHAIN_INFO=$(curl -sS --max-time 10 "${GATEWAY_URL}/chain-info" 2>/dev/null || echo "")
if [ -n "$CHAIN_INFO" ] && echo "$CHAIN_INFO" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
  NETWORK_GENESIS=$(echo "$CHAIN_INFO" | python3 -c "import sys,json; g=json.load(sys.stdin).get('genesis',''); print(g.replace('0x',''))" 2>/dev/null)
fi

# Fallback: fetch chain spec and compute genesis from it
if [ -z "$NETWORK_GENESIS" ]; then
  info "Fetching chain spec to determine network genesis..."
  TEMP_SPEC=$(mktemp)
  if curl -sSf --max-time 30 "$CHAIN_SPEC_URL" -o "$TEMP_SPEC" 2>/dev/null; then
    # We can't compute genesis without the node binary, so just download and use it
    NETWORK_GENESIS="from-spec"
  else
    warn "Could not determine network genesis. Skipping fork detection."
  fi
  rm -f "$TEMP_SPEC"
fi

if [ -n "$NETWORK_GENESIS" ] && [ "$NETWORK_GENESIS" != "from-spec" ] && [ "$MODE" = "validator" ]; then
  # Check local node genesis
  LOCAL_GENESIS=""
  if docker compose ps --status running 2>/dev/null | grep -q materios-node; then
    LOCAL_RESPONSE=$(curl -sS --max-time 5 -X POST http://localhost:9944 \
      -H "Content-Type: application/json" \
      -d '{"jsonrpc":"2.0","id":1,"method":"chain_getBlockHash","params":[0]}' 2>/dev/null || echo "")
    if [ -n "$LOCAL_RESPONSE" ]; then
      LOCAL_GENESIS=$(echo "$LOCAL_RESPONSE" | python3 -c "import sys,json; g=json.load(sys.stdin).get('result',''); print(g.replace('0x',''))" 2>/dev/null || echo "")
    fi
  fi

  if [ -n "$LOCAL_GENESIS" ] && [ "$LOCAL_GENESIS" != "$NETWORK_GENESIS" ]; then
    echo ""
    warn "Chain fork detected!"
    warn "  Local genesis:   ${LOCAL_GENESIS:0:16}..."
    warn "  Network genesis: ${NETWORK_GENESIS:0:16}..."
    NEEDS_RESET=true
  elif [ -n "$LOCAL_GENESIS" ]; then
    ok "Genesis matches network (${LOCAL_GENESIS:0:16}...)"
  fi
fi

# Also check attestor mode — compare CHAIN_ID in compose/env
if [ "$MODE" = "attestor" ] && [ -n "$NETWORK_GENESIS" ] && [ "$NETWORK_GENESIS" != "from-spec" ]; then
  CURRENT_CHAIN_ID=$(grep "CHAIN_ID" "$OPERATOR_DIR/docker-compose.yml" 2>/dev/null | head -1 | sed 's/.*CHAIN_ID.*"\(.*\)"/\1/' || echo "")
  if [ -n "$CURRENT_CHAIN_ID" ] && [ "$CURRENT_CHAIN_ID" != "$NETWORK_GENESIS" ]; then
    warn "Chain fork detected! CHAIN_ID mismatch."
    NEEDS_RESET=true
  fi
fi

# ── Step 3: Handle chain fork ────────────────────────────────────────────────
if [ "$NEEDS_RESET" = true ]; then
  info "Applying chain fork reset..."

  # Stop services
  docker compose down 2>/dev/null

  if [ "$MODE" = "validator" ]; then
    # Download new chain spec
    info "Downloading new chain spec..."
    curl -sSf --max-time 30 "$CHAIN_SPEC_URL" -o "$OPERATOR_DIR/chain-spec-raw.json" || fail "Failed to download chain spec"
    ok "Chain spec downloaded"

    # Ensure docker-compose.yml uses the chain spec file
    if ! grep -q "chain-spec-raw.json" "$OPERATOR_DIR/docker-compose.yml" 2>/dev/null; then
      info "Updating docker-compose.yml to use chain spec file..."
      # Replace --chain local with --chain /chain-spec/chain-spec-raw.json
      sed -i 's|"local"|"/chain-spec/chain-spec-raw.json"|' "$OPERATOR_DIR/docker-compose.yml"
      # Add volume mount for chain spec if not present
      if ! grep -q "chain-spec-raw.json:/chain-spec" "$OPERATOR_DIR/docker-compose.yml" 2>/dev/null; then
        # Add volume mount after the existing node-data volume mount
        sed -i '/node-data:\/data\/materios/a\      - ./chain-spec-raw.json:/chain-spec/chain-spec-raw.json:ro' "$OPERATOR_DIR/docker-compose.yml"
      fi
      ok "Compose file updated for chain spec"
    fi

    # Wipe chain data (keep keystore)
    info "Wiping chain data (keystore preserved)..."
    NODE_VOLUME=$(docker volume ls --format '{{.Name}}' | grep "node-data" | head -1)
    if [ -n "$NODE_VOLUME" ]; then
      docker run --rm -v "$NODE_VOLUME:/data" busybox sh -c \
        "rm -rf /data/materios/chains/*/db /data/materios/chains/*/network && echo WIPED"
      ok "Chain data wiped"
    else
      warn "Could not find node-data volume. You may need to wipe manually."
    fi
  fi

  # Update CHAIN_ID in docker-compose.yml
  if [ -n "$NETWORK_GENESIS" ] && [ "$NETWORK_GENESIS" != "from-spec" ]; then
    OLD_CHAIN_ID=$(grep "CHAIN_ID" "$OPERATOR_DIR/docker-compose.yml" 2>/dev/null | head -1 | sed 's/.*"\([a-f0-9]\{64\}\)".*/\1/' || echo "")
    if [ -n "$OLD_CHAIN_ID" ]; then
      sed -i "s/$OLD_CHAIN_ID/$NETWORK_GENESIS/g" "$OPERATOR_DIR/docker-compose.yml"
      ok "CHAIN_ID updated to ${NETWORK_GENESIS:0:16}..."
    fi
  fi

  # Wipe cert daemon state
  DAEMON_VOLUME=$(docker volume ls --format '{{.Name}}' | grep "cert-daemon" | head -1)
  if [ -n "$DAEMON_VOLUME" ]; then
    docker run --rm -v "$DAEMON_VOLUME:/data" busybox sh -c \
      "rm -f /data/daemon-state.json && echo DAEMON_STATE_CLEARED"
    ok "Cert daemon state cleared"
  fi

  echo ""
  ok "Chain fork reset complete"
fi

# ── Step 4: Restart services ─────────────────────────────────────────────────
info "Restarting services..."
docker compose up -d 2>&1 | tail -3
ok "Services restarted"

# ── Step 5: Wait for node to sync (validators only) ──────────────────────────
if [ "$MODE" = "validator" ]; then
  info "Waiting for node to sync..."
  for i in $(seq 1 30); do
    sleep 5
    HEALTH=$(curl -sS --max-time 5 -X POST http://localhost:9944 \
      -H "Content-Type: application/json" \
      -d '{"jsonrpc":"2.0","id":1,"method":"system_health","params":[]}' 2>/dev/null || echo "")
    if [ -n "$HEALTH" ] && echo "$HEALTH" | grep -q "peers"; then
      PEERS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['peers'])" 2>/dev/null || echo "0")
      SYNCING=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['isSyncing'])" 2>/dev/null || echo "True")
      if [ "$SYNCING" = "False" ] && [ "$PEERS" -gt 0 ] 2>/dev/null; then
        ok "Node synced ($PEERS peers)"
        break
      fi
      printf "."
    else
      printf "."
    fi
  done
  echo ""

  # Regenerate session keys if this was a fork reset
  if [ "$NEEDS_RESET" = true ]; then
    info "Regenerating session keys..."
    if [ -f "$OPERATOR_DIR/generate-session-keys.sh" ]; then
      bash "$OPERATOR_DIR/generate-session-keys.sh"
    else
      ROTATE=$(curl -sS --max-time 30 -X POST http://localhost:9944 \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"author_rotateKeys","params":[]}' 2>/dev/null || echo "")
      KEYS=$(echo "$ROTATE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('result',''))" 2>/dev/null || echo "")
      if [ -n "$KEYS" ] && [ ${#KEYS} -eq 130 ]; then
        echo "$KEYS" > "$OPERATOR_DIR/.session-keys"
        chmod 600 "$OPERATOR_DIR/.session-keys"
        ok "Session keys regenerated: ${KEYS:0:18}..."
        warn "Share these session keys with the Materios team to be added to the authority set."
      else
        warn "Could not generate session keys. Run: bash generate-session-keys.sh"
      fi
    fi
  fi
fi

# ── Step 6: Verify ───────────────────────────────────────────────────────────
echo ""
info "Checking status..."
sleep 5

if [ "$MODE" = "validator" ]; then
  BLOCK=$(curl -sS --max-time 5 -X POST http://localhost:9944 \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"chain_getHeader","params":[]}' 2>/dev/null | \
    python3 -c "import sys,json; print(int(json.load(sys.stdin)['result']['number'],16))" 2>/dev/null || echo "?")
  info "Block height: $BLOCK"
fi

DAEMON_STATUS=$(curl -sS --max-time 5 http://localhost:8080/health 2>/dev/null || echo "")
if [ -n "$DAEMON_STATUS" ]; then
  ok "Cert daemon healthy"
else
  info "Cert daemon starting up..."
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}  ═══════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD}  Materios Update Complete${RESET}"
echo -e "${BOLD}  ═══════════════════════════════════════════${RESET}"
echo ""
if [ "$NEEDS_RESET" = true ]; then
  echo -e "  ${YELLOW}Chain fork was detected and handled automatically.${RESET}"
  if [ "$MODE" = "validator" ]; then
    echo -e "  ${YELLOW}New session keys were generated — share them with the team.${RESET}"
  fi
  echo ""
fi
echo -e "  ${BOLD}Mode${RESET}  : ${MODE}"
echo -e "  ${BOLD}Dir${RESET}   : ${OPERATOR_DIR}"
echo ""
echo -e "  ${BOLD}Commands:${RESET}"
echo "    docker compose logs -f    # View logs"
echo "    docker compose restart    # Restart"
echo ""
