#!/usr/bin/env bash
# Materios Validator — One-Command Installer
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash -s -- --token <INVITE_TOKEN>
#
# What this does:
#   1. Checks prerequisites (Docker, Compose v2, disk, RAM, ports)
#   2. Pulls the validator node and cert daemon Docker images
#   3. Generates an sr25519 keypair inside a throwaway container
#   4. Redeems the invite token with the Materios gateway
#   5. Writes docker-compose.yml with both materios-node and cert-daemon
#   6. Starts the validator node, waits for chain sync
#   7. Generates session keys (Aura + Grandpa) via author_rotateKeys
#   8. Reports session keys to the gateway
#   9. Starts the cert daemon
#  10. Prints summary with next steps
#
set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────────────
OPERATOR_DIR="$HOME/materios-operator"
# Select image tag based on architecture
case "$(uname -m)" in
  arm64|aarch64) NODE_IMAGE="ghcr.io/flux-point-studios/materios-node:v105-arm64" ;;
  *)             NODE_IMAGE="ghcr.io/flux-point-studios/materios-node:v105" ;;
esac
DAEMON_IMAGE="ghcr.io/flux-point-studios/materios-operator-kit:latest"
GATEWAY_URL="https://materios.fluxpointstudios.com/blobs"
EXPLORER_URL="https://materios.fluxpointstudios.com/explorer/#/committee"
BOOTNODE="/ip4/5.78.94.109/tcp/30333/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp"
MIN_DISK_MB=51200   # 50 GB
MIN_RAM_MB=1800     # ~2 GB

# ── Colors (fall back to plain if no tty) ────────────────────────────────────
if [ -t 1 ]; then
  BOLD="\033[1m"
  GREEN="\033[32m"
  RED="\033[31m"
  YELLOW="\033[33m"
  CYAN="\033[36m"
  RESET="\033[0m"
else
  BOLD="" GREEN="" RED="" YELLOW="" CYAN="" RESET=""
fi

info()  { echo -e "${CYAN}[materios]${RESET} $*"; }
ok()    { echo -e "${GREEN}[OK]${RESET} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${RESET} $*"; }
fail()  { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

# ── Parse arguments ──────────────────────────────────────────────────────────
INVITE_TOKEN=""
LABEL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)  INVITE_TOKEN="$2"; shift 2 ;;
    --label)  LABEL="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: install.sh --token <INVITE_TOKEN> [--label <NODE_LABEL>]"
      echo ""
      echo "  --token   Invite token provided by the Materios team (required)"
      echo "  --label   Friendly name for your node (optional, defaults to hostname)"
      echo ""
      echo "This installs a full Materios validator node and cert daemon."
      echo "Requirements: 2+ vCPU, 2+ GB RAM, 50+ GB SSD, port 30333 open inbound."
      exit 0
      ;;
    *) fail "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[ -z "$INVITE_TOKEN" ] && fail "Missing --token. Get an invite token from the Materios team.\n  Usage: install.sh --token <INVITE_TOKEN>"
[ -z "$LABEL" ] && LABEL="$(hostname -s 2>/dev/null || echo operator)-$(date +%s | tail -c 5)"

# ── Banner ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}  Materios Validator Installer${RESET}"
echo "  ──────────────────────────────"
echo ""

# ── Step 1: Preflight checks ────────────────────────────────────────────────
info "Checking prerequisites..."

# Docker
command -v docker >/dev/null 2>&1 || fail "Docker is not installed. Install it from https://docs.docker.com/get-docker/"

# Docker Compose v2
if docker compose version >/dev/null 2>&1; then
  COMPOSE_VER=$(docker compose version --short 2>/dev/null || echo "unknown")
  ok "Docker Compose v2 ($COMPOSE_VER)"
else
  fail "Docker Compose v2 not found. Install it: https://docs.docker.com/compose/install/"
fi

# Docker daemon running
docker info >/dev/null 2>&1 || fail "Docker daemon is not running. Start it with: sudo systemctl start docker"

# Architecture — node image supports amd64 and arm64
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64) ok "Architecture: $ARCH" ;;
  arm64|aarch64) ok "Architecture: $ARCH (ARM64)" ;;
  *) fail "Unsupported architecture: $ARCH. The Materios node image requires x86_64 or arm64." ;;
esac

# Disk space
AVAIL_MB=$(df -m "$HOME" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
if [ "$AVAIL_MB" -lt "$MIN_DISK_MB" ]; then
  fail "Insufficient disk space: ${AVAIL_MB}MB available, need ${MIN_DISK_MB}MB (50 GB)"
fi
ok "Disk: ${AVAIL_MB}MB available"

# RAM
TOTAL_RAM_MB=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo 0)
if [ "$TOTAL_RAM_MB" -gt 0 ] && [ "$TOTAL_RAM_MB" -lt "$MIN_RAM_MB" ]; then
  fail "Insufficient RAM: ${TOTAL_RAM_MB}MB (need 2048MB+). A full validator node requires at least 2 GB."
else
  ok "RAM: ${TOTAL_RAM_MB}MB"
fi

# curl or wget
command -v curl >/dev/null 2>&1 || fail "curl is required but not installed."

# Internet — test gateway reachability
if curl -sSf --max-time 10 "${GATEWAY_URL}/health" >/dev/null 2>&1; then
  ok "Gateway reachable"
else
  fail "Cannot reach Materios gateway at ${GATEWAY_URL}. Check your internet connection."
fi

# Port 30333 — warn if something is already bound
if ss -tlnp 2>/dev/null | grep -q ':30333 '; then
  warn "Port 30333 is already in use. The node needs this port for P2P networking."
fi

# ── Step 2: Create operator directory ────────────────────────────────────────
info "Setting up ${OPERATOR_DIR}..."
mkdir -p "$OPERATOR_DIR"
cd "$OPERATOR_DIR"

# Check for existing installation
if [ -f "$OPERATOR_DIR/docker-compose.yml" ]; then
  warn "Existing installation found at $OPERATOR_DIR"
  if docker compose ps --status running 2>/dev/null | grep -q materios-node; then
    fail "Node is already running. Stop it first with: cd $OPERATOR_DIR && docker compose down"
  fi
  warn "Overwriting configuration (existing data volumes preserved)"
fi

# ── Step 3: Pull Docker images ──────────────────────────────────────────────
info "Pulling validator node image (this may take a few minutes)..."
docker pull "$NODE_IMAGE" || fail "Failed to pull node image. Check Docker / internet."
ok "Node image pulled"

info "Pulling cert daemon image..."
docker pull "$DAEMON_IMAGE" || fail "Failed to pull daemon image."
ok "Daemon image pulled"

# ── Step 4: Generate keypair inside throwaway container ──────────────────────
info "Generating sr25519 keypair..."

KEYGEN_OUTPUT=$(docker run --rm python:3.12-slim sh -c "
pip install -q substrate-interface mnemonic 2>/dev/null
python3 -c \"
from substrateinterface import Keypair
from mnemonic import Mnemonic
import json

m = Mnemonic('english')
mnemonic = m.generate(strength=256)
keypair = Keypair.create_from_mnemonic(mnemonic)

print(json.dumps({
    'mnemonic': mnemonic,
    'ss58': keypair.ss58_address,
    'public_key': '0x' + keypair.public_key.hex()
}))
\"
") || fail "Key generation failed. Check Docker."

# Parse with python3 first, fall back to jq
parse_json() {
  local field="$1"
  echo "$KEYGEN_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['${field}'])" 2>/dev/null || \
  echo "$KEYGEN_OUTPUT" | jq -r ".${field}" 2>/dev/null || echo ""
}

MNEMONIC=$(parse_json mnemonic)
SS58=$(parse_json ss58)
PUBKEY=$(parse_json public_key)

[ -z "$MNEMONIC" ] || [ -z "$SS58" ] && fail "Key generation produced empty output"
ok "Keypair generated: $SS58"

# ── Step 5: Save mnemonic securely ──────────────────────────────────────────
MNEMONIC_FILE="$OPERATOR_DIR/.secret-mnemonic"
echo "$MNEMONIC" > "$MNEMONIC_FILE"
chmod 600 "$MNEMONIC_FILE"
ok "Mnemonic saved to $MNEMONIC_FILE (chmod 600)"

# ── Step 6: Redeem invite token ─────────────────────────────────────────────
info "Registering with Materios gateway..."

REGISTER_RESPONSE=$(curl -sS --max-time 30 -X POST "${GATEWAY_URL}/operators/register" \
  -H "Content-Type: application/json" \
  -d "{
    \"invite_token\": \"${INVITE_TOKEN}\",
    \"ss58_address\": \"${SS58}\",
    \"public_key\": \"${PUBKEY}\",
    \"label\": \"${LABEL}\"
  }" 2>&1) || fail "Registration request failed. Check your internet connection."

# Parse response
REG_STATUS=$(echo "$REGISTER_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null || \
             echo "$REGISTER_RESPONSE" | jq -r '.status // empty' 2>/dev/null || echo "")
REG_ERROR=$(echo "$REGISTER_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',''))" 2>/dev/null || \
            echo "$REGISTER_RESPONSE" | jq -r '.error // empty' 2>/dev/null || echo "")
API_KEY=$(echo "$REGISTER_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('api_key',''))" 2>/dev/null || \
          echo "$REGISTER_RESPONSE" | jq -r '.api_key // empty' 2>/dev/null || echo "")

if [ "$REG_STATUS" != "registered" ]; then
  if [ -n "$REG_ERROR" ]; then
    fail "Registration failed: $REG_ERROR"
  else
    fail "Registration failed. Response: $REGISTER_RESPONSE"
  fi
fi

ok "Registered as $LABEL"

# Save API key for later use
echo "$API_KEY" > "$OPERATOR_DIR/.api-key"
chmod 600 "$OPERATOR_DIR/.api-key"

# ── Step 7: Write docker-compose.yml ────────────────────────────────────────
info "Writing configuration..."

cat > "$OPERATOR_DIR/docker-compose.yml" <<COMPOSE
## Materios Validator + Cert Daemon — Auto-generated by install.sh
## Operator: ${LABEL}
## SS58: ${SS58}
## Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)

services:
  materios-node:
    image: ${NODE_IMAGE}
    restart: unless-stopped
    command:
      - "--chain"
      - "local"
      - "--base-path"
      - "/data/materios"
      - "--rpc-port"
      - "9944"
      - "--unsafe-rpc-external"
      - "--rpc-cors"
      - "all"
      - "--rpc-methods"
      - "unsafe"
      - "--port"
      - "30333"
      - "--name"
      - "${LABEL}"
      - "--validator"
      - "--bootnodes"
      - "${BOOTNODE}"
    volumes:
      - node-data:/data/materios
    ports:
      - "30333:30333"        # P2P — must be reachable from internet
      - "127.0.0.1:9944:9944"  # RPC — localhost only for security
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9944/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s

  cert-daemon:
    image: ${DAEMON_IMAGE}
    restart: unless-stopped
    depends_on:
      materios-node:
        condition: service_healthy
    environment:
      SIGNER_URI: "${MNEMONIC}"
      BLOB_GATEWAY_API_KEY: "${API_KEY}"
      LOCATOR_REGISTRY_API_KEY: "${API_KEY}"
      MATERIOS_RPC_URL: "ws://materios-node:9944"
      BLOB_GATEWAY_URL: "${GATEWAY_URL}"
      LOCATOR_REGISTRY_URL: "${GATEWAY_URL}"
      HEARTBEAT_URL: "${GATEWAY_URL}"
      HEARTBEAT_INTERVAL: "30"
      CHECKPOINT_ENABLED: "false"
      CHAIN_ID: "5663079a485b93fdc9e386b862b4cf8d25499427df6b8c5f018535acfd2e5020"
      POLL_INTERVAL: "12"
      POLL_INTERVAL_FAST: "3"
      POLL_INTERVAL_IDLE: "12"
      DATA_DIR: "/data"
      BLOB_LOCAL_DIR: "/data/materios-blobs"
      CERT_STORE_DIR: "/data/certs"
      STATE_FILE: "/data/daemon-state.json"
      HEALTH_PORT: "8080"
      MAX_BLOB_FETCH_RETRIES: "3"
      BLOB_FETCH_TIMEOUT: "30"
      FINALITY_CONFIRMATIONS: "4"
      MAX_LEAF_WAIT_SECONDS: "90"
    volumes:
      - cert-daemon-data:/data
    ports:
      - "127.0.0.1:8080:8080"

volumes:
  node-data:
  cert-daemon-data:
COMPOSE

chmod 600 "$OPERATOR_DIR/docker-compose.yml"
ok "docker-compose.yml written"

# ── Step 8: Start the validator node ─────────────────────────────────────────
info "Starting validator node..."
docker compose up -d materios-node || fail "Failed to start node"
ok "Validator node started"

# ── Step 9: Wait for node to sync ────────────────────────────────────────────
info "Waiting for node to connect to the network and sync..."
info "This may take 5-15 minutes depending on chain height. Please be patient."
echo ""

NODE_READY=false
SYNC_ATTEMPTS=0
MAX_SYNC_WAIT=180  # 15 minutes in 5-second intervals
HIGHEST=0
CURRENT=0

while [ "$SYNC_ATTEMPTS" -lt "$MAX_SYNC_WAIT" ]; do
  SYNC_ATTEMPTS=$((SYNC_ATTEMPTS + 1))

  # Try to get sync state via RPC
  SYNC_RESPONSE=$(curl -sS --max-time 5 -X POST http://localhost:9944 \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"system_syncState","params":[]}' 2>/dev/null || echo "")

  if [ -n "$SYNC_RESPONSE" ]; then
    CURRENT=$(echo "$SYNC_RESPONSE" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('result',{}).get('currentBlock',0))" 2>/dev/null || echo "0")
    HIGHEST=$(echo "$SYNC_RESPONSE" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('result',{}).get('highestBlock',0))" 2>/dev/null || echo "0")

    if [ "$HIGHEST" -gt 0 ] 2>/dev/null; then
      GAP=$((HIGHEST - CURRENT))
      if [ "$GAP" -le 5 ]; then
        echo ""
        ok "Node synced! Block $CURRENT / $HIGHEST"
        NODE_READY=true
        break
      else
        # Show progress every 30 seconds
        if [ $((SYNC_ATTEMPTS % 6)) -eq 0 ]; then
          echo -e "\r  Syncing: block $CURRENT / $HIGHEST (${GAP} blocks remaining)    "
        fi
      fi
    fi
  fi

  sleep 5
  if [ $((SYNC_ATTEMPTS % 6)) -eq 0 ] && [ "$HIGHEST" = "0" ] 2>/dev/null; then
    printf "."
  fi
done

echo ""
if [ "$NODE_READY" != true ]; then
  warn "Node is still syncing. Session keys will be generated once sync completes."
  warn "Re-run this step later: cd $OPERATOR_DIR && bash generate-session-keys.sh"
fi

# ── Step 10: Generate session keys via author_rotateKeys ─────────────────────
SESSION_KEYS=""
if [ "$NODE_READY" = true ]; then
  info "Generating session keys (Aura + Grandpa)..."

  ROTATE_RESPONSE=$(curl -sS --max-time 30 -X POST http://localhost:9944 \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"author_rotateKeys","params":[]}' 2>/dev/null || echo "")

  SESSION_KEYS=$(echo "$ROTATE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('result',''))" 2>/dev/null || \
                 echo "$ROTATE_RESPONSE" | jq -r '.result // empty' 2>/dev/null || echo "")

  if [ -n "$SESSION_KEYS" ] && [ ${#SESSION_KEYS} -eq 130 ]; then
    ok "Session keys generated: ${SESSION_KEYS:0:18}..."
    echo "$SESSION_KEYS" > "$OPERATOR_DIR/.session-keys"
    chmod 600 "$OPERATOR_DIR/.session-keys"

    # Get peer ID
    PEER_RESPONSE=$(curl -sS --max-time 10 -X POST http://localhost:9944 \
      -H "Content-Type: application/json" \
      -d '{"jsonrpc":"2.0","id":1,"method":"system_localPeerId","params":[]}' 2>/dev/null || echo "")
    PEER_ID=$(echo "$PEER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('result',''))" 2>/dev/null || echo "")

    # Report session keys to gateway
    info "Reporting session keys to Materios gateway..."
    REPORT_RESPONSE=$(curl -sS --max-time 30 -X PATCH "${GATEWAY_URL}/operators/${SS58}/session-keys" \
      -H "Content-Type: application/json" \
      -d "{
        \"session_keys\": \"${SESSION_KEYS}\",
        \"peer_id\": \"${PEER_ID}\",
        \"api_key\": \"${API_KEY}\"
      }" 2>/dev/null || echo "")

    REPORT_STATUS=$(echo "$REPORT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
    if [ "$REPORT_STATUS" = "updated" ]; then
      ok "Session keys reported to gateway"
    else
      warn "Could not report session keys to gateway. The Materios team can retrieve them manually."
    fi
  else
    warn "Session key generation returned unexpected result. You may need to generate keys manually."
  fi
fi

# ── Step 11: Start the cert daemon ──────────────────────────────────────────
info "Starting cert daemon..."
docker compose up -d cert-daemon || fail "Failed to start cert daemon"
ok "Cert daemon started"

# ── Step 12: Wait for daemon health ─────────────────────────────────────────
info "Waiting for cert daemon health check (up to 90 seconds)..."
HEALTH_OK=false
for i in $(seq 1 18); do
  sleep 5
  HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" "http://localhost:8080/health" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" = "200" ]; then
    READY_CODE=$(curl -sS -o /dev/null -w "%{http_code}" "http://localhost:8080/ready" 2>/dev/null || echo "000")
    if [ "$READY_CODE" = "200" ]; then
      HEALTH_OK=true
      break
    fi
  fi
  printf "."
done
echo ""

if [ "$HEALTH_OK" = true ]; then
  ok "Cert daemon is healthy and connected"
else
  warn "Cert daemon started but not yet fully connected. This is normal."
  warn "Check status: curl http://localhost:8080/status"
fi

# ── Write helper script for post-sync key generation ─────────────────────────
cat > "$OPERATOR_DIR/generate-session-keys.sh" <<'KEYSCRIPT'
#!/usr/bin/env bash
# Generate session keys after node sync (run if installer timed out during sync)
set -euo pipefail
cd "$(dirname "$0")"

API_KEY=$(cat .api-key 2>/dev/null || echo "")
SS58=$(grep "^## SS58:" docker-compose.yml | awk '{print $3}')
GATEWAY_URL="https://materios.fluxpointstudios.com/blobs"

echo "Checking node sync state..."
SYNC=$(curl -sS -X POST http://localhost:9944 \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"system_syncState","params":[]}' 2>/dev/null)
CURRENT=$(echo "$SYNC" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('result',{}).get('currentBlock',0))" 2>/dev/null || echo "0")
HIGHEST=$(echo "$SYNC" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('result',{}).get('highestBlock',0))" 2>/dev/null || echo "0")
echo "Block: $CURRENT / $HIGHEST"

if [ "$HIGHEST" -gt 0 ] && [ $((HIGHEST - CURRENT)) -gt 5 ]; then
  echo "Node is still syncing. Please wait until sync completes."
  exit 1
fi

echo "Generating session keys..."
RESULT=$(curl -sS -X POST http://localhost:9944 \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"author_rotateKeys","params":[]}')
KEYS=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])" 2>/dev/null)
echo "Session keys: $KEYS"
echo "$KEYS" > .session-keys
chmod 600 .session-keys

PEER=$(curl -sS -X POST http://localhost:9944 \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"system_localPeerId","params":[]}')
PEER_ID=$(echo "$PEER" | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])" 2>/dev/null)
echo "Peer ID: $PEER_ID"

if [ -n "$API_KEY" ] && [ -n "$SS58" ]; then
  echo "Reporting to gateway..."
  curl -sS -X PATCH "${GATEWAY_URL}/operators/${SS58}/session-keys" \
    -H "Content-Type: application/json" \
    -d "{\"session_keys\":\"${KEYS}\",\"peer_id\":\"${PEER_ID}\",\"api_key\":\"${API_KEY}\"}"
  echo ""
  echo "Done! Session keys reported to Materios gateway."
else
  echo "Could not auto-report. Share these session keys with the Materios team:"
  echo "  SS58: $SS58"
  echo "  Session Keys: $KEYS"
  echo "  Peer ID: $PEER_ID"
fi
KEYSCRIPT
chmod +x "$OPERATOR_DIR/generate-session-keys.sh"

# ── Step 13: Print summary ──────────────────────────────────────────────────
echo ""
echo -e "${BOLD}  ═══════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD}  Materios Validator Online${RESET}"
echo -e "${BOLD}  ═══════════════════════════════════════════${RESET}"
echo ""
echo -e "  ${BOLD}SS58 Address${RESET}   : ${SS58}"
echo -e "  ${BOLD}Label${RESET}          : ${LABEL}"
if [ -n "$SESSION_KEYS" ] && [ ${#SESSION_KEYS} -eq 130 ]; then
echo -e "  ${BOLD}Session Keys${RESET}   : ${SESSION_KEYS:0:18}...${SESSION_KEYS:126:4}"
fi
if [ -n "${PEER_ID:-}" ]; then
echo -e "  ${BOLD}Peer ID${RESET}        : ${PEER_ID}"
fi
echo -e "  ${BOLD}P2P Port${RESET}       : 30333 (ensure this is open inbound)"
echo -e "  ${BOLD}Node RPC${RESET}       : http://localhost:9944"
echo -e "  ${BOLD}Daemon Health${RESET}  : http://localhost:8080/status"
echo -e "  ${BOLD}Explorer${RESET}       : ${EXPLORER_URL}"
echo -e "  ${BOLD}Mnemonic${RESET}       : ${MNEMONIC_FILE}"
echo ""
echo -e "  ${YELLOW}${BOLD}IMPORTANT:${RESET}"
echo -e "  ${YELLOW}- Back up your mnemonic file immediately${RESET}"
echo -e "  ${YELLOW}- Never share the mnemonic with anyone${RESET}"
echo -e "  ${YELLOW}- Ensure port 30333 is open in your firewall${RESET}"
echo -e "  ${YELLOW}- The Materios team will add you to the authority set shortly${RESET}"
echo ""
if [ -z "$SESSION_KEYS" ] || [ ${#SESSION_KEYS} -ne 130 ]; then
echo -e "  ${YELLOW}${BOLD}NOTE:${RESET} Node was still syncing. Once synced, run:"
echo -e "    cd $OPERATOR_DIR && bash generate-session-keys.sh"
echo ""
fi
echo -e "  ${BOLD}Commands:${RESET}"
echo "    cd $OPERATOR_DIR"
echo "    docker compose logs -f materios-node   # Node logs"
echo "    docker compose logs -f cert-daemon     # Daemon logs"
echo "    docker compose restart                 # Restart all"
echo "    docker compose down                    # Stop all"
echo "    docker compose pull && docker compose up -d  # Update"
echo ""
