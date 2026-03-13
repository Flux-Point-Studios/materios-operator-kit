#!/usr/bin/env bash
# Materios Operator — One-Command Installer
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash -s -- --token <INVITE_TOKEN>
#
# What this does:
#   1. Checks prerequisites (Docker, Compose v2, disk, internet)
#   2. Pulls the operator Docker image
#   3. Generates an sr25519 keypair inside a throwaway container
#   4. Redeems the invite token with the Materios gateway
#   5. Writes docker-compose.yml with all values filled in
#   6. Starts the cert daemon
#   7. Waits for first successful heartbeat
#   8. Prints summary
#
set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────────────
OPERATOR_DIR="$HOME/materios-operator"
IMAGE="ghcr.io/flux-point-studios/materios-operator-kit:latest"
GATEWAY_URL="https://materios.fluxpointstudios.com/blobs"
EXPLORER_URL="https://materios.fluxpointstudios.com/explorer/#/committee"
MIN_DISK_MB=2048
MIN_RAM_MB=384

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
      exit 0
      ;;
    *) fail "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[ -z "$INVITE_TOKEN" ] && fail "Missing --token. Get an invite token from the Materios team.\n  Usage: install.sh --token <INVITE_TOKEN>"
[ -z "$LABEL" ] && LABEL="$(hostname -s 2>/dev/null || echo operator)-$(date +%s | tail -c 5)"

# ── Banner ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}  Materios Operator Installer${RESET}"
echo "  ─────────────────────────────"
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

# Architecture
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64) ok "Architecture: $ARCH" ;;
  aarch64|arm64) ok "Architecture: $ARCH" ;;
  *) warn "Untested architecture: $ARCH — the image may not work" ;;
esac

# Disk space
AVAIL_MB=$(df -m "$HOME" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
if [ "$AVAIL_MB" -lt "$MIN_DISK_MB" ]; then
  fail "Insufficient disk space: ${AVAIL_MB}MB available, need ${MIN_DISK_MB}MB"
fi
ok "Disk: ${AVAIL_MB}MB available"

# RAM
TOTAL_RAM_MB=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo 0)
if [ "$TOTAL_RAM_MB" -gt 0 ] && [ "$TOTAL_RAM_MB" -lt "$MIN_RAM_MB" ]; then
  warn "Low RAM: ${TOTAL_RAM_MB}MB (recommended: ${MIN_RAM_MB}MB+)"
else
  ok "RAM: ${TOTAL_RAM_MB}MB"
fi

# Internet — test gateway reachability
if curl -sSf --max-time 10 "${GATEWAY_URL}/health" >/dev/null 2>&1; then
  ok "Gateway reachable"
else
  fail "Cannot reach Materios gateway at ${GATEWAY_URL}. Check your internet connection."
fi

# ── Step 2: Create operator directory ────────────────────────────────────────
info "Setting up ${OPERATOR_DIR}..."
mkdir -p "$OPERATOR_DIR"
cd "$OPERATOR_DIR"

# Check for existing installation
if [ -f "$OPERATOR_DIR/docker-compose.yml" ]; then
  warn "Existing installation found at $OPERATOR_DIR"
  if docker compose ps --status running 2>/dev/null | grep -q cert-daemon; then
    fail "Daemon is already running. Stop it first with: cd $OPERATOR_DIR && docker compose down"
  fi
  warn "Overwriting configuration (existing data volume preserved)"
fi

# ── Step 3: Pull Docker image ───────────────────────────────────────────────
info "Pulling operator image (this may take a minute)..."
docker pull "$IMAGE" || fail "Failed to pull image. Check Docker login / internet."
ok "Image pulled"

# ── Step 4: Generate keypair inside throwaway container ──────────────────────
info "Generating sr25519 keypair..."

KEYGEN_OUTPUT=$(docker run --rm "$IMAGE" python -c "
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
") || fail "Key generation failed. Check Docker."

MNEMONIC=$(echo "$KEYGEN_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['mnemonic'])" 2>/dev/null || \
           echo "$KEYGEN_OUTPUT" | jq -r '.mnemonic' 2>/dev/null) || fail "Failed to parse key output"
SS58=$(echo "$KEYGEN_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['ss58'])" 2>/dev/null || \
       echo "$KEYGEN_OUTPUT" | jq -r '.ss58' 2>/dev/null)
PUBKEY=$(echo "$KEYGEN_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['public_key'])" 2>/dev/null || \
         echo "$KEYGEN_OUTPUT" | jq -r '.public_key' 2>/dev/null)

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

# ── Step 7: Write docker-compose.yml ────────────────────────────────────────
info "Writing configuration..."

cat > "$OPERATOR_DIR/docker-compose.yml" <<COMPOSE
## Materios Cert Daemon — Auto-generated by install.sh
## Operator: ${LABEL}
## SS58: ${SS58}
## Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)

services:
  cert-daemon:
    image: ${IMAGE}
    restart: unless-stopped
    environment:
      SIGNER_URI: "${MNEMONIC}"
      BLOB_GATEWAY_API_KEY: "${API_KEY}"
      LOCATOR_REGISTRY_API_KEY: "${API_KEY}"
      MATERIOS_RPC_URL: "wss://materios.fluxpointstudios.com/rpc"
      BLOB_GATEWAY_URL: "${GATEWAY_URL}"
      LOCATOR_REGISTRY_URL: "${GATEWAY_URL}"
      HEARTBEAT_URL: "${GATEWAY_URL}"
      HEARTBEAT_INTERVAL: "30"
      CHECKPOINT_ENABLED: "false"
      CHAIN_ID: "8f6e531be80341a12a0ae1b04484770fcaa797bb49dcc1cc9e79788f770a41b3"
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
      - "8080:8080"

volumes:
  cert-daemon-data:
COMPOSE

chmod 600 "$OPERATOR_DIR/docker-compose.yml"
ok "docker-compose.yml written"

# ── Step 8: Start the daemon ────────────────────────────────────────────────
info "Starting cert daemon..."
docker compose up -d || fail "Failed to start daemon"
ok "Daemon started"

# ── Step 9: Wait for first healthy heartbeat ────────────────────────────────
info "Waiting for first heartbeat (up to 90 seconds)..."
HEALTH_OK=false
for i in $(seq 1 18); do
  sleep 5
  HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" "http://localhost:8080/health" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" = "200" ]; then
    # Check /ready too (confirms substrate connected + first poll)
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
  ok "Daemon is healthy and connected"
else
  warn "Daemon started but not yet fully connected. This is normal — it may take a few minutes."
  warn "Check status: curl http://localhost:8080/status"
fi

# ── Step 10: Print summary ──────────────────────────────────────────────────
echo ""
echo -e "${BOLD}  ────────────────────────────────────────${RESET}"
echo -e "${GREEN}${BOLD}  Materios Operator Online${RESET}"
echo -e "${BOLD}  ────────────────────────────────────────${RESET}"
echo ""
echo -e "  ${BOLD}SS58 Address${RESET}  : ${SS58}"
echo -e "  ${BOLD}Label${RESET}         : ${LABEL}"
echo -e "  ${BOLD}Health${RESET}        : http://localhost:8080/status"
echo -e "  ${BOLD}Explorer${RESET}      : ${EXPLORER_URL}"
echo -e "  ${BOLD}Mnemonic${RESET}      : ${MNEMONIC_FILE}"
echo ""
echo -e "  ${YELLOW}${BOLD}IMPORTANT:${RESET}"
echo -e "  ${YELLOW}- Back up your mnemonic file immediately${RESET}"
echo -e "  ${YELLOW}- Never share the mnemonic with anyone${RESET}"
echo -e "  ${YELLOW}- The Materios team will activate your committee seat shortly${RESET}"
echo ""
echo -e "  ${BOLD}Commands:${RESET}"
echo "    cd $OPERATOR_DIR"
echo "    docker compose logs -f       # Watch logs"
echo "    docker compose restart       # Restart daemon"
echo "    docker compose down          # Stop daemon"
echo "    docker compose pull && docker compose up -d  # Update"
echo ""
