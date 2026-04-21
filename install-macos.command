#!/bin/bash
# ============================================================================
#  Materios Node Installer for macOS
#
#  Double-click this file to run, or right-click → Open
#  If blocked: System Settings → Privacy & Security → Open Anyway
# ============================================================================

clear
echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║     Materios Node Installer (macOS)      ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# Check if Docker Desktop is running
if ! command -v docker &>/dev/null; then
    echo "  Docker is not installed."
    echo ""
    echo "  Please install Docker Desktop first:"
    echo "  https://www.docker.com/products/docker-desktop/"
    echo ""
    echo "  After installing, open Docker Desktop and wait for it to start,"
    echo "  then double-click this file again."
    echo ""
    read -p "  Press Enter to open the Docker download page..."
    open "https://www.docker.com/products/docker-desktop/"
    exit 1
fi

if ! docker info &>/dev/null 2>&1; then
    echo "  Docker Desktop is installed but not running."
    echo ""
    echo "  Opening Docker Desktop..."
    open -a Docker
    echo "  Please wait for Docker to start (whale icon in menu bar),"
    echo "  then double-click this file again."
    echo ""
    read -p "  Press Enter to exit..."
    exit 1
fi

echo "  Docker is running. Starting installation..."
echo ""

# Ask for mode
echo "  How would you like to participate?"
echo ""
echo "    1) Full Validator  — run a blockchain node + attestation daemon"
echo "       (Requires: 2+ CPU, 2GB RAM, 50GB disk, port 30333 open)"
echo ""
echo "    2) Attestor Only   — run just the attestation daemon (lighter)"
echo "       (Requires: 1 CPU, 512MB RAM, 1GB disk, outbound internet)"
echo ""
read -p "  Enter 1 or 2 [1]: " MODE_CHOICE
MODE_CHOICE="${MODE_CHOICE:-1}"

if [ "$MODE_CHOICE" = "2" ]; then
    MODE="attestor"
else
    MODE="validator"
fi

# Ask for name
echo ""
read -p "  Choose a name for your node: " NODE_LABEL
NODE_LABEL="${NODE_LABEL:-$(hostname -s)}"

# Ask for install dir (optional — lets operators run multiple attestors on one host)
echo ""
echo "  Install directory (leave blank for default)."
echo "  Only set this if you're running a SECOND attestor alongside an existing one."
echo "  Default: ~/materios-operator (validator) or ~/materios-attestor (attestor)"
read -p "  Install dir: " INSTALL_DIR

# Run the main installer
echo ""
echo "  Downloading and running installer..."
echo ""

INSTALL_ARGS=(--mode "$MODE" --label "$NODE_LABEL")
if [ -n "$INSTALL_DIR" ]; then
    INSTALL_ARGS+=(--install-dir "$INSTALL_DIR")
fi

curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash -s -- "${INSTALL_ARGS[@]}"

echo ""
echo "  ════════════════════════════════════════"
echo "  Installation complete!"
echo ""
echo "  To check your node:  open Terminal and run:"
if [ -n "$INSTALL_DIR" ]; then
    echo "    cd $INSTALL_DIR && docker compose logs -f"
else
    echo "    cd ~/materios-operator && docker compose logs -f"
fi
echo ""
read -p "  Press Enter to close this window..."
