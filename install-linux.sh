#!/bin/bash
# ============================================================================
#  Materios Node Installer for Linux
#
#  Download and run:
#    chmod +x install-linux.sh && ./install-linux.sh
#
#  Or one-liner:
#    curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash
# ============================================================================

clear
echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║     Materios Node Installer (Linux)      ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# Check if running as root (not recommended)
if [ "$(id -u)" -eq 0 ]; then
    echo "  WARNING: Running as root is not recommended."
    echo "  The installer will use sudo when needed."
    echo ""
    read -p "  Continue anyway? (y/N): " CONTINUE
    [ "$CONTINUE" != "y" ] && [ "$CONTINUE" != "Y" ] && exit 1
fi

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

# Install Docker if missing
if ! command -v docker &>/dev/null; then
    echo ""
    echo "  Docker is not installed. Installing..."
    echo ""

    if command -v apt-get &>/dev/null; then
        # Debian/Ubuntu
        sudo apt-get update -qq
        sudo apt-get install -y -qq ca-certificates curl gnupg
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        sudo chmod a+r /etc/apt/keyrings/docker.gpg
        CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $CODENAME stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update -qq
        sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo usermod -aG docker "$USER"
        echo ""
        echo "  Docker installed. You may need to log out and back in"
        echo "  for group changes to take effect."
        echo ""
    elif command -v dnf &>/dev/null; then
        # Fedora/RHEL
        sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
        sudo systemctl start docker && sudo systemctl enable docker
        sudo usermod -aG docker "$USER"
    else
        echo "  Unsupported package manager. Please install Docker manually:"
        echo "  https://docs.docker.com/engine/install/"
        exit 1
    fi
fi

# Run the main installer
echo ""
echo "  Running Materios installer..."
echo ""

curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash -s -- --mode "$MODE" --label "$NODE_LABEL"

echo ""
echo "  To check your node:"
echo "    cd ~/materios-operator && docker compose logs -f"
echo ""
