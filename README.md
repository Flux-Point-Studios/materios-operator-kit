# Materios External Validator — Operator Kit

Run your own attestation daemon to participate in the Materios certification committee. No VPN, Tailscale, or cluster access needed — everything connects over public HTTPS and WebSocket endpoints.

## What This Does

The Materios cert-daemon watches for new receipts on the Materios chain, verifies the associated blob data, and submits attestation (certification) transactions. When enough committee members attest to a receipt, it becomes "certified." The daemon also sends signed heartbeats so anyone can independently verify your validator is alive.

## Prerequisites

- **Docker** (with Docker Compose v2)
- **Python 3.8+** (only for key generation — not needed at runtime)
- **1 vCPU, 512 MB RAM, 1 GB disk**
- **Public internet** (outbound HTTPS + WSS)

## Setup

### Step 1: Clone this repo

```bash
git clone https://github.com/Flux-Point-Studios/materios-operator-kit.git
cd materios-operator-kit
```

### Step 2: Generate your committee key

```bash
pip install substrate-interface mnemonic
python generate_key.py
```

This prints three values:

| Value | What to do with it |
|-------|-------------------|
| **Mnemonic** (24 words) | Save securely. This is your private key. Never share it. |
| **SS58 Address** | Send to FPS team. This is your public identity on-chain. |
| **Public Key hex** | Send to FPS team alongside the SS58 address. |

### Step 3: Register with FPS

Send the FPS team:
- Your **SS58 Address**
- Your **Public Key hex**
- Your preferred **label** (e.g. "MyOrg-Validator")

The FPS team will:
1. Add your address to the on-chain attestation committee
2. Fund your account with MATRA (needed to submit transactions)
3. Provision an API key for you on the blob gateway
4. Send you back your **API key**

### Step 4: Configure the daemon

Edit `docker-compose.yml` and fill in the two required fields:

```yaml
SIGNER_URI: "your twenty four word mnemonic phrase goes here ..."
BLOB_GATEWAY_API_KEY: "your_api_key_from_fps_team"
LOCATOR_REGISTRY_API_KEY: "your_api_key_from_fps_team"   # same key
```

### Step 5: Start the daemon

```bash
docker compose up -d
```

Check the logs:
```bash
docker compose logs -f
```

You should see output like:
```
INFO  Connected to Materios Staging via wss://materios.fluxpointstudios.com/rpc
INFO  Polling for new receipts...
INFO  Heartbeat sent (seq=1, best_block=58100)
```

### Step 6: Verify

**Check the public explorer:**

https://materios.fluxpointstudios.com/explorer/#/committee

Your validator should appear with:
- A **green "Online"** badge (heartbeat received within 60s)
- A **checkmark** in the "Verified" column (sr25519 signature verified)

**Check the heartbeat API directly:**

```bash
curl -s https://materios.fluxpointstudios.com/blobs/heartbeats/status | python3 -m json.tool
```

Your SS58 address should appear in the validators list with `"status": "online"`.

**Check your local health endpoint:**

```bash
curl http://localhost:8080/health
curl http://localhost:8080/status
```

## Architecture

```
Your Machine                          FPS Infrastructure
+--------------+                      +----------------------+
|              |---WSS(/rpc)--------->| Materios RPC Node    |
| cert-daemon  |                      | (read chain state,   |
|              |---HTTPS(/blobs)----->| submit attestations) |
|              |  (heartbeats,       |                      |
|              |   blob verification)|  Blob Gateway         |
+--------------+                      |  (blob storage,      |
                                      |   heartbeat store)   |
                                      +----------------------+
```

| Endpoint | URL | Purpose |
|----------|-----|---------|
| RPC | `wss://materios.fluxpointstudios.com/rpc` | Read chain state, submit attestation transactions |
| Blob Gateway | `https://materios.fluxpointstudios.com/blobs` | Fetch blob data for verification, send heartbeats |
| Explorer | `https://materios.fluxpointstudios.com/explorer/` | View chain activity, receipts, committee health |
| Heartbeat Status | `https://materios.fluxpointstudios.com/blobs/heartbeats/status` | Public JSON endpoint — validator liveness |

## Security Model

- Your **mnemonic never leaves your machine**. The daemon signs transactions and heartbeats locally.
- **API keys** are for rate-limiting only — they don't authenticate your identity. Your on-chain identity comes from your sr25519 key.
- **Heartbeat signatures** are publicly verifiable. Anyone can confirm your daemon is alive by checking the signature against your on-chain public key. FPS cannot forge heartbeats for you.
- **Attestation transactions** are on-chain. Anyone can verify committee activity independently.

## Updating

Pull the latest image and restart:

```bash
docker compose pull
docker compose up -d
```

## Stopping

```bash
docker compose down
```

Your data (heartbeat sequence counter, state) is persisted in the `cert-daemon-data` Docker volume. Restarting picks up where you left off.

## Optional: Run Your Own Watchtower

Monitor committee health independently with your own Discord alerts. The watchtower only reads the **public** heartbeat endpoint — no API key needed.

```bash
docker compose run --rm \
  -e BLOB_GATEWAY_URL=https://materios.fluxpointstudios.com/blobs \
  -e DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_WEBHOOK \
  cert-daemon python -m daemon.watchtower
```

Or run it persistently by adding a second service to your `docker-compose.yml`:

```yaml
  watchtower:
    image: ghcr.io/flux-point-studios/materios-operator-kit:latest
    restart: unless-stopped
    command: ["python", "-m", "daemon.watchtower"]
    environment:
      BLOB_GATEWAY_URL: "https://materios.fluxpointstudios.com/blobs"
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/YOUR_WEBHOOK"
      WATCHTOWER_POLL_INTERVAL: "30"
      WATCHTOWER_THRESHOLD: "2"
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `docker compose pull` fails with 401 | GHCR image not accessible | Make sure you can reach `ghcr.io`. If using a firewall, allow outbound HTTPS to `ghcr.io` |
| Heartbeat not appearing on explorer | API key incorrect or not provisioned | Verify with FPS team that your key is active |
| `substrate_connected: false` in status | Can't reach RPC endpoint | Check that `wss://materios.fluxpointstudios.com/rpc` is reachable from your network |
| High finality gap (>10) | Chain-wide issue, not your daemon | Check the explorer dashboard — if all validators show high gap, it's a chain stall |
| `No locator found` in logs | Blob not yet uploaded for a receipt | Normal during brief windows — daemon retries automatically |
| Attestation tx fails with "Priority too low" | Nonce collision (rare) | Daemon auto-recovers on next poll cycle |
| Account balance too low | MATRA depleted from transaction fees | Ask FPS team to top up your account |

## FAQ

**Q: Do I need to run a Materios blockchain node?**
No. The daemon connects to the public RPC endpoint. You only run the cert-daemon container.

**Q: What happens if my daemon goes offline?**
The committee continues with the remaining members (threshold is 2-of-3). Your heartbeat will show "offline" on the explorer. Receipts can still be certified as long as at least 2 committee members are online. Restart your daemon when ready — it catches up automatically.

**Q: How much bandwidth does this use?**
Minimal. The daemon polls for new blocks every 3-12 seconds (small JSON-RPC calls) and sends a heartbeat every 30 seconds. Expect <1 GB/month.

**Q: Can I run this on a Raspberry Pi / ARM device?**
The Docker image is built for `linux/amd64`. ARM support (including Apple Silicon) is not available yet.
