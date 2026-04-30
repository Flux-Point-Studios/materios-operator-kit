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
| **SS58 Address** | Your public identity on-chain. Keep it handy. |
| **Public Key hex** | Your public key. Keep it handy. |

### Step 3: Configure the daemon

> **No registration required.** The daemon automatically joins the attestation committee on startup by submitting a `join_committee` extrinsic. If your account doesn't have enough MATRA to pay the transaction fee, the daemon auto-requests a faucet drip first.
>
> **Auto-bond is also handled for you.** The `OrinqReceipts` pallet enforces a `BondRequirement` (currently **1,000 MATRA** on preprod — subject to governance). Before the first `join_committee` call, the daemon reserves the required amount by submitting `bond(delta)` on your behalf. Already-bonded operators are detected and skipped, so restarts never double-bond. See [Auto-bond behaviour](#auto-bond-behaviour) below for the full contract and the single warning case an operator might see.
>
> **Optional:** Send your SS58 address and public key hex to the FPS team if you want higher gateway rate limits or a provisioned API key. This is not required to participate.

Edit `docker-compose.yml` and fill in the required fields:

```yaml
SIGNER_URI: "your twenty four word mnemonic phrase goes here ..."
BLOB_GATEWAY_API_KEY: ""        # optional — higher rate limits if set, not required
LOCATOR_REGISTRY_API_KEY: ""    # optional — reads are public, not required
```

> **Note**: API keys are optional. Your sr25519 signature authenticates heartbeats and blob uploads. API keys provide higher rate limits but are not required for any operation.

> **Heartbeat is required for explorer visibility.** The daemon only starts the heartbeat sender thread when `HEARTBEAT_URL` is set (see `daemon/main.py`). The shipped `docker-compose.yml` already configures `HEARTBEAT_URL: "https://materios.fluxpointstudios.com/preprod-blobs"` and `HEARTBEAT_INTERVAL: "30"` — leave them as-is. **Without `HEARTBEAT_URL` the daemon attests but never reports liveness — explorer will show "No heartbeat" forever.** If you copy the compose snippet into a custom file or onboarding paste, make sure both vars come along; the preprod path is `/preprod-blobs`, not `/blobs`.

### Step 4: Start the daemon

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

### Step 5: Verify

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
- **API keys** are optional for all operations. Your on-chain identity comes from your sr25519 key. Heartbeats and blob uploads are authenticated by sr25519 signature. API keys provide higher rate limits when provisioned.
- **Heartbeat signatures** are publicly verifiable. Anyone can confirm your daemon is alive by checking the signature against your on-chain public key. FPS cannot forge heartbeats for you.
- **Attestation transactions** are on-chain. Anyone can verify committee activity independently.

## Auto-bond behaviour

Committee members are required to reserve a bond under the `OrinqReceipts`
pallet before they can be admitted. The chain stores this floor in the
`BondRequirement` storage item (currently **1,000 MATRA = 1,000,000,000 base
units** at 6 decimals on preprod; mainnet value will be set by governance).
Without a sufficient bond, `join_committee` fails with `InsufficientBond` and
the daemon would loop retry forever, burning MOTRA on each attempt.

**The daemon handles this automatically.** On every startup (and once more as
a defensive fallback if a later `join_committee` still returns
`InsufficientBond`, e.g. because governance raised the floor), the daemon:

1. Queries `OrinqReceipts.BondRequirement` → `required`.
2. Queries `OrinqReceipts.AttestorBonds(<your-address>)` → `current`.
3. If `current >= required`, logs `already bonded {current} / {required}
   MATRA base units, skipping auto-bond` and continues to `join_committee`.
4. Otherwise, queries `System.Account(<your-address>).data.free` and submits
   `OrinqReceipts.bond(required - current)`, waiting for inclusion and
   logging the tx hash.

This is idempotent: restarting the daemon after a successful bond is a no-op.

### What you'll see if your MATRA is short

If your free balance is below the gap (`required - current`), the daemon
logs a WARNING like:

```
WARNING Insufficient free MATRA to auto-bond: need 1000000000 more base
units (have 250000000). Request more MATRA from the faucet at
https://materios.fluxpointstudios.com/blobs/faucet/drip and restart the
daemon, or wait for an automatic faucet drip. Continuing anyway so the
operator can see the join_committee error in the logs.
```

It **does not crash** — the daemon still attempts `join_committee` so the
upstream failure is visible, and a Discord warning (if configured) is posted.
You should either:

- Hit the faucet at `https://materios.fluxpointstudios.com/blobs/faucet/drip`
  with `{"address": "<your-ss58>"}` and restart the daemon, or
- Ask the FPS team in Discord; the standard operator drip is 1,000 MATRA.

Once your balance is topped up, the next startup (or next retry of
`_ensure_committee_membership` on the 60s loop) will post the bond cleanly.

### Fees

The `bond` extrinsic is signed by your attestor keypair and fees are paid in
MOTRA (not MATRA — the two-token split is documented in `docs/`). No extra
configuration is required: the same signer used for every other extrinsic
covers the bond too.

## Running multiple attestors on one host

You can run more than one independent attestor on the same machine using the
`--install-dir` flag on `install.sh`. Each attestor gets its own install
directory, mnemonic, Compose project, and cert-daemon container.

```bash
# First attestor (uses default ~/materios-attestor)
bash install.sh --mode attestor --label first

# Second attestor in a different directory
bash install.sh --mode attestor --label second \
  --install-dir ~/materios-attestor-2
```

Each `--install-dir` must be distinct from every other install on the host,
and each install must use a different `--label`. See
[docs/RUNNING_MULTIPLE_ATTESTORS.md](docs/RUNNING_MULTIPLE_ATTESTORS.md) for
a full walkthrough, edge cases (health-port collision, faucet drip behaviour,
macOS `realpath` caveat), and the manual smoke-test procedure.

## Updating

Pull the latest image and restart:

```bash
docker compose pull
docker compose up -d
```

Or use the one-liner. For non-default installs, pass the same `--install-dir`
you used at install time:

```bash
curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/update.sh \
  | bash -s -- --install-dir ~/materios-attestor-2
```

## Stopping

```bash
docker compose down
```

Your data (heartbeat sequence counter, state) is persisted in the `cert-daemon-data` Docker volume. Restarting picks up where you left off.

## Optional: Daemon Health Watchdog

Get alerted when your daemon goes down, loses RPC connection, or falls behind. The watchdog checks your daemon's local health endpoint and sends alerts via Discord, email, or stdout.

### Quick start (Discord)

1. Create a Discord webhook: Server Settings → Integrations → Webhooks → New Webhook
2. Run the watchdog alongside your daemon:

```bash
ALERT_METHOD=discord \
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN" \
OPERATOR_LABEL="my-attestor" \
./watchdog.sh &
```

### What it checks (every 60s)

| Check | Alert level | Condition |
|-------|------------|-----------|
| Container running | CRIT | `docker compose ps` shows no running containers |
| Health endpoint | CRIT | `http://localhost:8080/status` unreachable |
| RPC connection | WARN | `connected: false` in status response |
| Finality gap | WARN | Gap > 10 blocks (configurable) |
| Poll freshness | WARN | Last poll > 120s ago (daemon stuck) |
| Recovery | OK | Issues resolved — sends green "recovered" alert |

### Run as a Docker service

Add to your `docker-compose.yml`:

```yaml
  watchdog:
    image: alpine:3.19
    restart: unless-stopped
    depends_on:
      - cert-daemon
    entrypoint: ["/bin/sh", "-c", "apk add --no-cache curl python3 bash && /watchdog/watchdog.sh"]
    environment:
      ALERT_METHOD: "discord"
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN"
      OPERATOR_LABEL: "my-attestor"
      HEALTH_URL: "http://cert-daemon:8080/status"
      CHECK_INTERVAL: "60"
    volumes:
      - ./watchdog.sh:/watchdog/watchdog.sh:ro
```

### Email alerts

```bash
ALERT_METHOD=email \
ALERT_EMAIL="you@example.com" \
OPERATOR_LABEL="my-attestor" \
./watchdog.sh &
```

Requires `sendmail` or `msmtp` on the host.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ALERT_METHOD` | `stdout` | `discord`, `email`, or `stdout` |
| `DISCORD_WEBHOOK_URL` | — | Discord webhook URL |
| `ALERT_EMAIL` | — | Email recipient |
| `HEALTH_URL` | `http://localhost:8080/status` | Daemon status endpoint |
| `CHECK_INTERVAL` | `60` | Seconds between checks |
| `MAX_FINALITY_GAP` | `10` | Alert if finality gap exceeds this |
| `MAX_POLL_AGE_SECONDS` | `120` | Alert if daemon hasn't polled in this many seconds |
| `ALERT_COOLDOWN` | `300` | Don't repeat same alert within this window |
| `OPERATOR_LABEL` | `my-attestor` | Label shown in alerts |

## Optional: Committee Watchtower

Monitor the overall committee health (all members, not just yours). Reads the **public** heartbeat endpoint — no API key needed.

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
| Heartbeat not appearing on explorer | Daemon hasn't joined committee yet | Check logs for `join_committee` success; if faucet drip failed, retry or ask FPS team |
| `join_committee` fails with `InsufficientBond` | Free MATRA below `BondRequirement` gap | Daemon auto-bonds on startup. If the warning says `Insufficient free MATRA to auto-bond`, request a faucet drip and restart. See [Auto-bond behaviour](#auto-bond-behaviour). |
| `substrate_connected: false` in status | Can't reach RPC endpoint | Check that `wss://materios.fluxpointstudios.com/rpc` is reachable from your network |
| High finality gap (>10) | Chain-wide issue, not your daemon | Check the explorer dashboard — if all validators show high gap, it's a chain stall |
| `No locator found` in logs | Blob not yet uploaded for a receipt | Normal during brief windows — daemon retries automatically |
| Attestation tx fails with "Priority too low" | Nonce collision (rare) | Daemon auto-recovers on next poll cycle |
| Account balance too low | MATRA depleted from transaction fees | Daemon auto-requests a faucet drip; if faucet is dry, ask FPS team |

## FAQ

**Q: Do I need to run a Materios blockchain node?**
No. The daemon connects to the public RPC endpoint. You only run the cert-daemon container.

**Q: What happens if my daemon goes offline?**
The committee continues with the remaining members (threshold is 2-of-3). Your heartbeat will show "offline" on the explorer. Receipts can still be certified as long as at least 2 committee members are online. Restart your daemon when ready — it catches up automatically.

**Q: How much bandwidth does this use?**
Minimal. The daemon polls for new blocks every 3-12 seconds (small JSON-RPC calls) and sends a heartbeat every 30 seconds. Expect <1 GB/month.

**Q: Can I run this on a Raspberry Pi / ARM device?**
The Docker image is built for `linux/amd64`. ARM support (including Apple Silicon) is not available yet.
