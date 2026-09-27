# Custody watch

`daemon/custody_watch.py` is a read-only watcher that pages a Discord webhook on every
custody and authority move it can see. It holds no signing key and submits nothing.

## What it pages

| Source | CRITICAL (immediate, `@here`) | ALERT (immediate) | INFO (daily digest) |
|---|---|---|---|
| Materios finalized blocks | any `Sudo` call, a multisig leg whose account is `Sudo.Key`, anything signed by `Sudo.Key`, `System` code and storage changes, `Balances`/`Vesting` force calls, `Recovery`, `Treasury` spends, `Grandpa.note_stalled`, main-chain script changes, root-gated `OrinqReceipts` levers, `Sudo.Key` changing, a new genesis (chain reset) | session key changes, equivocation reports, native token transfers, committee membership changes, an extrinsic the runtime metadata cannot decode | committee rotations with unchanged membership |
| Cardano custody addresses | any outflow | any inflow | reads as a reference input |
| Cardano contract addresses | per address, as configured | per address, as configured | |
| Cardano policies | mint or burn, as configured | as configured | |
| Surrender pool | a spend that is not exactly a surrender: another redeemer, cMATRA to a non-claimant, an overpayment, non-cMATRA value moved, a continuing output without its datum, a custody wallet as claimant | an underpayment, an asset outside the rate table, value arriving outside a pool spend | a surrender paid exactly its rate-table entitlement |

Each Materios page carries the decoded call tree, the signer, the derived multisig
account and, while the node still holds the block's state, the dispatch result.

## Configuration

The configuration lives on the host that runs the watcher, never in this repository.
`tests/fixtures/custody/config.json` shows every field; the shape is:

```json
{
  "state_db": "/var/lib/custody-watch/state.db",
  "digest_hour_utc": 13,
  "source_stale_seconds": 900,
  "materios": {"name": "materios-preprod", "rpc_url": "ws://<node>:9945", "poll_seconds": 6},
  "cardano": [
    {
      "name": "cardano-mainnet",
      "blockfrost_url": "https://cardano-mainnet.blockfrost.io/api/v0",
      "project_id_file": "blockfrost-mainnet.key",
      "poll_seconds": 60,
      "reorg_depth_blocks": 30,
      "addresses": [{"label": "<name>", "address": "addr1...", "role": "custody"}],
      "policies": [{"label": "<name>", "policy_id": "<56 hex>", "severity": "CRITICAL"}],
      "surrender_pool": {"label": "surrender-pool", "address": "addr1w...", "...": "see the fixture"}
    }
  ]
}
```

`role` is `custody` (outflow CRITICAL, inflow ALERT) or `contract` (every transaction at
the address's own `severity`). A relative `project_id_file` is read from the config
file's directory. The webhook comes from `DISCORD_WEBHOOK_URL` in the environment; `run`
refuses to start without it.

## Running it

```ini
[Unit]
Description=custody-watch: page on custody and authority moves
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=600
StartLimitBurst=5
OnFailure=custody-watch-failed.service

[Service]
Type=notify
NotifyAccess=main
WatchdogSec=900
DynamicUser=yes
EnvironmentFile=/etc/custody-watch/discord.env
LoadCredential=config.json:/etc/custody-watch/config.json
LoadCredential=blockfrost-mainnet.key:/etc/custody-watch/blockfrost-mainnet.key
Environment=PYTHONDONTWRITEBYTECODE=1
WorkingDirectory=/opt/custody-watch/src
ExecStart=/opt/custody-watch/venv/bin/python -m daemon.custody_watch run --config %d/config.json
Restart=always
RestartSec=30
Nice=10
CPUQuota=25%
MemoryMax=512M
StateDirectory=custody-watch
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`LoadCredential` hands the service its config and keys in a directory only it can
read (`%d`), where the config's relative `project_id_file` finds them.
`custody-watch-failed.service` is a oneshot with the same `EnvironmentFile` running
`python -m daemon.custody_watch page-failure --unit custody-watch.service`, so a unit
that exhausts its restarts pages too. It reads no config, so a config that keeps the
watcher from starting cannot also keep that page from going out. `test-page` sends one
message through the webhook to prove delivery end to end.

## Guarantees

- **No double pages on restart.** A block or transaction and its findings commit in one
  SQLite transaction with the source cursor; a finding is keyed by chain position and
  stored once. A page is marked sent only after Discord accepts it, so delivery is
  at least once: a crash between Discord's answer and the mark is the only window for a
  repeat.
- **Nothing is skipped quietly.** The first start begins at the current head and
  baselines each Cardano address and asset; after that every block and every
  transaction in a `reorg_depth_blocks` overlap is read, and an asset newly minted
  under a watched policy is read from its first mint.
- **Its own death is visible.** The daily digest is also the liveness signal. A source
  that cannot be read for `source_stale_seconds` is paged, and paged again when it
  recovers. systemd restarts a loop that stops pinging its watchdog, and
  `OnFailure` pages when restarts are exhausted.

## Replaying history

```sh
python -m daemon.custody_watch backtest --config /etc/custody-watch/config.json \
    --days 30 --state /tmp/custody-backtest.db [--materios-rpc ws://<other node>:9944]
```

prints every finding the watcher would have raised over the window, one JSON object per
line, and pages nothing. Blocks whose state the node has pruned are decoded against the
node's current runtime; an extrinsic that runtime cannot read is reported as an ALERT,
never dropped.
