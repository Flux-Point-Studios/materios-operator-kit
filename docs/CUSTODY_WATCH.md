# Custody watch

`daemon/custody_watch.py` is a read-only watcher that pages a Discord webhook on every
custody and authority move it can see. It holds no signing key and submits nothing.

## What it pages

| Source | CRITICAL (immediate, `@here`) | ALERT (immediate) | INFO (daily digest) |
|---|---|---|---|
| Materios finalized blocks | any `Sudo` call, a multisig leg whose account is `Sudo.Key`, anything signed by `Sudo.Key`, `System` code and storage changes, `Balances`/`Vesting` force calls, `Treasury` spends, `Grandpa.note_stalled`, main-chain script changes, root-gated `OrinqReceipts` levers, any `RootTimelock` call, a `Recovery` call that names `Sudo.Key` or an authority account, `Sudo.Key` changing, a new genesis (chain reset), an extrinsic the runtime metadata cannot decode, the block's decode budget does not reach, or the classifier cannot read | any other `Recovery` call, session key changes, equivocation reports, native token transfers, committee membership changes, a call in neither the severity table nor the routine list | committee rotations with unchanged membership; an attempt that could not take effect (below) |
| Cardano custody addresses | any outflow, collateral a failed script consumed included | any inflow | reads as a reference input |
| Cardano contract addresses | a spend, at the address's `severity` | a spend, at the address's `severity`; any payment in | |
| Cardano policies | mint or burn, as configured | as configured | |
| Surrender pool | a spend that is not exactly a surrender: another redeemer, cMATRA to a non-claimant, an overpayment, non-cMATRA value moved, a continuing output without its datum, a custody wallet as claimant, any mint or burn in the spend, a surrendered unit outside its redemption's pinned asset names | an underpayment, an asset outside the rate table, a payout above `max_payout`, value arriving outside a pool spend, the quarantine address holding more of a redemption than its rate-table supply | a surrender paid exactly its rate-table entitlement |

Each Materios page carries the decoded call tree, the signer, the derived multisig
account and, while the node still holds the block's state, the dispatch result. Its
headline names the most severe call's path (`Sudo.sudo > Utility.batch_all >
System.set_code`), and every privileged call is listed with its own arguments, most
severe first, ahead of the tree, so a page cut to Discord's length still shows what
matters. Calls nested as deep as a runtime decodes them (`MAX_EXTRINSIC_DEPTH`, 256)
are decoded and named; an extrinsic that still cannot be decoded may hide any call, so
it pages CRITICAL, in one finding per block that lists each such extrinsic's size and
hash. Every call of the
pinned runtime is in the severity table or the routine list, and a test holds it there;
a call a runtime upgrade adds pages as an ALERT until it is classified. Text from the
chain is rendered as JSON with every backtick replaced, so it can never close the
page's code block or format itself outside it.

A Cardano transaction whose script fails phase 2 consumes its collateral in place of its
inputs and produces its collateral return in place of its outputs, so a custody key can
move the reserve through one on purpose. db-sync stores that collateral and return as the
transaction's inputs and outputs, and Blockfrost lists them without the `collateral`
flag its documentation describes; for such a transaction the watcher counts every input
and output row it is given, once per UTxO, so the outflow pages CRITICAL with what left
whichever way the rows are flagged. It never counts as a surrender.

Root comes only from `Sudo`, and `Sudo` dispatches only for `Sudo.Key`, so a root-gated
call reached from any other account cannot take effect. Once the block's events are
read, such an attempt, and any extrinsic that failed outright, goes to the digest
rather than paging unless its signer, or a multisig or derivative account of its
signer, is `Sudo.Key` or a configured authority. Naming an authority as the target of
`sudo_as`, `as_recovered` or `dispatch_as` proves nothing about who signed, so it does
not count. An event from the `Sudo` pallet proves its caller held the
key at that block, so the call pages whatever key the watcher last read. While the
events cannot be read, every attempt pages as though it took effect.

## Paging

Pages go out most severe first, and within a severity a finding that pages alone goes
before a group. Findings that anyone can cause cheaply are grouped: Materios findings
by signer (unless an authority is involved), Cardano payments into, or contract
spends from, one address, and anything unclassifiable, per source. Every pending finding of a group goes out as one message.
Custody outflows, surrender-pool spends and watched-policy mints always page alone.

A failing webhook holds every post, the digest included: for its `Retry-After` when
it rate-limits, otherwise for a delay that doubles with each consecutive failure, up
to a minute. That covers an unreachable webhook, a server error, and a revoked or
deleted one that answers 401, 403 or 404 to every post. Such a webhook is asked about
once a minute rather than once per page per cycle, since Discord's edge bans an address
that sends it thousands of refused requests, and the pages wait intact until it takes
them again. A page whose own content is refused (400 or 413) is retried on its own
doubling delay without holding back the rest, and after three refusals goes out as its
headline alone. Every message stays under 1900 characters, inside Discord's 2000
however it counts an emoji. The digest counts the pages still waiting. Pages go
out, and the systemd watchdog is pinged, after each source is polled, and a Cardano poll
classifies at most 200 transactions, so a flood at one address delays neither the other
sources nor their pages.

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

A redemption by `policy_id` must list the `asset_names` (hex) it redeems, pinned from
the supply its rate was set for, so a collection policy that can still mint cannot make a
fresh name redeemable; `max_payout` (cMATRA base units) is the per-surrender payout above
which a surrender pages. A watched policy of more than ten assets has each asset's mint
count read only when that asset's supply moves, and every count once an hour.

`role` is `custody` (outflow CRITICAL, inflow ALERT) or `contract` (a spend at the
address's own `severity`, a payment in as an ALERT). `materios.authority_accounts` lists
the SS58 accounts besides `Sudo.Key` whose moves are authority moves, such as the sudo
multisig's signatories. A relative `project_id_file` is read from the config file's
directory. The webhook comes from `DISCORD_WEBHOOK_URL` in the environment; `run`
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
read (`%d`), where the config's relative `project_id_file` finds them; the unit needs
one `LoadCredential=` line for each key file its config names.
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
- **No input stalls a cursor.** Argument values, addresses and datums are chosen by
  whoever builds the transaction, so classification never trusts their shape. An
  extrinsic, committee inherent or Cardano transaction the classifier still cannot
  read is paged CRITICAL as unclassifiable, and the cursor moves past it.
- **Bounded work per block.** Any funded account can fill a block to its length limit
  with one-byte elements, and scalecodec builds a Python object for each one it reads.
  A block's extrinsics, and separately its events, are decoded within a budget of
  50,000 values (`DECODE_BUDGET`), about a second of CPU and a few megabytes at most.
  Unsigned extrinsics, the inherents among them, are decoded first and signed ones
  smallest first, so filler cannot spend the budget a small privileged leg needs. What
  the budget does not reach pages CRITICAL and the cursor moves on. Events past the
  budget are left unread, so the block's attempts page as though they took effect. A
  Materios poll ends after the block that brings it to the budget, so the Cardano
  sources are read between expensive blocks. Every call tree is walked once, holds at
  most the 200 lines a page can show, and long arguments are rendered from a hash or
  their first characters, so rendering never copies a block-sized value.
- **Its own death is visible.** The daily digest is also the liveness signal, and it
  names every stale source instead of reporting the watcher alive. A source that
  cannot be read for `source_stale_seconds` is paged CRITICAL, again every hour it
  stays unreadable, and once more (ALERT) when it recovers. A node answering with a
  finalized head that has not moved for `source_stale_seconds`, or a Cardano tip older
  than that, counts as unreadable, since a node that has stopped following its chain
  still answers every read. systemd restarts a loop
  that stops pinging its watchdog, and `OnFailure` pages when restarts are exhausted.

## Replaying history

```sh
python -m daemon.custody_watch backtest --config /etc/custody-watch/config.json \
    --days 30 --state /tmp/custody-backtest.db [--materios-rpc ws://<other node>:9944]
```

prints every finding the watcher would have raised over the window, one JSON object per
line, and pages nothing. Blocks whose state the node has pruned are decoded against the
node's current runtime; an extrinsic that runtime cannot read is reported as CRITICAL,
never dropped.
