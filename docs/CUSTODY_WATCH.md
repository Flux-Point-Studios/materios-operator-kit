# Custody watch

`daemon/custody_watch.py` is a read-only watcher that pages a Discord webhook on every
custody and authority move it can see. It holds no signing key and submits nothing.

## What it pages

| Source | CRITICAL (immediate, `@here`) | ALERT (immediate) | INFO (daily digest) |
|---|---|---|---|
| Materios finalized blocks | any `Sudo` call, a multisig leg whose account is `Sudo.Key`, anything signed by `Sudo.Key`, `System` code and storage changes, `Balances`/`Vesting` force calls, `Treasury` spends, `Grandpa.note_stalled`, main-chain script changes, root-gated `OrinqReceipts` levers, any `RootTimelock` call, any `Recovery` call from `Sudo.Key` or an authority account, a vouch, claim, `cancel_recovered` or `as_recovered` that names one and could take effect (below), `Sudo.Key` changing, any change in the recovery of `Sudo.Key` or an authority other than a recovery started (below), a `RuntimeEnvironmentUpdated` header digest, the runtime code's hash changing, a new genesis (chain reset), an extrinsic the classifier cannot read, and one the runtime metadata cannot decode, or the block's decode budget does not reach, that is unsigned or signed by an account that can act for `Sudo.Key` or an authority | any other `Recovery` call, a recovery of `Sudo.Key` or an authority started with no friend's vouch, an extrinsic that cannot be decoded from any other account, session key changes, equivocation reports, native token transfers, committee membership changes, a call in neither the severity table nor the routine list | committee rotations with unchanged membership; an attempt that could not take effect (below); the recovery state as first read |
| Cardano custody addresses | any outflow, collateral a failed script consumed included | any inflow | reads as a reference input |
| Cardano contract addresses | a spend, at the address's `severity` | a spend, at the address's `severity`; any payment in | |
| Cardano policies | mint or burn, as configured | as configured | |
| Surrender pool | a spend that is not exactly a surrender: another redeemer, cMATRA to a wallet that gave up no legacy units or beyond the entitlement of the units it gave up, legacy units reaching a wallet instead of the quarantine address, cMATRA paid into the quarantine address, an overpayment, non-cMATRA value moved, a continuing output without its datum, a custody wallet as claimant, any mint or burn in the spend, a surrendered unit outside its redemption's pinned asset names | an underpayment, an asset outside the rate table, a payout above `max_payout`, value arriving outside a pool spend, the quarantine address holding more of a redemption than its rate-table supply | a surrender paid exactly its rate-table entitlement |

Each Materios page carries the decoded call tree, the signer, the derived multisig
account and, while the node still holds the block's state, the dispatch result. Its
headline names the most severe call's path (`Sudo.sudo > Utility.batch_all >
System.set_code`), and every privileged call is listed with its own arguments, most
severe first, ahead of the tree, so a page cut to Discord's length still shows what
matters. Calls nested as deep as a runtime decodes them (`MAX_EXTRINSIC_DEPTH`, 256)
are decoded and named. An extrinsic that still cannot be decoded may hide any call its
signer can make, so it is paged in a finding that lists each such extrinsic's signer,
size and hash. Those signed by `Sudo.Key`, a configured authority, or an account that
can act in the recovery of one (a friend, a proxy acting as one, or a rescuer its
friends have vouched for up to its threshold, read at the head) page CRITICAL alone, one
finding per signer and block. Unsigned ones, which only the runtime admits, page
CRITICAL grouped. Those signed by anyone else page one grouped ALERT a block: no state
lets such a signer move custody or authority, and any funded account can send one. Every call of the
pinned runtime is in the severity table or the routine list, and a test holds it there;
a call a runtime upgrade adds pages as an ALERT until it is classified. Text from the
chain is rendered as JSON with every backtick replaced, so it can never close the
page's code block or format itself outside it.

A surrender's payout is checked per wallet. Each payment credential is credited with the
legacy units it gave up net of its change, and must be paid, net of its own cMATRA
coming back as change, exactly the rate-table entitlement of those units. A wallet that
adds an input to someone else's surrender, whether it carries only ADA or a legacy unit
of its own, cannot take that surrender's payout without a CRITICAL page. The payment
credential decides who can spend a payout, so a stake credential never joins two wallets.

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
key at that block, so the call pages whatever key the watcher last read.

An ordinary account's call can take effect only where the chain's state lets it: a
`Sudo` call only for `Sudo.Key`; `as_recovered`, and `cancel_recovered` of a watched
account, only for the proxy of the account it names; a vouch for the recovery of
`Sudo.Key` or an authority only from one of its friends; a claim only from a rescuer its
friends have vouched for up to its threshold; `System.apply_authorized_upgrade` only
once Root has authorized an upgrade; `Treasury.payout` only for a spend Root approved.
The extrinsic's events do not settle this: any funded account can push a block's events
past the decode budget, and `Utility.force_batch` succeeds whether or not the calls it
wraps do. So for such a call that did not fail outright, the watcher reads the storage
that decides it (`Sudo.Key`, `Recovery.Proxy`, `Recovery.Recoverable`,
`Recovery.ActiveRecoveries`, `System.AuthorizedUpgrade`, `Treasury.Spends`) at the
block's parent and at the block, in storage queries of at most 1,000 keys. `Sudo.Key`
must be the same at both to prove anything; for the rest either state letting the call
through is enough to count it. `Sudo.Key` and a `Recovery.Proxy` can change and change
back inside one block, so neither proves anything in a block where `Sudo.Key`, an
authority, or a friend, proxy or vouched rescuer in the recovery of one (read at the
head, below) made a privileged call or signed something that cannot be decoded. A call the state rules out could not take effect and goes to the
digest with everything it wraps. A root-gated call from an account needs no proof. What
the state cannot rule out, as when it is pruned, pages, grouped per source rather than
per signer when the events are unread; an authority's attempts always page alone, and so
does what a friend, a proxy or a vouched rescuer does in the recovery of `Sudo.Key` or
an authority.

`Sudo.Key` has a recovery config on the chain, so its friends can take it over together
with a rescuer after a delay. Each poll reads, at the finalized head, `Recovery.Recoverable`
of `Sudo.Key` and of every configured authority, their `Recovery.ActiveRecoveries`, and
every `Recovery.Proxy` that acts as one of those accounts or for one: a key listing per
account and one of the whole `Proxy` map, then their values in storage queries of at most
1,000 keys each. Reading the whole map finds a proxy that no recovery under way names:
one Root's `set_recovered` made, one kept after its recovery was closed, or one older
than the watcher. The first read goes to the digest; any change after it, an entry
added, changed or removed, pages CRITICAL alone with the friends, threshold, delay,
rescuer and vouches decoded, a removed entry as it last stood. The one exception is a
poll whose only changes are recoveries started with no friend's vouch yet: any funded
account can start one each poll, and it can do nothing until friends vouch, so those
page as an ALERT in the group `<source> recovery started`. Each entry is decoded once,
when it is first read with its value, so each recovery of `Sudo.Key` a stranger starts
costs a poll one more key to read and one decode. What the friends and rescuers it names sign
decodes ahead of other accounts' extrinsics (below), but none of them counts as an
authority: any funded account becomes a rescuer of `Sudo.Key` by starting a recovery of
it for a deposit. Their attempts are judged like any other account's, and a vouch, claim,
`cancel_recovered` or `as_recovered` naming `Sudo.Key` or an authority pages CRITICAL once
it can have taken effect (above). Starting a recovery, and naming `Sudo.Key` as a friend in
one's own recovery config, give no power over `Sudo.Key` and page as an ALERT. An authority
acting as the rescuer of some other account
is paged from its own extrinsics, and from its `Recovery.Proxy` once it claims.

## Surrender-pool coverage

With `surrender_pool.coverage` configured, the daily digest says whether the pool can pay
everything still redeemable before the deadline. It reads the merger's redemption pin
and rate table as the merger publishes them (`audit_pack/<date>/redemption_pin.json`,
`audit_pack/<date>/rate_table_cmatra.json`). A unit may still be surrendered up to its
supply at the pin, less the team waiver, less what the quarantine address held at the
pin; units the quarantine address has received since are no longer outstanding. The
outstanding units of each asset are summed and priced floor(count × numerator /
denominator), as the merger's `compute_redemption` prices a surrender, and a test holds
the result equal to the merger's own figure at the pin. The digest gives:

- the pool's cMATRA balance and what is outstanding, with the coverage and any shortfall;
- the cMATRA the pool paid out in the last 7 days, read from each of its transactions
  (at most 200; beyond that the figure is a lower bound and says so);
- the days to the deadline, and how long the pool lasts at the last 7 days' pace.

Before the deadline, a pool that covers less than `floor_percent` (default 90) of what is
outstanding, or that at the last 7 days' pace runs out before the deadline and within
`runout_page_days` (default 14), pages an ALERT, without `@here`, once a day. The reading
happens once a day with the digest; one that fails is named in the digest in its place.
The config refuses a pinned asset the rate table does not price, and a `redemptions`
entry priced differently from the rate table, so the digest and the surrender checks
never disagree about a rate. It also refuses a `deadline_utc` without a UTC offset,
which would otherwise move with the host's time zone.

```json
"coverage": {
  "redemption_pin_file": "redemption_pin.json",
  "rate_table_file": "rate_table_cmatra.json",
  "deadline_utc": "2026-11-29T00:00:00Z",
  "floor_percent": 90,
  "runout_page_days": 14
}
```

## Paging

Pages go out in three lanes, most severe first within each:

1. What only `Sudo.Key`, an authority account, a custody key, or the keys that spend
   the surrender pool or mint under a watched policy can cause: what `Sudo.Key` or an
   authority signs, its multisig legs included; what a friend, a proxy, or a rescuer
   its friends have vouched for does in the recovery of one; a change of `Sudo.Key`, of the runtime
   code or of the genesis; a runtime environment digest; a change in the recovery of
   `Sudo.Key` or an authority other than a recovery started; a custody outflow; a
   surrender-pool spend that is not a surrender; a mint or burn under a watched policy.
2. Every other finding that pages alone, such as a coverage page, a committee change or
   a stale source.
3. Groups. Only what an account anyone can be causes is grouped, per source: Materios
   findings by signer, those whose dispatch result could not be verified and anything
   unclassifiable per chain, and on Cardano payments into, or contract spends from, one
   address, and a surrender whose claimant made it underpay or pay above `max_payout`,
   per address.

A group pages its first finding at once. After that it waits ten minutes
(`GROUP_WINDOW`) and pages everything it gathered in one message, so an account that
raises a finding in every block costs one message per ten minutes. All groups together
take at most one post every 30 seconds, a fifth of the bucket below, however many
accounts send them, and when more than three groups are ready they go out in one
summary that names each group, its count and its first headline.

Every post, page or digest, spends a token from a bucket that holds three and refills
one every 6 seconds: ten messages a minute at most, a third of Discord's 30 a minute
per channel and inside its 5 per 2 seconds per webhook. However many accounts flood a
block, the watcher never holds the webhook at its rate limit, so a watchdog that shares
it still gets through. Only a page of the first lane may spend the bucket's last token,
so it goes out the moment it is found whatever else is paging. Once the digest is due,
one more token is kept back for it, so a flood cannot starve the watcher's liveness
signal. `discord_webhook_file` gives the watcher its own channel (below); without it, it
pages through `DISCORD_WEBHOOK_URL`.

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
`tests/fixtures/custody/config.json` shows every field of a source, and a pool's
`coverage` block is shown above; the shape is:

```json
{
  "state_db": "/var/lib/custody-watch/state.db",
  "digest_hour_utc": 13,
  "source_stale_seconds": 900,
  "discord_webhook_file": "discord-webhook",
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
multisig's signatories. Recovery friends and rescuers are never authority accounts,
whatever the recovery state names. A relative `project_id_file` is read from the config file's
directory. `discord_webhook_file` names a file, read the same way, that holds the
webhook of a channel of the watcher's own; without it the webhook comes from
`DISCORD_WEBHOOK_URL` in the environment. `run` refuses to start without an https
webhook from one or the other, and `test-page --config <config>` posts through the one
`run` would use. The failure page of `custody-watch-failed.service` reads no config and
always goes through `DISCORD_WEBHOOK_URL`.

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
LoadCredential=discord-webhook:/etc/custody-watch/discord-webhook
LoadCredential=redemption_pin.json:/etc/custody-watch/redemption_pin.json
LoadCredential=rate_table_cmatra.json:/etc/custody-watch/rate_table_cmatra.json
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
read (`%d`), where the config's relative paths find them; the unit needs one
`LoadCredential=` line for each file its config names.
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
  Extrinsics signed by `Sudo.Key` or a configured authority, whose signer the watcher
  reads from the extrinsic header before decoding any call, are decoded first against a
  budget of their own: any funded account can fill a block with extrinsics smaller than
  a multisig leg, but none can sign as those accounts. The rest share the block's
  budget: unsigned extrinsics first, the inherents among them, then those signed by a
  recovery friend of `Sudo.Key` or an authority, then by a rescuer of one, then
  everyone else's, smallest first within each. A large extrinsic cannot spend what the
  inherents and a small privileged call need, and filler cannot push a friend's vouch
  out. Anyone can become a rescuer, so rescuers' filler can push out another rescuer's
  claim, though never a friend's vouch or an authority's leg; that claim still pages
  CRITICAL once its friends have vouched for it (above), and as a change in
  `Recovery.Proxy` once it takes effect. What a budget does not reach pages with its
  signer, at the severity above, and the cursor moves on. Events past the budget are left
  unread, and the block's state decides what its attempts could do (above). A
  Materios poll ends after the block that brings it to the budget, so the Cardano
  sources are read between expensive blocks. Every call tree is walked once, holds at
  most the 200 lines a page can show, and long arguments are rendered from a hash or
  their first characters, so rendering never copies a block-sized value.
- **Its own death is visible.** The daily digest is also the liveness signal, and it
  names every stale source instead of reporting the watcher alive. A source that has
  not been read up to its head for `source_stale_seconds` is paged CRITICAL, again
  every hour it stays that way, and once more (ALERT) when it recovers. That covers a
  source whose reads fail and one read without error that stays behind, as when its
  blocks take longer to read than the chain takes to make them. A node answering with a
  finalized head that has not moved for `source_stale_seconds`, or a Cardano tip older
  than that, counts as unreadable, since a node that has stopped following its chain
  still answers every read. systemd restarts a loop
  that stops pinging its watchdog, and `OnFailure` pages when restarts are exhausted.
- **A runtime replaced by a raw storage write.** `System.set_storage` of the runtime
  code under `Sudo` pages CRITICAL like any root-gated call, but deposits no runtime
  upgrade digest. Each poll reads the hash of `:code` at the finalized head, pages
  CRITICAL when it changes, and decodes the blocks after that with the new code's
  metadata; decoders and stored metadata are keyed by that hash, never by a spec version
  a new runtime may reuse. Blocks read in the same poll as the write, before its head,
  are decoded with the metadata of the runtime it replaced. Every
  `RuntimeEnvironmentUpdated` header digest pages CRITICAL from the header alone, so no
  undecoded extrinsic can hide a new runtime.

## Replaying history

```sh
python -m daemon.custody_watch backtest --config /etc/custody-watch/config.json \
    --days 30 --state /tmp/custody-backtest.db [--materios-rpc ws://<other node>:9944]
```

prints every finding the watcher would have raised over the window, one JSON object per
line, and pages nothing. Blocks whose state the node has pruned are decoded against the
node's current runtime; an extrinsic that runtime cannot read is reported as CRITICAL,
never dropped.
