# Checkpoint anchoring

The cert-daemon that anchors checkpoints (`CHECKPOINT_ENABLED=true`) turns
each `AvailabilityCertified` event into a pending leaf; the committee's echoes
of one certificate count once. A flush builds a Merkle root over pending leaves
and posts it to the anchor worker at `CARDANO_ANCHOR_URL`. External attestors
do not checkpoint: the kit's compose file and `install.sh` set
`CHECKPOINT_ENABLED: "false"`.

## When a flush happens

The daemon checks the triggers once per poll tick (`POLL_INTERVAL_FAST` while
receipts await attestation, `POLL_INTERVAL_IDLE` otherwise).

| Trigger | Variable | Default | Fires when |
|---|---|---|---|
| Size | `CHECKPOINT_BATCH_SIZE` | 100 | at least this many leaves are pending |
| Age | `MAX_LEAF_WAIT_SECONDS` | 90 | the oldest pending leaf was added more than this many seconds ago |
| Interval | `CHECKPOINT_INTERVAL` | 60 (minutes) | this many minutes have passed since the last flush that anchored leaves; 0 turns it off |
| Aligned interval | `CHECKPOINT_ALIGN_SECONDS` | unset (off) | replaces the interval trigger; see below |

A flush anchors every pending leaf whose certification block is at least
`FINALITY_CONFIRMATIONS` (default 12) blocks below the best block. Leaves not
yet that deep stay pending. If the submission fails, the leaves stay pending
and the next tick tries again. No flush runs until the daemon has read the
chain's genesis hash from the node.

## Wall-clock alignment

Set `CHECKPOINT_ALIGN_SECONDS=N` to flush at fixed UTC times instead of
`CHECKPOINT_INTERVAL` minutes after the last anchor. Unset or 0 keeps the
interval trigger above. With N set:

- The boundaries are the multiples of N seconds since 1970-01-01T00:00:00Z.
  An N that divides 86400 puts them at the same UTC times every day: N=3600
  flushes at HH:00:00, and N=900 at :00, :15, :30 and :45.
- The daemon flushes at the first poll tick on or after each boundary, so the
  anchor is posted at most one tick (`POLL_INTERVAL_IDLE`, 12 s by default,
  plus the tick's own work) after it. Each boundary is a fixed time, so a slow
  flush does not push the next one later and the schedule does not drift.
- `CHECKPOINT_INTERVAL` is ignored.
- A boundary's flush anchors the leaves confirmed at that tick. A leaf not
  yet `FINALITY_CONFIRMATIONS` blocks deep, or added after the boundary, waits
  for the next boundary, unless the size or age trigger flushes it first.
- A boundary with no confirmed leaf pending makes no anchor and no Cardano
  transaction.

### Restarts and failures

The daemon records the start of each flush that left no confirmed leaf pending
(a sweep) as `last_sweep_time` in `checkpoint-state.json`. A flush with
nothing to anchor is a sweep too. A boundary is due once it has passed since
the last sweep. Therefore:

- A restart after a boundary's sweep does not flush that boundary again, and
  the next boundary still flushes.
- A boundary that passed while the daemon was down is flushed at the first
  tick after the restart. The boundaries after it follow the clock.
- A boundary whose flush fails stays due, and every tick retries it until a
  flush succeeds.
- A flush that starts before a boundary and ends after it does not sweep that
  boundary.
- A state file written by a daemon version without `CHECKPOINT_ALIGN_SECONDS`
  has no `last_sweep_time`. The first tick after enabling alignment on such a
  file flushes, anchoring any confirmed pending leaves at once.

### Precedence with the size and age triggers

The size and age triggers still fire between boundaries. A successful flush
started by either one is a sweep: it clears a boundary that is still due, but
never a later one, so the next boundary still flushes whatever is confirmed by
then.

With alignment on, the age trigger is a backstop. It fires whenever the oldest
pending leaf is older than `MAX_LEAF_WAIT_SECONDS`, at a boundary or not. A
leaf can wait up to N plus the confirmation wait (about 72 s at 12 blocks of
about 6 s) plus a tick for its boundary. Set `MAX_LEAF_WAIT_SECONDS` above
that so that the age trigger does not anchor between boundaries. With N=3600,
7200 keeps it from firing while boundary flushes succeed. At startup, the
daemon logs a warning when `MAX_LEAF_WAIT_SECONDS` is not above N. At exactly
N, a leaf certified in about the last 72 s before a boundary misses that
boundary and can reach the age limit just before the next one. It is then
anchored a few seconds before that boundary instead of at it.

Hourly anchors:

```
CHECKPOINT_ALIGN_SECONDS=3600
MAX_LEAF_WAIT_SECONDS=7200
```

## Submission cut-off for callers

With `CHECKPOINT_ALIGN_SECONDS=3600` and `FINALITY_CONFIRMATIONS=12` at about
6 s blocks:

- The HH:00 anchor holds every leaf whose certification block is at least 12
  blocks below the best block at the first tick on or after HH:00:00. That is
  a receipt certified by about HH:58:48.
- Submit by HH:58 to be in the HH:00 anchor. This leaves about 48 s for the
  receipt to be included in a block and certified.
- A receipt certified later can miss that anchor and is then in the next
  hour's.
- The daemon posts the anchor to the anchor worker within one poll tick of
  HH:00:00. The Cardano transaction follows from the worker.
