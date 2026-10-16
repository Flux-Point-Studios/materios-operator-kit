# Cardano BP block watcher

`ops/cardano_bp_block_watcher.py` pages Discord when the Materios preprod stake pool
(`pool1y36klnfa4kc3hyggc3fujrm3j6zlgf9r8jhtyufzmgufz3k5pt2`) forges a Cardano block.
`cardano-bp-block-watcher.timer` runs one check every five minutes: the pool's lifetime
`blocks_minted` on Iagon Insight (`https://preprod.insight.iagon.com/api/v0`) against the
count the last check stored in `/var/lib/cardano-bp-block-watcher/last-block-count`. The
first check only stores the count. A new block pages with the newest block's hash,
height, epoch and slot; a check that cannot read Insight, or whose page Discord refuses,
exits 1 and leaves the stored count, so the next run pages the block again.

Of the pool's fields it reads only `blocks_minted`; Insight serves the `live_*` fields lagged
on preprod.

## Installing it

The script is standard-library Python. Its two secrets reach it as systemd credentials;
neither the script nor the unit holds them:

```sh
sudo install -d -m 0700 /etc/cardano-bp-block-watcher
# write each secret from stdin, never from a command line
sudo install -m 0600 /dev/stdin /etc/cardano-bp-block-watcher/insight.key
sudo install -m 0600 /dev/stdin /etc/cardano-bp-block-watcher/discord-webhook
sudo install -m 0700 ops/cardano_bp_block_watcher.py /home/cardano-bp/scripts/block-watcher.py
sudo install -m 0644 ops/cardano-bp-block-watcher.service ops/cardano-bp-block-watcher.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cardano-bp-block-watcher.timer
sudo systemctl start cardano-bp-block-watcher.service
journalctl -u cardano-bp-block-watcher.service -n 5
```
