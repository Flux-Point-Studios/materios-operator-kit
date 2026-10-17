#!/usr/bin/env python3
"""Pages Discord when the Materios stake pool forges a Cardano preprod block.

cardano-bp-block-watcher.timer runs one check every five minutes: the pool's lifetime
``blocks_minted`` on Iagon Insight against the count the last check stored. The first
check only stores it. systemd hands in the Insight key and the Discord webhook as the
credentials ``insight.key`` and ``discord-webhook``. A check that cannot read Insight,
or whose page Discord refuses, fails the run and leaves the stored count, so the next
run pages the block again.
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

POOL_ID = "pool1y36klnfa4kc3hyggc3fujrm3j6zlgf9r8jhtyufzmgufz3k5pt2"
INSIGHT = "https://preprod.insight.iagon.com/api/v0"
STATE_FILE = Path("/var/lib/cardano-bp-block-watcher/last-block-count")
# Discord refuses urllib's default agent.
USER_AGENT = "materios-operator-kit/1.0 (+https://github.com/Flux-Point-Studios/materios-operator-kit)"


def check(read, page, state: Path) -> str:
    """Page the pool's newest block when it forged any since the stored count; the line to log."""
    minted = read(f"/pools/{POOL_ID}")["blocks_minted"]
    if not state.exists():
        state.write_text(f"{minted}\n")
        return f"first run: stored blocks_minted={minted}"
    last = int(state.read_text())
    if minted <= last:
        return f"no change: blocks_minted={minted}"
    newest, height, epoch, slot = "?", "?", "?", "?"
    try:
        [newest] = read(f"/pools/{POOL_ID}/blocks?count=1&order=desc")
        block = read(f"/blocks/{newest}")
        height, epoch, slot = block["height"], block["epoch"], block["slot"]
    except (OSError, ValueError, KeyError) as e:  # the count alone is worth the page
        print(f"block details not read, paging without them: {type(e).__name__}: {e}", file=sys.stderr)
    page(f":tada: **The Materios SPO forged a Cardano preprod block!**\n"
         f"• Lifetime block count: {last} -> {minted} (+{minted - last})\n"
         f"• Latest hash: `{newest}`\n"
         f"• Height {height} | epoch {epoch} | slot {slot}\n"
         f"• https://preprod.cexplorer.io/block/{newest}")
    state.write_text(f"{minted}\n")
    return f"paged block {newest}"


def main() -> int:
    credentials = Path(os.environ["CREDENTIALS_DIRECTORY"])
    key = (credentials / "insight.key").read_text().strip()
    webhook = (credentials / "discord-webhook").read_text().strip()

    def read(path: str):
        request = urllib.request.Request(INSIGHT + path, headers={
            "project_id": key, "User-Agent": USER_AGENT, "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)

    def page(content: str) -> None:
        request = urllib.request.Request(webhook, data=json.dumps({"content": content}).encode(), headers={
            "Content-Type": "application/json", "User-Agent": USER_AGENT})
        urllib.request.urlopen(request, timeout=15).close()

    try:
        print(check(read, page, STATE_FILE), file=sys.stderr)
    except (OSError, ValueError, KeyError) as e:
        # Never the error's text: urllib's names the URL it could not open, the webhook included.
        detail = getattr(e, "code", None) or getattr(e, "reason", "")
        print(f"check failed: {type(e).__name__} {detail}".rstrip(), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
