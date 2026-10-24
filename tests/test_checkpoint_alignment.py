"""CHECKPOINT_ALIGN_SECONDS: the interval trigger fires at UTC multiples of N
seconds instead of CHECKPOINT_INTERVAL minutes after the last anchor.

Each test drives the checkpointer the way the poll loop does, on a fake clock:
a block every BLOCK_SECONDS, one tick every TICK seconds, and an anchor worker
that takes WORKER_SECONDS to answer. A receipt certified at time t is in block
t // BLOCK_SECONDS, and the daemon adds it at its first tick at or after t."""
import hashlib
import logging
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

import daemon.checkpoint as checkpoint
from daemon.checkpoint import CardanoCheckpointer
from daemon.config import DaemonConfig

GENESIS = "0e46e33f639a56cc8780fd871d9a15e16d99af248526f907cb560cb40849f7bf"
HOUR = 3600
BLOCK_SECONDS = 6
# POLL_INTERVAL_IDLE plus the tick's own work. It does not divide an hour, so
# the ticks fall at every phase of the boundary.
TICK = 12 + 1.7
WORKER_SECONDS = 4.3
B = 1792839600  # 2026-10-24T11:00:00Z


@dataclass
class Anchor:
    at: float
    leaves: list


def _receipt(n: int) -> tuple[str, bytes]:
    return "0x" + hashlib.sha256(b"receipt-%d" % n).hexdigest(), hashlib.sha256(b"cert-%d" % n).digest()


def _leaf(n: int) -> str:
    receipt_id, cert_hash = _receipt(n)
    return hashlib.sha256(
        b"materios-checkpoint-v1" + bytes.fromhex(GENESIS) + bytes.fromhex(receipt_id[2:]) + cert_hash
    ).hexdigest()


class Daemon:
    def __init__(self, tmp_path, monkeypatch, start, worker_seconds=WORKER_SECONDS, **overrides):
        self.now = float(start)
        self.worker_seconds = worker_seconds
        self.worker_down = (0.0, 0.0)
        self.config = DaemonConfig(signer_uri="//Alice", cardano_anchor_url="http://anchor.test", blob_gateway_url="")
        for name, value in overrides.items():
            setattr(self.config, name, value)
        self.state_file = str(tmp_path / "checkpoint-state.json")
        self.anchors: list[Anchor] = []
        self.ticks: list[float] = []
        self.unseen: list[tuple[float, int]] = []
        self.certified = 0
        monkeypatch.setattr(checkpoint, "time", SimpleNamespace(time=lambda: self.now, sleep=self._sleep))
        monkeypatch.setattr(checkpoint, "requests", SimpleNamespace(post=self._post))
        monkeypatch.delenv("BLOB_GATEWAY_URL", raising=False)
        self.cp = CardanoCheckpointer(self.config, self.state_file)

    def certify(self, at: float) -> str:
        """Certify a receipt at `at`; returns its checkpoint leaf."""
        n = self.certified
        self.certified += 1
        self.unseen.append((at, n))
        return _leaf(n)

    def tick(self):
        self.ticks.append(self.now)
        for at, n in sorted(c for c in self.unseen if c[0] <= self.now):
            self.unseen.remove((at, n))
            receipt_id, cert_hash = _receipt(n)
            self.cp.add_cert(receipt_id, cert_hash, block_num=int(at // BLOCK_SECONDS))
        if self.cp.should_flush():
            self.cp.flush(current_best_block=int(self.now // BLOCK_SECONDS), live_chain_genesis=GENESIS)

    def run(self, until: float):
        while self.now < until:
            self.tick()
            self.now += TICK

    def down(self, until: float):
        self.now = until

    def restart(self):
        self.cp = CardanoCheckpointer(self.config, self.state_file)

    def batches(self) -> list[list[str]]:
        return [a.leaves for a in self.anchors]

    def lateness(self, boundaries: list[float]) -> list[float]:
        """Seconds from each boundary to its anchor, checking the anchor came at
        the first tick on or after that boundary."""
        assert len(self.anchors) == len(boundaries), [a.at - B for a in self.anchors]
        late = []
        for anchor, boundary in zip(self.anchors, boundaries):
            previous_tick = max(t for t in self.ticks if t < anchor.at)
            assert previous_tick < boundary <= anchor.at, (
                f"anchor at B{anchor.at - B:+.1f}s is not the first tick after boundary B{boundary - B:+.0f}s"
            )
            late.append(anchor.at - boundary)
        return late

    def _post(self, url, json, headers, timeout):
        if self.worker_down[0] <= self.now < self.worker_down[1]:
            return SimpleNamespace(status_code=500, text="worker down")
        self.anchors.append(Anchor(self.now, json["batchMetadata"]["leafHashes"]))
        self.now += self.worker_seconds
        return SimpleNamespace(status_code=200, json=lambda: {"anchorId": json["anchorId"]}, text="")

    def _sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def daemon(tmp_path, monkeypatch):
    return lambda start, **kwargs: Daemon(tmp_path, monkeypatch, start, **kwargs)


def aligned(**overrides):
    return {"checkpoint_align_seconds": HOUR, "max_leaf_wait_seconds": 2 * HOUR, **overrides}


def test_checkpoint_align_seconds_is_read_from_the_environment_and_off_when_unset(monkeypatch):
    monkeypatch.delenv("CHECKPOINT_ALIGN_SECONDS", raising=False)
    assert DaemonConfig.from_env().checkpoint_align_seconds == 0
    monkeypatch.setenv("CHECKPOINT_ALIGN_SECONDS", "3600")
    assert DaemonConfig.from_env().checkpoint_align_seconds == 3600


def test_without_alignment_the_interval_still_runs_from_the_last_anchor(daemon):
    d = daemon(B + 1000, max_leaf_wait_seconds=2 * HOUR)
    first = d.certify(B + 1000)
    second = d.certify(B + 1500)
    d.run(until=B + 1000 + 2 * HOUR)
    assert d.batches() == [[first], [second]]
    since_first = d.anchors[1].at - (d.anchors[0].at + WORKER_SECONDS)
    assert HOUR <= since_first < HOUR + TICK


def test_48_aligned_flushes_each_land_on_the_first_tick_after_their_boundary(daemon):
    start = B - 2557  # 10:17:23Z, a start well into the hour
    d = daemon(start, **aligned())
    for i in range(48 * 9):
        d.certify(start + 61 + 420 * i)
    d.run(until=B + 47 * HOUR + 60)
    late = d.lateness([B + i * HOUR for i in range(48)])
    assert all(0 <= x < TICK for x in late), late
    anchored = [leaf for batch in d.batches() for leaf in batch]
    assert len(anchored) == len(set(anchored))


def test_a_leaf_12_blocks_deep_at_the_boundary_tick_is_in_that_anchor_and_a_later_one_waits_for_the_next(daemon):
    d = daemon(B - 1800, **aligned())
    early = d.certify(B - 73)  # in a block 13 below the block at B
    late = d.certify(B - 40)  # 12 blocks deep only from B + 30
    alone = d.certify(B + 2 * HOUR - 40)  # the only leaf pending at B + 2h, still unconfirmed there
    d.run(until=B + 3 * HOUR + 60)
    assert d.batches() == [[early], [late], [alone]]
    assert all(0 <= x < TICK for x in d.lateness([B, B + HOUR, B + 3 * HOUR]))


def test_an_empty_boundary_makes_no_anchor_and_a_mid_hour_leaf_waits_for_the_next_one(daemon):
    d = daemon(B - 1800, **aligned(checkpoint_interval=1))
    first = d.certify(B - 900)
    mid_hour = d.certify(B + HOUR + 1200)
    d.run(until=B + HOUR + 1200)
    d.now += 100  # a slow tick: the leaf is already 12 blocks deep when the daemon adds it
    d.run(until=B + 2 * HOUR + 60)
    assert d.batches() == [[first], [mid_hour]]
    assert all(0 <= x < TICK for x in d.lateness([B, B + 2 * HOUR]))


def test_a_restart_neither_repeats_a_swept_boundary_nor_skips_the_next_one(daemon):
    d = daemon(B - 1800, **aligned())
    first = d.certify(B - 900)
    straddler = d.certify(B - 30)  # unconfirmed when B is swept, 12 blocks deep from B + 42
    later = d.certify(B + HOUR + 600)
    d.run(until=B + 40)
    d.restart()
    d.run(until=B + 1800)
    d.restart()
    d.run(until=B + 2 * HOUR + 60)
    assert d.batches() == [[first], [straddler], [later]]
    assert all(0 <= x < TICK for x in d.lateness([B, B + HOUR, B + 2 * HOUR]))


def test_a_boundary_missed_while_the_daemon_was_down_flushes_at_the_first_tick_after_restart(daemon):
    d = daemon(B - 1800, **aligned())
    first = d.certify(B - 900)
    missed = d.certify(B + 1800)
    late = d.certify(B + HOUR + 1500)
    d.run(until=B + HOUR - 300)
    d.down(until=B + HOUR + 1200)
    d.restart()
    d.run(until=B + 2 * HOUR + 60)
    assert d.batches() == [[first], [missed], [late]]
    assert d.anchors[1].at == B + HOUR + 1200
    assert 0 <= d.anchors[2].at - (B + 2 * HOUR) < TICK


def test_a_boundary_whose_flush_fails_stays_due_until_a_flush_succeeds(daemon):
    d = daemon(B - 1800, **aligned())
    leaf = d.certify(B - 900)
    d.worker_down = (B - 5, B + 60)
    d.run(until=B + HOUR - 60)
    assert d.batches() == [[leaf]]
    assert B + 60 <= d.anchors[0].at < B + 60 + TICK


def test_the_size_trigger_still_flushes_between_boundaries_and_the_next_boundary_still_fires(daemon):
    d = daemon(B - 1800, **aligned(checkpoint_batch_size=3))
    trio = [d.certify(B + 1200 + i) for i in range(3)]  # all in block B/6 + 200
    after = d.certify(B + 2400)
    d.run(until=B + HOUR + 60)
    assert d.batches() == [trio, [after]]
    assert B + 1272 <= d.anchors[0].at < B + 1272 + TICK
    assert 0 <= d.anchors[1].at - (B + HOUR) < TICK


def test_the_age_trigger_still_flushes_between_boundaries(daemon):
    d = daemon(B - 1800, **aligned(max_leaf_wait_seconds=600))
    leaf = d.certify(B + 300)
    d.run(until=B + HOUR + 60)
    assert d.batches() == [[leaf]]
    assert B + 900 < d.anchors[0].at < B + 900 + 2 * TICK


def test_a_flush_that_runs_across_a_boundary_leaves_that_boundary_due(daemon):
    d = daemon(B + 600, worker_seconds=20, **aligned(max_leaf_wait_seconds=HOUR - 610))
    aged = d.certify(B + 600)
    fresh = d.certify(B + HOUR - 50)  # 12 blocks deep from B + h + 18
    d.tick()
    d.now = B + HOUR - 5  # aged passes MAX_LEAF_WAIT_SECONDS; the worker answers at B + h + 15
    d.tick()
    d.now = B + HOUR + 30
    d.tick()
    assert d.batches() == [[aged], [fresh]]
    assert [a.at for a in d.anchors] == [B + HOUR - 5, B + HOUR + 30]


@pytest.mark.parametrize(
    "align, max_wait, warned",
    [(HOUR, 90, True), (HOUR, HOUR, True), (HOUR, 2 * HOUR, False), (0, 90, False)],
)
def test_a_max_leaf_wait_not_above_the_alignment_is_flagged_at_startup(daemon, caplog, align, max_wait, warned):
    with caplog.at_level(logging.WARNING, logger="daemon.checkpoint"):
        daemon(B, checkpoint_align_seconds=align, max_leaf_wait_seconds=max_wait)
    assert any("MAX_LEAF_WAIT_SECONDS" in r.getMessage() for r in caplog.records) is warned
