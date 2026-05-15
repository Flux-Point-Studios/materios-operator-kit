"""Tests for daemon/multisig_aggregator.py — the gateway-mediated peer-sig
bulletin-board client (task #286).

Uses an in-process aiohttp.web app to stand in for the real gateway
(``/v2/multisig_sigs/{kind}/{key}`` endpoints). Sync test functions drive
the coroutines via ``asyncio.run`` — matches the repo's no-pytest-asyncio
convention."""
from __future__ import annotations

import asyncio

import aiohttp
import pytest
from aiohttp import web

from daemon.multisig_aggregator import (
    KIND_EXPIRE,
    KIND_SETTLE,
    MultisigAggregator,
)


# ---- helper: in-proc gateway -------------------------------------------------


class FakeGateway:
    """Minimal ``/v2/multisig_sigs/{kind}/{key}`` server. Records POSTs,
    returns rows on GET. No sr25519 verify here — we only test the client.
    """

    def __init__(self) -> None:
        # key: (kind, key_hex, digest_hex, pubkey_hex) -> sig_hex
        self.store: dict[tuple[str, str, str, str], str] = {}
        # canned responses for failure-mode tests
        self.force_post_status: int | None = None
        self.force_get_status: int | None = None
        self.canned_get_body: dict | None = None

    async def post(self, request: web.Request) -> web.Response:
        if self.force_post_status is not None:
            return web.json_response({"error": "forced"}, status=self.force_post_status)
        kind = request.match_info["kind"]
        key = request.match_info["key"]
        body = await request.json()
        self.store[(kind, key, body["digest"], body["pubkey"])] = body["sig"]
        return web.json_response({"ok": True, "stored": True, "expires_at_unix": 0})

    async def get(self, request: web.Request) -> web.Response:
        if self.force_get_status is not None:
            return web.json_response({"error": "forced"}, status=self.force_get_status)
        if self.canned_get_body is not None:
            return web.json_response(self.canned_get_body)
        kind = request.match_info["kind"]
        key = request.match_info["key"]
        digest_filter = request.query.get("digest")
        sigs = []
        for (k, key_h, dig_h, pub_h), sig_h in self.store.items():
            if k != kind or key_h != key:
                continue
            if digest_filter is not None and dig_h != digest_filter:
                continue
            sigs.append({"pubkey": pub_h, "sig": sig_h, "digest": dig_h, "created_at": 0})
        return web.json_response({"kind": kind, "key": key, "sigs": sigs, "count": len(sigs)})

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/v2/multisig_sigs/{kind}/{key}", self.post)
        app.router.add_get("/v2/multisig_sigs/{kind}/{key}", self.get)
        return app


async def _start_gateway(gw: FakeGateway) -> tuple[web.AppRunner, int]:
    runner = web.AppRunner(gw.app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    # Discover the bound port via the runner's TCP site.
    server = site._server
    sock = server.sockets[0]  # type: ignore[union-attr]
    port = sock.getsockname()[1]
    return runner, port


async def _with_gateway(coro_factory):
    """Spin up gateway + aggregator, run a coroutine that takes
    (gw, agg, session), tear down. Returns whatever the coro returned."""
    gw = FakeGateway()
    runner, port = await _start_gateway(gw)
    try:
        agg = MultisigAggregator(gateway_url=f"http://127.0.0.1:{port}")
        async with aiohttp.ClientSession() as session:
            return await coro_factory(gw, agg, session)
    finally:
        await runner.cleanup()


# ---- tests -------------------------------------------------------------------


def test_share_sig_happy_path():
    async def run(gw, agg, session):
        ok = await agg.share_sig(
            session,
            kind=KIND_SETTLE,
            key=b"\xaa" * 32,
            digest=b"\xbb" * 32,
            pubkey=b"\xcc" * 32,
            sig=b"\xdd" * 64,
        )
        assert ok is True
        assert ("settle", "aa" * 32, "bb" * 32, "cc" * 32) in gw.store
    asyncio.run(_with_gateway(run))


def test_share_sig_validates_byte_lengths():
    async def run(_gw, agg, session):
        with pytest.raises(ValueError, match="digest must be 32 bytes"):
            await agg.share_sig(
                session, kind=KIND_SETTLE, key=b"\xaa" * 32,
                digest=b"\xbb" * 16, pubkey=b"\xcc" * 32, sig=b"\xdd" * 64,
            )
        with pytest.raises(ValueError, match="pubkey must be 32 bytes"):
            await agg.share_sig(
                session, kind=KIND_SETTLE, key=b"\xaa" * 32,
                digest=b"\xbb" * 32, pubkey=b"\xcc" * 16, sig=b"\xdd" * 64,
            )
        with pytest.raises(ValueError, match="sig must be 64 bytes"):
            await agg.share_sig(
                session, kind=KIND_SETTLE, key=b"\xaa" * 32,
                digest=b"\xbb" * 32, pubkey=b"\xcc" * 32, sig=b"\xdd" * 32,
            )
    asyncio.run(_with_gateway(run))


def test_unknown_kind_raises_value_error():
    async def run(_gw, agg, session):
        with pytest.raises(ValueError, match="kind must be"):
            await agg.share_sig(
                session, kind="batch", key=b"\xaa" * 32,
                digest=b"\xbb" * 32, pubkey=b"\xcc" * 32, sig=b"\xdd" * 64,
            )
    asyncio.run(_with_gateway(run))


def test_key_must_be_32_bytes():
    async def run(_gw, agg, session):
        with pytest.raises(ValueError, match="key must be 32 bytes"):
            await agg.share_sig(
                session, kind=KIND_SETTLE, key=b"\xaa" * 16,
                digest=b"\xbb" * 32, pubkey=b"\xcc" * 32, sig=b"\xdd" * 64,
            )
    asyncio.run(_with_gateway(run))


def test_share_sig_returns_false_on_http_error():
    async def run(gw, agg, session):
        gw.force_post_status = 500
        ok = await agg.share_sig(
            session, kind=KIND_SETTLE, key=b"\xaa" * 32,
            digest=b"\xbb" * 32, pubkey=b"\xcc" * 32, sig=b"\xdd" * 64,
        )
        assert ok is False
    asyncio.run(_with_gateway(run))


def test_fetch_envelope_filters_by_digest():
    async def run(gw, agg, session):
        gw.store[("settle", "aa" * 32, "11" * 32, "cc" * 32)] = "dd" * 64
        gw.store[("settle", "aa" * 32, "22" * 32, "cc" * 32)] = "ee" * 64
        env = await agg.fetch_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
        )
        assert len(env) == 1
        assert env[0][0] == bytes.fromhex("cc" * 32)
        assert env[0][1] == bytes.fromhex("dd" * 64)
    asyncio.run(_with_gateway(run))


def test_fetch_envelope_drops_wrong_digest_defense_in_depth():
    """Even if the gateway misbehaves and returns rows with a non-matching
    digest, the client filters them out."""
    async def run(gw, agg, session):
        gw.canned_get_body = {
            "kind": "settle",
            "key": "aa" * 32,
            "sigs": [
                {"pubkey": "cc" * 32, "sig": "dd" * 64, "digest": "99" * 32},
            ],
            "count": 1,
        }
        env = await agg.fetch_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
        )
        assert env == []
    asyncio.run(_with_gateway(run))


def test_fetch_envelope_dedupes_by_pubkey():
    async def run(gw, agg, session):
        gw.canned_get_body = {
            "kind": "settle",
            "key": "aa" * 32,
            "sigs": [
                {"pubkey": "cc" * 32, "sig": "dd" * 64, "digest": "11" * 32},
                {"pubkey": "cc" * 32, "sig": "ee" * 64, "digest": "11" * 32},
            ],
            "count": 2,
        }
        env = await agg.fetch_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
        )
        assert len(env) == 1
    asyncio.run(_with_gateway(run))


def test_fetch_envelope_drops_malformed_hex():
    async def run(gw, agg, session):
        gw.canned_get_body = {
            "kind": "settle",
            "key": "aa" * 32,
            "sigs": [
                {"pubkey": "not-hex", "sig": "dd" * 64, "digest": "11" * 32},
                {"pubkey": "cc" * 32, "sig": "ee" * 32, "digest": "11" * 32},
                {"pubkey": "cc" * 32, "sig": "ff" * 64, "digest": "11" * 32},
            ],
            "count": 3,
        }
        env = await agg.fetch_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
        )
        assert len(env) == 1
        assert env[0][0] == bytes.fromhex("cc" * 32)
        assert env[0][1] == bytes.fromhex("ff" * 64)
    asyncio.run(_with_gateway(run))


def test_fetch_envelope_returns_empty_on_http_error():
    async def run(gw, agg, session):
        gw.force_get_status = 503
        env = await agg.fetch_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
        )
        assert env == []
    asyncio.run(_with_gateway(run))


def test_assemble_envelope_adds_local_sig_then_dedupes():
    async def run(gw, agg, session):
        gw.store[("settle", "aa" * 32, "11" * 32, "01" * 32)] = "02" * 64
        my_pub = bytes.fromhex("03" * 32)
        my_sig = bytes.fromhex("04" * 64)
        env = await agg.assemble_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
            my_pubkey=my_pub, my_sig=my_sig,
        )
        assert len(env) == 2
        pubkeys = [p for p, _ in env]
        assert pubkeys == sorted(pubkeys)
        pubkey_set = {p for p, _ in env}
        assert my_pub in pubkey_set
        assert bytes.fromhex("01" * 32) in pubkey_set
    asyncio.run(_with_gateway(run))


def test_assemble_envelope_includes_self_even_if_post_fails():
    """If the gateway POST fails, local sig must still appear in the
    assembled envelope. The threshold check at the caller defers the
    submit to the next tick — correct behavior."""
    async def run(gw, agg, session):
        gw.force_post_status = 500
        my_pub = bytes.fromhex("03" * 32)
        my_sig = bytes.fromhex("04" * 64)
        env = await agg.assemble_envelope(
            session, kind=KIND_SETTLE,
            key=bytes.fromhex("aa" * 32),
            digest=bytes.fromhex("11" * 32),
            my_pubkey=my_pub, my_sig=my_sig,
        )
        assert env == [(my_pub, my_sig)]
    asyncio.run(_with_gateway(run))


def test_expire_kind_routes_independently():
    """settle and expire are namespaced — same key in both kinds doesn't collide."""
    async def run(gw, agg, session):
        await agg.share_sig(
            session, kind=KIND_SETTLE, key=b"\xaa" * 32,
            digest=b"\xbb" * 32, pubkey=b"\xcc" * 32, sig=b"\xdd" * 64,
        )
        await agg.share_sig(
            session, kind=KIND_EXPIRE, key=b"\xaa" * 32,
            digest=b"\xbb" * 32, pubkey=b"\xcc" * 32, sig=b"\xee" * 64,
        )
        assert len(gw.store) == 2
        assert gw.store[("settle", "aa" * 32, "bb" * 32, "cc" * 32)] == "dd" * 64
        assert gw.store[("expire", "aa" * 32, "bb" * 32, "cc" * 32)] == "ee" * 64
    asyncio.run(_with_gateway(run))


def test_find_my_sig():
    """Pure-Python helper — no fixture needed."""
    agg = MultisigAggregator(gateway_url="http://unused")
    env = [
        (bytes.fromhex("01" * 32), bytes.fromhex("a1" * 64)),
        (bytes.fromhex("02" * 32), bytes.fromhex("a2" * 64)),
    ]
    assert agg.find_my_sig(env, bytes.fromhex("02" * 32)) == bytes.fromhex("a2" * 64)
    assert agg.find_my_sig(env, bytes.fromhex("99" * 32)) is None
