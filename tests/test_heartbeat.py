"""The heartbeat's finality_gap against the gateway's heartbeat contract.

materios-gateway src/routes/heartbeats.ts (origin/main 78a6544) accepts a
finality_gap that is absent or null, and otherwise only a safe integer in
[0, 0xffffffff] (lines 128-145). It rebuilds the signed string with
``String(finality_gap ?? 0)`` and ``String(finalized_block ?? 0)``
(lines 288-301).
"""

import json
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from substrateinterface import Keypair

from daemon.heartbeat import HeartbeatSender

U32_MAX = 0xFFFF_FFFF
ABSENT = object()
ALICE = Keypair.create_from_uri("//Alice")


def _send(tmp_path, **metrics) -> tuple[dict, str]:
    """One heartbeat from these metrics, as the gateway receives it."""
    snapshot = {
        "last_processed_block": 120,
        "finalized_head": 118,
        "finality_gap": 2,
        "pending_receipts": 3,
        "certs_submitted_total": 7,
        "substrate_connected": True,
    }
    snapshot.update(metrics)
    config = SimpleNamespace(
        data_dir=str(tmp_path),
        heartbeat_url="https://gateway.test",
        blob_gateway_api_key="",
        heartbeat_interval=30,
    )
    sender = HeartbeatSender(config, snapshot, threading.Lock(), ALICE)
    with patch(
        "daemon.heartbeat.requests.post", return_value=MagicMock(status_code=200)
    ) as post:
        sender._send_heartbeat()
    post.assert_called_once()
    wire = json.loads(json.dumps(post.call_args.kwargs["json"], allow_nan=False))
    return wire, post.call_args.kwargs["headers"]["x-heartbeat-sig"]


def _gateway_rejection(payload: dict, signature: str) -> str | None:
    """The gateway's verdict on the fields this test varies, or None if it
    accepts. Stricter than the gateway on one point: an integral float is
    refused here, though only its signed form would refuse it there."""
    for field in ("finalized_block", "finality_gap"):
        value = payload.get(field)
        if value is not None and (type(value) is not int or not 0 <= value <= U32_MAX):
            return f"Invalid {field}: must be an integer in [0, {U32_MAX}]"

    def or_zero(field):
        value = payload.get(field)
        return "0" if value is None else str(value)

    signed = "|".join([
        "materios-heartbeat-v1",
        payload["validator_id"],
        str(payload["seq"]),
        str(payload["timestamp"]),
        str(payload["best_block"]),
        or_zero("finalized_block"),
        or_zero("finality_gap"),
        str(payload["pending_receipts"]),
        str(payload["certs_submitted"]),
        "1" if payload["substrate_connected"] else "0",
        payload["version"],
        str(payload["uptime_seconds"]),
    ])
    signer = Keypair(ss58_address=payload["validator_id"])
    if not signer.verify(signed.encode("utf-8"), bytes.fromhex(signature.removeprefix("0x"))):
        return "Invalid signature"
    return None


@pytest.mark.parametrize(
    ("gap", "sent"),
    [
        (2, 2),
        (0, 0),
        (-3, 0),
        (2.75, 2),
        (5.0, 5),
        ("7", 7),
        (float("nan"), ABSENT),
        (float("inf"), ABSENT),
        ("n/a", ABSENT),
        (None, ABSENT),
        (U32_MAX, U32_MAX),
        (U32_MAX + 1, U32_MAX),
        (10**30, U32_MAX),
    ],
    ids=[
        "in-range", "zero", "negative", "fractional", "integral-float",
        "numeric-text", "nan", "infinity", "text", "none", "u32-max",
        "u32-max-plus-one", "far-over-range",
    ],
)
def test_finality_gap_is_sent_as_the_gateway_accepts_it(tmp_path, gap, sent) -> None:
    payload, signature = _send(tmp_path, finality_gap=gap)
    assert payload.get("finality_gap", ABSENT) == sent
    assert _gateway_rejection(payload, signature) is None


@pytest.mark.parametrize("head", ["last_processed_block", "finalized_head"])
def test_finality_gap_is_omitted_when_a_head_is_unknown(tmp_path, head) -> None:
    payload, _ = _send(tmp_path, **{head: None, "finality_gap": 4})
    assert "finality_gap" not in payload


def test_beat_with_an_unknown_finalized_head_is_accepted(tmp_path) -> None:
    payload, signature = _send(tmp_path, finalized_head=None, finality_gap=4)
    assert payload["finalized_block"] is None
    assert _gateway_rejection(payload, signature) is None
