"""Tests for cert-daemon's Ogmios client (task-185).

Coverage:
  - get_cardano_epoch graceful failure on NXDOMAIN, refused, timeout, non-200,
    malformed JSON, and missing currentEpoch.
  - State-transition WARNING rate limit: warn once per reachable->unreachable
    transition, recover with INFO on unreachable->reachable.
  - OGMIOS_URL env override flows through to DaemonConfig.from_env().
  - probe_ogmios() at startup correctly identifies reachable vs unreachable.
  - LIVE integration probe against the LAN preprod Ogmios at 192.168.0.133:1337
    (skipped if not reachable from the test host — keeps CI green when run off
    the operator LAN).

Mocking style follows the rest of the suite: requests is patched at the
import seam in daemon.cert_daemon; no live HTTP unless the live-integration
test is enabled.
"""

from __future__ import annotations

import logging
import os
import socket
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig
from daemon.main import probe_ogmios


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_daemon(ogmios_url: str = "http://test-ogmios:1337") -> CertDaemon:
    """Build a CertDaemon stub with just enough state to call get_cardano_epoch."""
    config = DaemonConfig()
    config.ogmios_url = ogmios_url
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = config
    daemon._ogmios_reachable = None  # match __init__'s initial state
    return daemon


# ---------------------------------------------------------------------------
# Default value — task-185 fix
# ---------------------------------------------------------------------------

def test_default_ogmios_url_is_not_stale_k8s_dns():
    """Regression: task-185 — the broken k8s DNS default is gone."""
    cfg = DaemonConfig()
    assert "svc.cluster.local" not in cfg.ogmios_url, (
        "ogmios_url default still references k8s in-cluster DNS — "
        "task-185 was supposed to remove that. NXDOMAIN every poll cycle."
    )
    assert cfg.ogmios_url.startswith(("http://", "https://"))


def test_env_override_takes_precedence():
    """OGMIOS_URL env var must win over the default."""
    custom = "http://1.2.3.4:9999"
    with patch.dict(os.environ, {"OGMIOS_URL": custom}, clear=False):
        cfg = DaemonConfig.from_env()
    assert cfg.ogmios_url == custom


def test_env_override_unset_yields_default():
    env = {k: v for k, v in os.environ.items() if k != "OGMIOS_URL"}
    with patch.dict(os.environ, env, clear=True):
        cfg = DaemonConfig.from_env()
    assert cfg.ogmios_url == DaemonConfig.ogmios_url


# ---------------------------------------------------------------------------
# get_cardano_epoch — graceful failure modes
# ---------------------------------------------------------------------------

def test_get_cardano_epoch_returns_zero_on_dns_failure(caplog):
    daemon = _make_daemon()
    fake_requests = MagicMock()
    fake_requests.get.side_effect = ConnectionError("Name or service not known")
    with patch.dict("sys.modules", {"requests": fake_requests}):
        with caplog.at_level(logging.WARNING, logger="daemon.cert_daemon"):
            result = daemon.get_cardano_epoch()
    assert result == 0
    assert daemon._ogmios_reachable is False
    assert any("Failed to get Cardano epoch from Ogmios" in r.message for r in caplog.records)


def test_get_cardano_epoch_returns_zero_on_connection_refused():
    daemon = _make_daemon()
    fake_requests = MagicMock()
    fake_requests.get.side_effect = ConnectionRefusedError("Connection refused")
    with patch.dict("sys.modules", {"requests": fake_requests}):
        result = daemon.get_cardano_epoch()
    assert result == 0
    assert daemon._ogmios_reachable is False


def test_get_cardano_epoch_returns_zero_on_timeout():
    daemon = _make_daemon()
    fake_requests = MagicMock()
    fake_requests.get.side_effect = TimeoutError("timed out")
    with patch.dict("sys.modules", {"requests": fake_requests}):
        result = daemon.get_cardano_epoch()
    assert result == 0
    assert daemon._ogmios_reachable is False


def test_get_cardano_epoch_returns_zero_on_malformed_json():
    daemon = _make_daemon()
    fake_resp = MagicMock()
    fake_resp.json.side_effect = ValueError("bad json")
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.dict("sys.modules", {"requests": fake_requests}):
        result = daemon.get_cardano_epoch()
    assert result == 0
    assert daemon._ogmios_reachable is False


def test_get_cardano_epoch_returns_epoch_when_present():
    daemon = _make_daemon()
    fake_resp = MagicMock()
    fake_resp.json.return_value = {"currentEpoch": 285, "network": "preprod"}
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.dict("sys.modules", {"requests": fake_requests}):
        result = daemon.get_cardano_epoch()
    assert result == 285
    assert daemon._ogmios_reachable is True


def test_get_cardano_epoch_falls_back_to_slot_division():
    daemon = _make_daemon()
    fake_resp = MagicMock()
    # currentEpoch absent — use slot/432000 (preprod epoch length)
    fake_resp.json.return_value = {"lastKnownTip": {"slot": 432000 * 285 + 1234}}
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.dict("sys.modules", {"requests": fake_requests}):
        result = daemon.get_cardano_epoch()
    assert result == 285
    assert daemon._ogmios_reachable is True


# ---------------------------------------------------------------------------
# State-transition WARNING rate limit
# ---------------------------------------------------------------------------

def test_warning_logged_only_on_first_failure(caplog):
    """task-185 core fix: stop spamming WARNING per poll cycle."""
    daemon = _make_daemon()
    fake_requests = MagicMock()
    fake_requests.get.side_effect = ConnectionError("dns fail")

    with patch.dict("sys.modules", {"requests": fake_requests}):
        with caplog.at_level(logging.WARNING, logger="daemon.cert_daemon"):
            for _ in range(5):
                daemon.get_cardano_epoch()

    failure_warnings = [
        r for r in caplog.records
        if "Failed to get Cardano epoch from Ogmios" in r.message
    ]
    assert len(failure_warnings) == 1, (
        f"expected exactly 1 warning across 5 failed polls, got {len(failure_warnings)}. "
        f"This is the bug task-185 fixes — 1 warning per cycle floods logs."
    )


def test_recovery_logged_at_info_after_failure(caplog):
    daemon = _make_daemon()
    daemon._ogmios_reachable = False  # simulate prior failure

    fake_resp = MagicMock()
    fake_resp.json.return_value = {"currentEpoch": 285}
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp

    with patch.dict("sys.modules", {"requests": fake_requests}):
        with caplog.at_level(logging.INFO, logger="daemon.cert_daemon"):
            result = daemon.get_cardano_epoch()

    assert result == 285
    assert daemon._ogmios_reachable is True
    recoveries = [r for r in caplog.records if "Ogmios reachable again" in r.message]
    assert len(recoveries) == 1


def test_no_warning_or_info_on_repeated_success(caplog):
    daemon = _make_daemon()
    fake_resp = MagicMock()
    fake_resp.json.return_value = {"currentEpoch": 285}
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp

    with patch.dict("sys.modules", {"requests": fake_requests}):
        with caplog.at_level(logging.INFO, logger="daemon.cert_daemon"):
            for _ in range(5):
                daemon.get_cardano_epoch()

    # No "reachable again" because never went unreachable.
    recoveries = [r for r in caplog.records if "reachable again" in r.message]
    assert len(recoveries) == 0


# ---------------------------------------------------------------------------
# probe_ogmios() startup probe
# ---------------------------------------------------------------------------

def test_probe_ogmios_unreachable_dns():
    # NXDOMAIN on a definitely-not-real host
    ok, detail = probe_ogmios("http://does-not-exist-task-185.invalid:1337", timeout=2.0)
    assert ok is False
    assert detail  # non-empty error description


def test_probe_ogmios_unreachable_refused():
    # Localhost on an unused port — connection refused
    ok, detail = probe_ogmios("http://127.0.0.1:1", timeout=2.0)
    assert ok is False
    assert detail


def test_probe_ogmios_strips_trailing_slash():
    """Defensive: OGMIOS_URL with trailing slash shouldn't double-slash /health."""
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {
        "currentEpoch": 285, "network": "preprod", "networkSynchronization": 1.0,
    }
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp

    with patch.dict("sys.modules", {"requests": fake_requests}):
        ok, detail = probe_ogmios("http://test:1337/", timeout=2.0)
    assert ok is True
    assert "epoch=285" in detail
    # called URL must be http://test:1337/health (no double slash)
    called_url = fake_requests.get.call_args[0][0]
    assert called_url == "http://test:1337/health"


def test_probe_ogmios_reports_non_200():
    fake_resp = MagicMock()
    fake_resp.status_code = 502
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp

    with patch.dict("sys.modules", {"requests": fake_requests}):
        ok, detail = probe_ogmios("http://test:1337", timeout=2.0)
    assert ok is False
    assert "502" in detail


# ---------------------------------------------------------------------------
# LIVE integration — preprod LAN Ogmios on Node-3
# ---------------------------------------------------------------------------

def _lan_ogmios_reachable() -> bool:
    """Quick sanity check so the live test self-skips off-LAN."""
    s = socket.socket()
    s.settimeout(1.5)
    try:
        s.connect(("192.168.0.133", 1337))
        return True
    except Exception:
        return False
    finally:
        s.close()


@pytest.mark.skipif(
    not _lan_ogmios_reachable(),
    reason="LAN Ogmios at 192.168.0.133:1337 not reachable — skipping live probe",
)
def test_live_probe_against_lan_ogmios():
    """REAL AF: hit the actual preprod Ogmios and confirm a real epoch."""
    ok, detail = probe_ogmios("http://192.168.0.133:1337", timeout=5.0)
    assert ok is True, f"LAN Ogmios probe failed: {detail}"
    assert "epoch=" in detail
    assert "preprod" in detail


@pytest.mark.skipif(
    not _lan_ogmios_reachable(),
    reason="LAN Ogmios at 192.168.0.133:1337 not reachable — skipping live probe",
)
def test_live_get_cardano_epoch_against_lan_ogmios():
    """REAL AF: call get_cardano_epoch through the real client path."""
    daemon = _make_daemon(ogmios_url="http://192.168.0.133:1337")
    epoch = daemon.get_cardano_epoch()
    assert epoch > 280, f"epoch looks bogus: {epoch}"
    assert epoch < 1000, f"epoch looks bogus: {epoch}"
    assert daemon._ogmios_reachable is True
