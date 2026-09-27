import http.server
import json
import threading

import pytest

from daemon import discord


class _Hook(http.server.BaseHTTPRequestHandler):
    status = 204
    headers_out: dict = {}
    seen: list = []

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        type(self).seen.append((self.headers.get("User-Agent"), json.loads(body)))
        self.send_response(type(self).status)
        for name, value in type(self).headers_out.items():
            self.send_header(name, value)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def webhook():
    _Hook.seen = []
    _Hook.headers_out = {}
    server = http.server.HTTPServer(("127.0.0.1", 0), _Hook)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/api/webhooks/1/SECRET-TOKEN"
    server.shutdown()


def test_post_sends_json_with_a_non_default_user_agent(webhook):
    _Hook.status = 204
    discord.post_json(webhook, {"content": "hello"})
    user_agent, body = _Hook.seen[0]
    assert body == {"content": "hello"}
    assert user_agent and not user_agent.startswith("Python-urllib")


def test_a_rejected_post_raises_without_leaking_the_webhook_token(webhook):
    _Hook.status = 403
    with pytest.raises(discord.DiscordError) as err:
        discord.post_json(webhook, {"content": "x"})
    assert "403" in str(err.value)
    assert "SECRET-TOKEN" not in str(err.value)
    assert err.value.__cause__ is None and err.value.__suppress_context__


def test_a_rejection_carries_its_status_and_discords_retry_after(webhook):
    _Hook.status, _Hook.headers_out = 429, {"Retry-After": "2.5"}
    with pytest.raises(discord.DiscordError) as limited:
        discord.post_json(webhook, {"content": "x"})
    assert (limited.value.status, limited.value.retry_after) == (429, 2.5)
    _Hook.status, _Hook.headers_out = 400, {}
    with pytest.raises(discord.DiscordError) as refused:
        discord.post_json(webhook, {"content": "x"})
    assert (refused.value.status, refused.value.retry_after) == (400, None)


def test_an_unreachable_webhook_raises_without_leaking_the_token():
    with pytest.raises(discord.DiscordError) as err:
        discord.post_json("http://127.0.0.1:9/api/webhooks/1/SECRET-TOKEN", {"content": "x"}, timeout=2)
    assert "SECRET-TOKEN" not in str(err.value)


def test_a_malformed_webhook_url_raises_without_echoing_it():
    with pytest.raises(discord.DiscordError) as err:
        discord.post_json("not-a-url/SECRET-TOKEN", {"content": "x"})
    assert "SECRET-TOKEN" not in str(err.value)


def test_watchtower_pages_through_the_shared_poster(monkeypatch):
    from daemon.watchtower import Watchtower

    sent = []
    monkeypatch.setattr(discord, "post_json", lambda url, payload, **kw: sent.append((url, payload)))
    monkeypatch.setenv("BLOB_GATEWAY_URL", "http://gateway.invalid")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://hook.invalid")
    Watchtower()._send_discord("Quorum Marginal", "only 2/3 online", 0xFFAA00)
    url, payload = sent[0]
    assert url == "http://hook.invalid"
    assert payload["embeds"][0]["description"] == "only 2/3 online"


def test_watchtower_logs_a_rejected_page_instead_of_crashing(monkeypatch, caplog):
    from daemon.watchtower import Watchtower

    def reject(url, payload, **kw):
        raise discord.DiscordError("webhook answered HTTP 403")

    monkeypatch.setattr(discord, "post_json", reject)
    monkeypatch.setenv("BLOB_GATEWAY_URL", "http://gateway.invalid")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://hook.invalid")
    Watchtower()._send_discord("t", "d", 0)
    assert "HTTP 403" in caplog.text


def test_cert_daemon_pages_through_the_shared_poster(monkeypatch):
    import asyncio

    from daemon.cert_daemon import CertDaemon
    from daemon.config import DaemonConfig

    sent = []
    monkeypatch.setattr(discord, "post_json", lambda url, payload, **kw: sent.append((url, payload)))
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = DaemonConfig(discord_webhook_url="http://hook.invalid")
    asyncio.run(daemon.send_discord("Connection lost", "critical"))
    url, payload = sent[0]
    assert url == "http://hook.invalid"
    assert "Connection lost" in payload["content"]
