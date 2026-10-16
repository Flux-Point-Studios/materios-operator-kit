"""The preprod stake pool's block watcher: one check per timer run, paging each new block."""

import http.server
import json
import os
import threading
from pathlib import Path

import pytest

from ops import cardano_bp_block_watcher as bw

POOL = f"/pools/{bw.POOL_ID}"
NEWEST = f"{POOL}/blocks?count=1&order=desc"
BLOCK = "9f" * 32


class Chain:
    """Insight's answers for the pool, and every page sent."""

    def __init__(self, minted: int):
        self.routes = {POOL: {"blocks_minted": minted}, NEWEST: [BLOCK],
                       f"/blocks/{BLOCK}": {"height": 4_900_001, "epoch": 320, "slot": 130_000_000}}
        self.pages = []

    def read(self, path):
        if path not in self.routes:
            raise OSError(f"no route {path}")
        return self.routes[path]

    def page(self, content):
        self.pages.append(content)


@pytest.fixture
def state(tmp_path):
    return tmp_path / "last-block-count"


def test_the_first_run_stores_the_count_and_pages_nothing(state):
    chain = Chain(minted=3)
    assert bw.check(chain.read, chain.page, state) == "first run: stored blocks_minted=3"
    assert state.read_text() == "3\n" and chain.pages == []


def test_a_block_the_pool_forged_is_paged_with_where_it_landed(state):
    state.write_text("3\n")
    chain = Chain(minted=4)
    assert bw.check(chain.read, chain.page, state) == f"paged block {BLOCK}"
    [page] = chain.pages
    assert "3 -> 4 (+1)" in page and f"`{BLOCK}`" in page
    assert "Height 4900001 | epoch 320 | slot 130000000" in page
    assert state.read_text() == "4\n"


def test_an_unchanged_count_pages_nothing(state):
    state.write_text("4\n")
    chain = Chain(minted=4)
    assert bw.check(chain.read, chain.page, state) == "no change: blocks_minted=4"
    assert chain.pages == []


def test_a_block_whose_details_cannot_be_read_is_paged_without_them(state):
    state.write_text("3\n")
    chain = Chain(minted=5)
    del chain.routes[f"/blocks/{BLOCK}"]
    bw.check(chain.read, chain.page, state)
    [page] = chain.pages
    assert "3 -> 5 (+2)" in page and "Height ? | epoch ? | slot ?" in page
    assert state.read_text() == "5\n"


def test_a_page_discord_refuses_is_sent_again_on_the_next_run(state):
    state.write_text("3\n")
    chain = Chain(minted=4)

    def refuse(content):
        raise OSError("HTTP Error 403: Forbidden")

    with pytest.raises(OSError):
        bw.check(chain.read, refuse, state)
    assert state.read_text() == "3\n"
    bw.check(chain.read, chain.page, state)
    assert len(chain.pages) == 1


class _Endpoint(http.server.BaseHTTPRequestHandler):
    """Insight's pool routes on GET, the Discord webhook on POST."""

    seen = []
    pages = []
    status = 200

    def do_GET(self):
        type(self).seen.append((self.path, self.headers.get("project_id"), self.headers.get("User-Agent")))
        answers = {f"/api/v0{POOL}": {"blocks_minted": 7}}
        if self.path in answers:
            self._answer(type(self).status, answers[self.path])
        else:
            self._answer(404, {"status_code": 404})

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        type(self).pages.append((self.path, self.headers.get("User-Agent"), json.loads(body)["content"]))
        self._answer(204, None)

    def _answer(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if body is not None:
            self.wfile.write(json.dumps(body).encode())

    def log_message(self, *args):
        pass


@pytest.fixture
def endpoint(tmp_path, monkeypatch):
    _Endpoint.seen, _Endpoint.pages, _Endpoint.status = [], [], 200
    server = http.server.HTTPServer(("127.0.0.1", 0), _Endpoint)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    root = f"http://127.0.0.1:{server.server_port}"
    credentials = tmp_path / "credentials"
    credentials.mkdir()
    (credentials / "insight.key").write_text("SECRETKEY\n")
    (credentials / "discord-webhook").write_text(f"{root}/api/webhooks/1/token\n")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(credentials))
    monkeypatch.setattr(bw, "INSIGHT", f"{root}/api/v0")
    monkeypatch.setattr(bw, "STATE_FILE", tmp_path / "last-block-count")
    yield
    server.shutdown()


def test_the_insight_key_and_webhook_come_from_the_units_credentials(endpoint, capsys):
    assert bw.main() == 0
    path, key, agent = _Endpoint.seen[0]
    assert path == f"/api/v0{POOL}" and key == "SECRETKEY"
    assert agent and not agent.startswith("Python-urllib")
    assert "first run: stored blocks_minted=7" in capsys.readouterr().err


def test_a_page_goes_to_the_webhook_with_a_named_agent(endpoint):
    bw.STATE_FILE.write_text("6\n")
    assert bw.main() == 0
    [(path, agent, content)] = _Endpoint.pages
    assert path == "/api/webhooks/1/token" and "6 -> 7 (+1)" in content
    assert agent and not agent.startswith("Python-urllib")


def test_a_check_insight_refuses_fails_the_run_without_the_key_in_its_output(endpoint, capsys):
    _Endpoint.status = 403
    assert bw.main() == 1
    err = capsys.readouterr().err
    assert "403" in err and "SECRETKEY" not in err
    assert not bw.STATE_FILE.exists()


def test_a_malformed_webhook_never_reaches_the_journal(endpoint, capsys):
    # urllib names the URL it cannot open in its error, and the journal is readable by adm.
    bw.STATE_FILE.write_text("6\n")
    (Path(os.environ["CREDENTIALS_DIRECTORY"]) / "discord-webhook").write_text("discord.test/api/webhooks/1/TOKEN")
    assert bw.main() == 1
    err = capsys.readouterr().err
    assert "check failed: ValueError" in err and "TOKEN" not in err
