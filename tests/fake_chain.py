"""A substrate-interface stand-in over a scripted chain, for the attest path's
dead-socket tests.

``FakeChain`` holds what the runtime decides an attest by: each signer's nonce,
each receipt's recorded attesters and certification, and spec 242's pool rule
(an attest the runtime would not count is refused as 1010 "Transaction call is
not expected", a spent nonce as 1010 "Transaction is outdated"). ``FakeSI`` is
one websocket onto it, whose death the test scripts.
"""
from types import SimpleNamespace

from substrateinterface.exceptions import SubstrateRequestException
from substrateinterface.utils.ss58 import ss58_encode

from daemon.cert_daemon import CertDaemon
from daemon.cert_store import CertStore
from daemon.config import DaemonConfig
from daemon.models import AttestationLevel, VerificationResult
from daemon.substrate_client import SubstrateClient

CALL_NOT_EXPECTED = {"code": 1010, "message": "Invalid Transaction", "data": "Transaction call is not expected"}
OUTDATED = {"code": 1010, "message": "Invalid Transaction", "data": "Transaction is outdated"}
RECEIPT_ID = "0x" + "ab" * 32
GENESIS = "0x" + "00" * 32


class FakeChain:
    def __init__(self, threshold: int = 2):
        self.threshold = threshold
        self.members: set[bytes] = set()
        self.nonces: dict[bytes, int] = {}
        self.attesters: dict[str, set[bytes]] = {}
        self.certified: set[str] = set()
        self.blocks: dict[int, list[str]] = {}
        self.included: list[tuple[bytes, int, str]] = []
        self.clock = 0.0

    def add_receipt(self, receipt_id: str, block: int) -> None:
        self.attesters[receipt_id] = set()
        self.blocks.setdefault(block, []).append(receipt_id)

    def apply_attest(self, signer: bytes, nonce: int, receipt_id: str) -> None:
        if nonce < self.nonces.get(signer, 0):
            raise SubstrateRequestException(OUTDATED)
        if (signer not in self.members or receipt_id in self.certified
                or signer in self.attesters[receipt_id]):
            raise SubstrateRequestException(CALL_NOT_EXPECTED)
        self.nonces[signer] = nonce + 1
        self.attesters[receipt_id].add(signer)
        self.included.append((signer, nonce, receipt_id))
        if len(self.attesters[receipt_id]) >= self.threshold:
            self.certified.add(receipt_id)

    def receipt_row(self, receipt_id: str) -> dict:
        certified = receipt_id in self.certified
        row = {name: "0x" + "11" * 32 for name in (
            "content_hash", "base_root_sha256", "storage_locator_hash", "schema_hash",
            "base_manifest_hash", "safety_manifest_hash", "monitor_config_hash",
            "attestation_evidence_hash")}
        row.update(availability_cert_hash="0x" + ("cc" if certified else "00") * 32,
                   created_at_millis=0, submitter="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY")
        return row

    def attestation_row(self, receipt_id: str):
        signers = self.attesters.get(receipt_id)
        # The runtime removes the entry once the receipt is certified.
        if not signers or receipt_id in self.certified:
            return None
        return ("0x" + "cc" * 32, [ss58_encode(signer, 0) for signer in sorted(signers)])


class FakeSI:
    """One websocket onto a ``FakeChain``. Once ``failure`` is set, every call
    (or only those named in ``failing``) raises it, as a dead socket does. With
    ``idle_limit`` the socket dies once it has been idle that long on the
    chain's clock, as behind a proxy that drops idle websockets."""

    def __init__(self, ledger: FakeChain, idle_limit: float | None = None):
        self.ledger = ledger
        self.chain = "Materios Fake"
        self.websocket = SimpleNamespace(connected=True)
        self.failure: BaseException | None = None
        self.failing: set[str] | None = None
        self.lose_submit_response = False
        self.idle_limit = idle_limit
        self.last_used = ledger.clock
        self.calls: list[str] = []
        self.closed = False

    def _use(self, name: str) -> None:
        self.calls.append(name)
        if self.idle_limit is not None and self.ledger.clock - self.last_used > self.idle_limit:
            self.failure = self.failure or BrokenPipeError(32, "Broken pipe")
        if self.failure is not None and (self.failing is None or name in self.failing):
            raise self.failure
        self.last_used = self.ledger.clock

    def close(self) -> None:
        self.closed = True

    def rpc_request(self, method, params):
        self._use(method)
        return {"jsonrpc": "2.0", "id": 1, "result": {"peers": 3, "isSyncing": False}}

    def query(self, module, storage_function, params=None, block_hash=None):
        self._use("query")
        receipt_id = params[0]
        if storage_function == "Receipts":
            known = receipt_id in self.ledger.attesters
            return SimpleNamespace(value=self.ledger.receipt_row(receipt_id) if known else None)
        if storage_function == "Attestations":
            return SimpleNamespace(value=self.ledger.attestation_row(receipt_id))
        raise AssertionError(f"unexpected storage read {module}.{storage_function}")

    def compose_call(self, call_module, call_function, call_params):
        self._use("compose_call")
        return SimpleNamespace(function=call_function, params=call_params)

    def create_signed_extrinsic(self, call, keypair):
        self._use("create_signed_extrinsic")
        # substrate-interface 1.8 signs with AccountNonceApi at the best block:
        # the chain's nonce, blind to the pool.
        return SimpleNamespace(call=call, signer=keypair.public_key,
                               nonce=self.ledger.nonces.get(keypair.public_key, 0))

    def submit_extrinsic(self, extrinsic, wait_for_inclusion=False):
        self._use("submit_extrinsic")
        self.ledger.apply_attest(extrinsic.signer, extrinsic.nonce, extrinsic.call.params["receipt_id"])
        if self.lose_submit_response:
            self.failure = ConnectionResetError(104, "Connection reset by peer")
            raise self.failure
        return SimpleNamespace(is_success=True, error_message=None, block_hash="0x01", triggered_events=[])

    def get_block_hash(self, block_number):
        self._use("get_block_hash")
        return f"0x{block_number:064x}"

    def get_events(self, block_hash):
        self._use("get_events")
        return [SimpleNamespace(value={
            "module_id": "OrinqReceipts", "event_id": "ReceiptSubmitted",
            "attributes": {"receipt_id": rid, "content_hash": "0x" + "11" * 32, "submitter": ""},
        }) for rid in self.ledger.blocks.get(int(block_hash, 16), [])]


def fake_endpoint(ledger: FakeChain, idle_limit: float | None = None):
    """A ``_create_substrate_interface`` stand-in: every connection it opens
    is appended to ``opened``; while ``up`` is False it refuses to connect, and
    a set ``failure`` makes each new connection dead on arrival."""
    def connect():
        if not connect.up:
            raise ConnectionRefusedError(111, "Connection refused")
        si = FakeSI(ledger, idle_limit)
        si.failure = connect.failure
        connect.opened.append(si)
        return si
    connect.up = True
    connect.failure = None
    connect.opened = []
    return connect


def dialling_client(endpoint, config: DaemonConfig | None = None) -> SubstrateClient:
    """A connected ``SubstrateClient`` whose conns ``endpoint`` opens: write
    first, then read. Reconnect backoff is instant and capped at 2 attempts."""
    client = SubstrateClient(config or DaemonConfig(rpc_url="ws://test-rpc:9944"))
    client._create_substrate_interface = endpoint
    client._backoff_sleep = lambda _seconds: None
    client._reconnect_max_attempts = 2
    assert client.connect()
    return client


def connected_client(ledger=None, **endpoint_kwargs):
    endpoint = fake_endpoint(ledger or FakeChain(), **endpoint_kwargs)
    return dialling_client(endpoint), endpoint


def health(client):
    return client._call("rpc_request", "system_health", [])["result"]


def attest_daemon(ledger: FakeChain, endpoint, tmp_path, *, last_processed_block: int = 99):
    """A ``CertDaemon`` whose real ``SubstrateClient`` dials ``endpoint``, with
    blob resolution and verification stubbed to pass and the cert store on
    disk. Discord messages land in ``daemon.discord``."""
    config = DaemonConfig(checkpoint_enabled=False, cert_store_dir=str(tmp_path / "certs"))
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = config
    daemon.client = dialling_client(endpoint, config)
    ledger.members.add(daemon.client.keypair.public_key)

    class Locator:
        async def resolve(self, receipt_id, content_hash=None):
            return SimpleNamespace(receipt_id=receipt_id, chunks=[])

    class Verifier:
        async def verify(self, receipt, manifest):
            return VerificationResult(attestation_level=AttestationLevel.ROOT_VERIFIED)

    daemon.locator = Locator()
    daemon.verifier = Verifier()
    daemon.content_validator = None
    daemon.cert_store = CertStore(config)
    daemon.checkpointer = None
    daemon.pending = {}
    daemon.last_processed_block = last_processed_block
    daemon._running = True
    daemon._notified_ids = {}
    daemon._pruned_warned_blocks = set()
    daemon._live_chain_genesis = GENESIS
    daemon._chain_write_lock = None
    daemon._pending_lock = None
    daemon._concurrency_sem = None
    daemon.save_state = lambda: None
    daemon.discord = []

    async def send_discord(message, level="info"):
        daemon.discord.append((level, message))

    daemon.send_discord = send_discord
    return daemon
