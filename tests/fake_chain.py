"""A substrate-interface stand-in over a scripted chain, for the attest path's
dead-socket tests.

``FakeChain`` holds what the runtime decides an attest by: each signer's nonce,
the committee, each receipt's recorded attesters and certification, and spec
242's pool rule (an attest the runtime would not count is refused as 1010
"Transaction call is not expected", a spent nonce as 1010 "Transaction is
outdated"). Its live state is the best block's; ``head`` freezes that state
under a block hash a read can pin. ``FakeSI`` is one websocket onto it, whose
death the test scripts.
"""
import errno
import hashlib
import os
import threading
from types import SimpleNamespace

import websocket._socket
from substrateinterface.exceptions import SubstrateRequestException
from substrateinterface.utils.ss58 import ss58_encode
from websocket import WebSocketTimeoutException

from daemon import health_server
from daemon.cert_builder import scale_cert_encode
from daemon.cert_daemon import CertDaemon
from daemon.cert_store import CertStore
from daemon.config import DaemonConfig
from daemon.models import AttestationLevel, VerificationResult
from daemon.substrate_client import SubstrateClient

CALL_NOT_EXPECTED = {"code": 1010, "message": "Invalid Transaction", "data": "Transaction call is not expected"}
OUTDATED = {"code": 1010, "message": "Invalid Transaction", "data": "Transaction is outdated"}
RECEIPT_ID = "0x" + "ab" * 32
GENESIS = "0x" + "00" * 32
RECEIPT_FIELD = "0x" + "11" * 32
OTHER_MEMBERS = (b"\x07" * 32, b"\x08" * 32)


def canonical_cert_hash(receipt_id: str) -> bytes:
    """The hash spec 242 accepts for a ``FakeChain`` receipt: its cert at GENESIS."""
    field = bytes.fromhex(RECEIPT_FIELD[2:])
    return hashlib.sha256(scale_cert_encode(
        chain_genesis=GENESIS, receipt_id=receipt_id, content_hash=field,
        base_root_sha256=field, storage_locator_hash=field,
    )).digest()


class _TimedOutSocket:
    """A socket the kernel failed with ETIMEDOUT: its peer went silent and
    TCP keepalive's probes went unanswered (tcp(7))."""

    def gettimeout(self):
        return None

    def recv(self, _bufsize):
        raise TimeoutError(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))


def kernel_timeout() -> WebSocketTimeoutException:
    """What websocket-client raises on a read of a kernel-timed-out socket."""
    try:
        websocket._socket.recv(_TimedOutSocket(), 1)
    except WebSocketTimeoutException as error:
        return error
    raise AssertionError("websocket-client returned from a timed-out socket")


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
        self.frozen: dict[str, SimpleNamespace] = {}
        self.finalized: str | None = None

    def add_receipt(self, receipt_id: str, block: int) -> None:
        self.attesters[receipt_id] = set()
        self.blocks.setdefault(block, []).append(receipt_id)

    def apply_attest(self, signer: bytes, nonce: int, receipt_id: str, claimed: bytes | None = None) -> None:
        if nonce < self.nonces.get(signer, 0):
            raise SubstrateRequestException(OUTDATED)
        if (signer not in self.members or receipt_id not in self.attesters
                or receipt_id in self.certified
                or (claimed is not None and claimed != canonical_cert_hash(receipt_id))
                or signer in self.attesters[receipt_id]):
            raise SubstrateRequestException(CALL_NOT_EXPECTED)
        self.nonces[signer] = nonce + 1
        self.attesters[receipt_id].add(signer)
        self.included.append((signer, nonce, receipt_id))
        if len(self.attesters[receipt_id]) >= self.threshold:
            self.certified.add(receipt_id)

    def head(self) -> str:
        """A new best block's hash, with the state as it stands now frozen under it."""
        block_hash = f"0x{len(self.frozen) + 1:064x}"
        self.frozen[block_hash] = SimpleNamespace(
            members=frozenset(self.members),
            certified=frozenset(self.certified),
            attesters={receipt: frozenset(signers) for receipt, signers in self.attesters.items()},
        )
        return block_hash

    def at(self, block_hash: str | None):
        """The state at ``block_hash``; the best block's when None."""
        return self if block_hash is None else self.frozen[block_hash]

    @staticmethod
    def receipt_row(state, receipt_id: str) -> dict:
        certified = receipt_id in state.certified
        row = {name: RECEIPT_FIELD for name in (
            "content_hash", "base_root_sha256", "storage_locator_hash", "schema_hash",
            "base_manifest_hash", "safety_manifest_hash", "monitor_config_hash",
            "attestation_evidence_hash")}
        cert_hash = canonical_cert_hash(receipt_id) if certified else bytes(32)
        row.update(availability_cert_hash="0x" + cert_hash.hex(),
                   created_at_millis=0, submitter="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY")
        return row

    @staticmethod
    def attestation_row(state, receipt_id: str):
        signers = state.attesters.get(receipt_id)
        # The runtime removes the entry once the receipt is certified.
        if not signers or receipt_id in state.certified:
            return None
        return ("0x" + canonical_cert_hash(receipt_id).hex(),
                [ss58_encode(signer, 0) for signer in sorted(signers)])


class FakeSI:
    """One websocket onto a ``FakeChain``. Once ``failure`` is set, every call
    (or only those named in ``failing``) raises it, as a dead socket does. With
    ``idle_limit`` the socket dies once it has been idle that long on the
    chain's clock, as behind a proxy that drops idle websockets. A set ``best``
    keeps this node's best block at that frozen block, as a node that lags.

    A submit holds the socket until ``inclusion`` is set, as one waiting for
    inclusion does: ``holding`` is set meanwhile, and a call made then waits
    behind it and is named in ``queued``."""

    def __init__(self, ledger: FakeChain, idle_limit: float | None = None):
        self.ledger = ledger
        self.chain = "Materios Fake"
        self.websocket = SimpleNamespace(connected=True)
        self.failure: BaseException | None = None
        self.failing: set[str] | None = None
        self.lose_submit_response = False
        self.idle_limit = idle_limit
        self.last_used = ledger.clock
        self.best: str | None = None
        self.calls: list[str] = []
        self.closed = False
        self.inclusion = threading.Event()
        self.inclusion.set()
        self.holding = threading.Event()
        self.queued: list[str] = []

    def _use(self, name: str) -> None:
        self.calls.append(name)
        if self.holding.is_set():
            self.queued.append(name)
            self.inclusion.wait()
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

    def get_chain_head(self):
        self._use("get_chain_head")
        return self.best or self.ledger.head()

    def get_chain_finalised_head(self):
        self._use("get_chain_finalised_head")
        return self.ledger.finalized

    def query(self, module, storage_function, params=None, block_hash=None):
        self._use("query")
        state = self.ledger.at(block_hash or self.best)
        if storage_function == "CommitteeMembers":
            return SimpleNamespace(value=[ss58_encode(member, 0) for member in sorted(state.members)])
        receipt_id = params[0]
        if storage_function == "Receipts":
            known = receipt_id in state.attesters
            return SimpleNamespace(value=self.ledger.receipt_row(state, receipt_id) if known else None)
        if storage_function == "Attestations":
            return SimpleNamespace(value=self.ledger.attestation_row(state, receipt_id))
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
        self.holding.set()
        self.inclusion.wait()
        self.holding.clear()
        params = extrinsic.call.params
        self.ledger.apply_attest(extrinsic.signer, extrinsic.nonce, params["receipt_id"],
                                 bytes(params["claimed_hash"]))
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
            "attributes": {"receipt_id": rid, "content_hash": RECEIPT_FIELD, "submitter": ""},
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
    return client._call("rpc_request", "system_health", [], conn=client._write)["result"]


def attest_daemon(ledger: FakeChain, endpoint, tmp_path, *, last_processed_block: int = 99):
    """A ``CertDaemon`` whose real ``SubstrateClient`` dials ``endpoint``, with
    blob resolution and verification stubbed to pass, and the cert store and
    state file on disk under ``tmp_path``. Discord messages land in
    ``daemon.discord``."""
    config = DaemonConfig(checkpoint_enabled=False, cert_store_dir=str(tmp_path / "certs"),
                          state_file=str(tmp_path / "daemon-state.json"))
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
    daemon.discord = []

    async def send_discord(message, level="info"):
        daemon.discord.append((level, message))

    daemon.send_discord = send_discord
    return daemon


def receipt_scenario(tmp_path):
    """RECEIPT_ID submitted in block 100, a committee of the daemon and
    OTHER_MEMBERS, and the daemon about to process the block."""
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    ledger.members.update(OTHER_MEMBERS)
    endpoint = fake_endpoint(ledger)
    return ledger, endpoint, attest_daemon(ledger, endpoint, tmp_path)


def certify_by_others(ledger: FakeChain) -> None:
    for member in OTHER_MEMBERS:
        ledger.apply_attest(member, 0, RECEIPT_ID)


def refuse_submits(endpoint, refusal):
    """The write conn's pool refuses every submit with ``refusal``, for a
    reason its best block does not show."""
    write = endpoint.opened[0]
    write.failure = SubstrateRequestException(refusal)
    write.failing = {"submit_extrinsic"}
    return write


def fail_receipt_reads(daemon, failures):
    """``get_receipt`` raises each of ``failures`` in turn, then reads the chain."""
    read = daemon.client.get_receipt
    pending = iter(failures)
    attempts = []

    def get_receipt(receipt_id):
        attempts.append(receipt_id)
        for failure in pending:
            raise failure
        return read(receipt_id)

    daemon.client.get_receipt = get_receipt
    return attempts


def included(ledger: FakeChain) -> list[str]:
    return [receipt for _, _, receipt in ledger.included]


def signatures(endpoint) -> int:
    return sum(si.calls.count("create_signed_extrinsic") for si in endpoint.opened)


def criticals(daemon) -> list[str]:
    return [message for level, message in daemon.discord if level == "critical"]


def metric(name: str):
    return health_server._metrics.get(name, 0)
