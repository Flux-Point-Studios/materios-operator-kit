import collections
import errno
import functools
import hashlib
import json
import logging
import os
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.base import ExtrinsicReceipt, QueryMapResult
from substrateinterface.exceptions import SubstrateRequestException
from substrateinterface.utils.ss58 import ss58_decode
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException

from daemon import health_server
from daemon.config import DaemonConfig
from daemon.models import ReceiptRecord
from daemon.voucher_canonicalize import (
    AddressDecodeError,
    ChainIdentity,
    compute_voucher_digest_with_address,
    split_type0_address_bytes,
)

logger = logging.getLogger(__name__)


# Slash signing contract fingerprint -> committee digest protocol. Derivation
# and provenance: docs/slash-watcher-frv2.md. Never add a fallback protocol.
SLASH_PROTOCOLS = {
    "96dfe9b41a5471b578e065d9e3055a359e27e9a6576f94b1f932bd668219e85c": "FRAU",
    "693ba3e8bc9106fb036f96bc73b3c7797d314c36ef95c0564c715ce58113eeb7": "FRV2",
}
SUPERSEDE_ARGS = ["claim_id", "settled_direct", "evidence", "signatures"]
# settlement_slash_protocol metric: 0 means the runtime is refused.
SLASH_PROTOCOL_GAUGE = {"FRAU": 1, "FRV2": 2}
RUNTIME_CODE_KEY = "0x" + b":code".hex()


# frame_system's genesis stores this as BlockHash(0); block 1 overwrites it
# with the real genesis hash.
BLOCK_ZERO_PLACEHOLDER = b"\x45" * 32

# How often a call waiting for an SI's lock checks whether that SI was
# replaced. Closing a socket does not always wake a worker blocked reading it
# (websocket-client skips the shutdown once it has marked the socket closed),
# so a waiting call cannot count on that worker leaving.
REPLACED_CHECK_SECS = 0.05


class ChainIdRefused(Exception):
    """The Materios chain id a voucher digest would bind is unusable."""


def select_slash_protocol(fingerprint: str, tag: Optional[bytes]) -> str:
    """The runtime's ``SlashSettlementTag`` names the protocol and the contract
    must be that protocol's pin; a runtime without the tag gets FRAU only on
    the FRAU pin."""
    if tag is None:
        expected = "FRAU"
    elif tag == b"FRV2":
        expected = "FRV2"
    else:
        raise ValueError(f"unknown SlashSettlementTag 0x{tag.hex()}")
    if SLASH_PROTOCOLS.get(fingerprint) != expected:
        raise ValueError(f"slash contract {fingerprint} is not the {expected} pin")
    return expected


def supersede_offered(v14: dict, attest_tag: Optional[bytes]) -> bool:
    """Whether the runtime has the reviewed ``supersede_settlement_request``:
    these arguments, verified against the STCA digest. Any other shape refuses."""
    pallet = next(p for p in v14["pallets"] if p["name"] == "IntentSettlement")
    calls = next(e["type"]["def"] for e in v14["types"]["types"] if e["id"] == pallet["calls"]["ty"])
    call = next((c for c in calls["variant"]["variants"] if c["name"] == "supersede_settlement_request"), None)
    if call is None:
        return False
    if [field["name"] for field in call["fields"]] != SUPERSEDE_ARGS:
        raise ValueError("supersede_settlement_request arguments are not the reviewed ones")
    if attest_tag != b"STCA":
        shown = "absent" if attest_tag is None else f"0x{attest_tag.hex()}"
        raise ValueError(f"supersede_settlement_request with AttestSettleTag {shown}, not STCA")
    return True


def settlement_contract(metadata: Any) -> dict:
    """What a runtime's decoded metadata makes the daemons sign: the slash
    protocol and the contract fingerprint behind it, whether supersede is
    offered, the TTL and MinFinalityDepth. Raises on any runtime outside the
    reviewed contracts."""
    v14 = metadata.value[1]["V14"]
    constants = {c.name: bytes(c.constant_value)
                 for c in metadata.get_metadata_pallet("IntentSettlement").constants}
    fingerprint = slash_contract_fingerprint(v14)
    return dict(
        protocol=select_slash_protocol(fingerprint, constants.get("SlashSettlementTag")),
        fingerprint=fingerprint,
        supersede=supersede_offered(v14, constants.get("AttestSettleTag")),
        ttl=_u32_constant(constants, "SettlementRequestTtl"),
        min_finality_depth=_u32_constant(constants, "MinFinalityDepth"),
    )


def _u32_constant(constants: dict, name: str) -> int:
    raw = constants[name]
    if len(raw) != 4:
        raise ValueError(f"{name} is not a u32")
    return int.from_bytes(raw, "little")


def slash_contract_fingerprint(v14: dict) -> str:
    """SHA-256 of the IntentSettlement items that define the slash signature.

    Types are resolved structurally, so docs, Rust paths, type names and
    registry ids never contribute.
    """
    types = {entry["id"]: entry["type"]["def"] for entry in v14["types"]["types"]}

    def resolve(type_id, seen=()):
        if type_id in seen:
            raise ValueError("recursive type in slash contract")
        seen += (type_id,)
        (kind, body), = types[type_id].items()
        if kind == "composite":
            return {kind: fields(body["fields"], seen)}
        if kind == "variant":
            variants = sorted(body["variants"], key=lambda v: v["index"])
            return {kind: [[v["index"], v["name"], fields(v["fields"], seen)] for v in variants]}
        if kind in ("sequence", "compact"):
            return {kind: resolve(body["type"], seen)}
        if kind == "array":
            return {kind: [body["len"], resolve(body["type"], seen)]}
        if kind == "tuple":
            return {kind: [resolve(item, seen) for item in body]}
        if kind == "primitive":
            return {kind: body}
        raise ValueError(f"unsupported {kind} type in slash contract")

    def fields(entries, seen=()):
        return [[field["name"], resolve(field["type"], seen)] for field in entries]

    def map_types(entry):
        layout = entry["type"]["Map"]
        return [resolve(layout["key"]), resolve(layout["value"])]

    pallet = next(p for p in v14["pallets"] if p["name"] == "IntentSettlement")
    calls = types[pallet["calls"]["ty"]]["variant"]["variants"]
    call = next(c for c in calls if c["name"] == "slash_bad_settlement_evidence")
    storage = {entry["name"]: entry for entry in pallet["storage"]["entries"]}
    ttl = next(c for c in pallet["constants"] if c["name"] == "SettlementRequestTtl")
    request_slashes = storage.get("SlashedSettlementRequests")
    contract = {
        "call": [call["index"], call["name"], fields(call["fields"])],
        "requests": map_types(storage["ClaimSettlementRequests"]),
        "request_slashes": None if request_slashes is None else map_types(request_slashes),
        "ttl": [ttl["name"], resolve(ttl["type"])],
    }
    return hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Bounded-timeout + auto-reconnect wrapper for substrate-interface
# (tasks #288 + #156).
#
# Why this exists: substrate-interface 1.x is built on the blocking
# ``websocket-client`` library. After multi-hour uptime (correlates with
# multi-minute extrinsic phases where the WS sits idle), the underlying
# socket enters a state where ``recv()`` blocks past any timeout passed to
# it. No exception fires. The Python call sits indefinitely. Watchdog
# evidence from 2026-05-21 shows ~5 outer-container restarts in 9 hours
# triggered by this wedge.
#
# The wrapper enforces three contracts:
#   1. Every RPC method call runs inside a ``ThreadPoolExecutor`` worker
#      thread; the calling thread sees ``RPCTimeoutError`` if the call
#      exceeds ``MATERIOS_RPC_TIMEOUT_SECS`` (default 30s). The worker
#      thread is daemonised — if the socket recv never returns, the
#      thread leaks at most one Python thread per timeout, which is
#      cheap (the OS reaps the underlying fd when the new SI replaces it).
#   2. After ``MATERIOS_RPC_RECONNECT_AFTER_TIMEOUTS`` consecutive
#      timeouts (default 2) the wrapper closes the current SI's websocket
#      and constructs a new SI. A successful call resets the streak.
#   3. Reconnect failures retry with exponential backoff: 1s, 2s, 4s, 8s,
#      16s, then capped at 30s. The streak gauge surfaces to /metrics.
#   4. A call that fails on a dead socket (``is_transport_dead``) replaces
#      that conn's SI at once and is retried once on the fresh one. A write
#      conn left without an SI reconnects on its next call; a read conn left
#      without one sends reads to the write conn and is reopened in the
#      background. A timeout on an SI the conn has already replaced does not
#      count toward the new SI's streak.
#   5. One call at a time runs on each SI (#72). substrate-interface's
#      websocket transport reads every reply off one socket into one shared
#      queue, so concurrent calls take each other's replies and the robbed
#      call waits out its budget. A call that times out leaves its worker on
#      the socket, and the next call waits within its own budget for that
#      worker to leave or for the SI to be replaced.
#
# Metrics surfaced on success/failure:
#   - cert_daemon_ws_rpc_timeouts_total (counter)
#   - cert_daemon_ws_force_reconnects_total (counter)
#   - cert_daemon_ws_transport_replacements_total (counter)
#   - cert_daemon_ws_consecutive_reconnect_failures (gauge)
#   - last_poll_timestamp bumped on every success
# ---------------------------------------------------------------------------


class RPCTimeoutError(Exception):
    """Raised when an RPC method call exceeds the bounded-timeout budget.

    A daemon-side wrapper type so callers can catch this specifically and
    react (retry, log, drop pending row, etc.) without coupling to the
    threading impl detail.
    """


# What a websocket raises once its peer, or a proxy in between (Cloudflare
# drops idle websockets after ~100 s), has dropped the connection: every later
# call on it fails the same way. substrate-interface reconnects by itself only
# when websocket-client has already cleared the socket, which a failed send
# (EPIPE) never does. CPython builds an OSError carrying EPIPE, ECONNRESET or
# ECONNABORTED as the matching subclass below. websocket-client raises
# WebSocketTimeoutException for a socket the kernel failed with ETIMEDOUT
# (TCP keepalive probes unanswered): the daemon sets no websocket timeout, so
# nothing else raises it. Node verdicts (SubstrateRequestException, a 1010)
# and validation errors (ValueError) are not transport failures.
TRANSPORT_DEAD_ERRORS = (
    BrokenPipeError,
    ConnectionResetError,
    ConnectionAbortedError,
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
    ssl.SSLEOFError,
)
# A call whose socket another call closed under it fails with EBADF (plain or
# TLS alike), or ENOTSOCK once the descriptor is reused; a socket no longer
# connected fails with ENOTCONN. None of these has an OSError subclass.
TRANSPORT_DEAD_ERRNOS = frozenset({errno.EBADF, errno.ENOTSOCK, errno.ENOTCONN})


def _error_chain(error: Optional[BaseException]) -> Iterator[BaseException]:
    """``error`` and what caused it, in the order a traceback prints them."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        yield error
        error = error.__cause__ or (None if error.__suppress_context__ else error.__context__)


def is_transport_dead(error: BaseException) -> bool:
    """Whether ``error``, or an error that caused it, says the socket is dead."""
    return any(
        isinstance(e, TRANSPORT_DEAD_ERRORS) or (isinstance(e, OSError) and e.errno in TRANSPORT_DEAD_ERRNOS)
        for e in _error_chain(error)
    )


class AttestUnsettled(Exception):
    """The node refused this signer's attest of a receipt, and the chain, read
    at the refusing node's best block, neither holds the attest nor rules it
    out yet. ``cert_hash`` is the hash the attest claimed."""

    def __init__(self, receipt_id: str, cert_hash: bytes, refusal: BaseException):
        super().__init__(
            f"attest of {receipt_id} refused before the chain settled it: {type(refusal).__name__}: {refusal}"
        )
        self.cert_hash = cert_hash


def should_defer(error: BaseException) -> bool:
    """Whether a receipt whose processing raised ``error`` is retried on a
    later tick: the RPC did not serve a call at all (a dead socket, or no
    answer within the timeout budget), or the node refused its attest for a
    state the chain does not show yet."""
    return is_transport_dead(error) or any(
        isinstance(e, (RPCTimeoutError, AttestUnsettled)) for e in _error_chain(error)
    )


def _read_float_env(name: str, default: float) -> float:
    """Parse a float env var, falling back to ``default`` on missing or
    malformed input. Used for tunable budgets like ``MATERIOS_RPC_TIMEOUT_SECS``.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        v = float(raw)
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def _read_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


@dataclass
class SubmitCertOutcome:
    """Result of `submit_availability_cert`.

    Post-spec-219, the runtime returns `Ok(())` on cert-hash mismatch (so
    `BadAttestStrike` + `AutoSlashedForBadAttest` side-effects persist),
    which means `ExtrinsicReceipt.is_success` is a misleading indicator
    of attestation acceptance — it's only an indicator of inclusion. The
    daemon MUST inspect `triggered_events` for the strike/slash markers
    matching its own signer to know whether the attest was counted. This
    outcome carries the verdict explicitly so callers don't have to learn
    the convention.

    `success` is True iff the dispatch landed AND no `BadAttestStrike`
    against our signer fired. A strike at threshold also triggers
    `AutoSlashedForBadAttest` (committee ejection + bond slashed); we
    surface that as a separate flag so the caller can fire a more urgent
    operator alert.

    `already_on_chain` marks a success this submit did not make: the runtime
    refused it because the receipt is certified or already carries this
    signer's attest.
    """
    success: bool
    already_on_chain: bool = False
    bad_attest_strike: bool = False
    auto_slashed: bool = False
    strikes: int = 0
    claimed: Optional[bytes] = None
    canonical: Optional[bytes] = None
    slashed_amount: int = 0
    error_message: Optional[str] = None

    def __bool__(self) -> bool:
        return self.success


def _to_bytes32(val) -> bytes:
    """Convert SCALE-decoded [u8; 32] to bytes. Handles hex strings, lists, and bytes."""
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return bytes.fromhex(val.removeprefix("0x"))
    if isinstance(val, (list, tuple)):
        return bytes(val)
    return bytes(val)


def _to_bytes_exact(val, expected_len: int) -> bytes:
    """Convert SCALE-decoded [u8; N] to bytes, validating the length.

    Like ``_to_bytes32`` but for arbitrary fixed widths (the settlement
    pallet exposes a 28-byte ``beneficiary_addr_blake2_224`` field that
    we have to thread through without silently widening).
    Raises ``ValueError`` if the decoded value's length doesn't match.
    """
    raw = _to_bytes32(val) if expected_len == 32 else (
        val if isinstance(val, bytes) else (
            bytes.fromhex(val.removeprefix("0x")) if isinstance(val, str)
            else bytes(val)
        )
    )
    if len(raw) != expected_len:
        raise ValueError(
            f"expected {expected_len}-byte value, got {len(raw)} bytes"
        )
    return raw


def _account_id_bytes(account: str) -> bytes:
    """An AccountId32 from its SS58 or 0x-hex form."""
    raw = bytes.fromhex(account[2:] if account.startswith("0x") else ss58_decode(account))
    if len(raw) != 32:
        raise ValueError("account id must be 32 bytes")
    return raw


def _account_ids(accounts) -> frozenset[bytes]:
    """The AccountId32s of a decoded storage list of accounts."""
    return frozenset(_account_id_bytes(str(account)) for account in accounts)


def _signature_params(sigs: list) -> list[tuple[str, str]]:
    """A ``Vec<(CommitteePubkey, CommitteeSig)>`` argument, checked for width."""
    if not sigs:
        raise ValueError("sigs must be non-empty")
    for i, (pub, sig) in enumerate(sigs):
        if len(pub) != 32:
            raise ValueError(f"sigs[{i}].pubkey must be 32 bytes, got {len(pub)}")
        if len(sig) != 64:
            raise ValueError(f"sigs[{i}].sig must be 64 bytes, got {len(sig)}")
    return [("0x" + pub.hex(), "0x" + sig.hex()) for pub, sig in sigs]


def _maybe_bytes32(val) -> Optional[bytes]:
    """Best-effort `_to_bytes32` — returns None on undecodable / missing input.

    Used when surfacing event attributes for logs: we'd rather log "?" than
    crash the strike-detection path on a malformed event field.
    """
    if val is None:
        return None
    try:
        return _to_bytes32(val)
    except (ValueError, TypeError):
        return None


def _bytes_from_scale_value(val) -> bytes:
    """Convert SCALE-decoded variable-length bytes to a Python ``bytes``.

    substrate-interface decodes ``BoundedVec<u8, ...>`` as either a
    ``"0x..."`` hex string or a list of ints, depending on the type
    registry. Both shapes need to round-trip to the same bytes for the
    voucher-digest derivation to work regardless of decoder mood.
    """
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return bytes.fromhex(val.removeprefix("0x"))
    if isinstance(val, (list, tuple)):
        return bytes(val)
    raise TypeError(f"can't convert {type(val).__name__} to bytes")


def _extract_voucher_fields_for_digest(row: dict) -> dict:
    """Pluck the fields required by :func:`compute_voucher_digest_with_address`
    out of a ``Vouchers[claim_id]`` storage row.

    The pallet's ``Voucher`` struct (``pallets/intent-settlement/src/types.rs``):

      - ``policy_id``: H256 (32B)
      - ``beneficiary_cardano_addr``: BoundedVec<u8, MAX_CARDANO_ADDR>
      - ``amount_ada``: u64
      - ``batch_fairness_proof_digest``: [u8; 32]
      - ``issued_block``: u32
      - ``expiry_slot_cardano``: u64

    Raises ``KeyError`` / ``TypeError`` / ``ValueError`` for malformed
    rows so the caller logs the specific shape mismatch.
    """
    return {
        "policy_id": _to_bytes_exact(row["policy_id"], 32),
        "beneficiary_cardano_addr": _bytes_from_scale_value(
            row["beneficiary_cardano_addr"]
        ),
        "amount_ada": int(row["amount_ada"]),
        "batch_fairness_proof_digest": _to_bytes_exact(
            row["batch_fairness_proof_digest"], 32
        ),
        "issued_block": int(row["issued_block"]),
        "expiry_slot_cardano": int(row["expiry_slot_cardano"]),
    }


class _FifoLock:
    """A lock granted in arrival order: a release hands it to the longest
    waiter. A caller that releases and asks again at once queues behind the
    others instead of starving them, so a wait is bounded by the calls ahead
    of it (#72). Any thread may release it."""

    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._held = False
        self._waiters: collections.deque[threading.Event] = collections.deque()

    def acquire(self, timeout: float, give_up: Callable[[], bool]) -> bool:
        """Whether the lock was granted within ``timeout`` seconds. A waiter
        checks ``give_up`` every REPLACED_CHECK_SECS and leaves the queue
        once it holds."""
        with self._mutex:
            if not self._held:
                self._held = True
                return True
            granted = threading.Event()
            self._waiters.append(granted)
        deadline = time.monotonic() + timeout
        while not granted.wait(max(0.0, min(REPLACED_CHECK_SECS, deadline - time.monotonic()))):
            if give_up() or time.monotonic() >= deadline:
                with self._mutex:
                    if not granted.is_set():
                        self._waiters.remove(granted)
                        return False
                break
        return True

    def release(self) -> None:
        with self._mutex:
            if self._waiters:
                self._waiters.popleft().set()
            else:
                self._held = False


class _WsConn:
    """One ``SubstrateInterface`` plus its independent bounded-timeout and
    reconnect state.

    Two run concurrently: a ``write`` conn that signs and submits, and a
    ``read`` conn for reads. A ``submit_extrinsic(wait_for_inclusion=True)``
    holds the write socket for seconds, and a call made on that socket
    meanwhile waits behind it, past its budget under load: the
    get_events-behind-submit wedge (tasks #288/#500) that silently stalled
    attestation across the committee. The only reads on the write conn are a
    submit's own outcome, within its call, and the two that must see the node
    a submit goes to, which run under the chain-write lock that serializes
    submits. Each conn force-replaces itself in isolation: a wedged write
    socket never trips a read reconnect, and vice versa.

    ``bound`` pairs the SI with the lock that admits one call at a time onto
    it (#72). Every SI gets a fresh lock, so a worker abandoned on a replaced
    SI never holds up its replacement. Reading ``bound`` once gives an SI and
    its own lock, even while another thread replaces them.
    """

    __slots__ = (
        "name",
        "bound",
        "consecutive_timeouts",
        "consecutive_reconnect_failures",
        "replace_lock",
    )

    def __init__(self, name: str) -> None:
        self.name = name
        self.si = None
        self.consecutive_timeouts = 0
        self.consecutive_reconnect_failures = 0
        # Held while a dead or missing SI is replaced, so calls that fail
        # together on one socket replace it once.
        self.replace_lock = threading.Lock()

    @property
    def si(self) -> Optional[SubstrateInterface]:
        return self.bound[0]

    @si.setter
    def si(self, value: Optional[SubstrateInterface]) -> None:
        self.bound = (value, _FifoLock())


class SubstrateClient:
    def __init__(self, config: DaemonConfig):
        self.config = config
        # Two independent websocket connections (task #500): `write` signs and
        # submits, `read` carries reads. `self.substrate` aliases the write
        # conn's interface for test fixtures.
        self._write = _WsConn("write")
        self._read = _WsConn("read")
        # (runtime code hash, its settlement contract) from the last metadata read.
        self._settlement_contract: Optional[tuple[str, dict]] = None
        # The runtime whose settlement contract, or refusal, was last logged.
        self._settlement_reported: Optional[str] = None
        # Bounded-timeout + reconnect knobs (tasks #288 + #156). Each is
        # overridable via env so operators can tune in prod without a
        # re-deploy; the defaults are conservative.
        #
        # The default 30s timeout is set well above ``wait_for_inclusion``
        # block time (~6s per block, occasional 12s tip lag) so legitimate
        # extrinsic submits don't false-alarm — but well below the
        # outer-watchdog's 180s STALE_THRESHOLD so the wedge is broken
        # before the container restarts.
        self._rpc_timeout_secs: float = _read_float_env(
            "MATERIOS_RPC_TIMEOUT_SECS", 30.0
        )
        self._reconnect_after_timeouts: int = _read_int_env(
            "MATERIOS_RPC_RECONNECT_AFTER_TIMEOUTS", 2
        )
        # Hard cap on reconnect attempts in a single force-replace cycle.
        # Without this, a persistent network outage could spin the
        # backoff loop forever. After the cap is hit the wrapper bails
        # with the latest constructor error; the next user-call kicks
        # off a fresh cycle.
        self._reconnect_max_attempts: int = _read_int_env(
            "MATERIOS_RPC_RECONNECT_MAX_ATTEMPTS", 8
        )
        # Seconds between keepalive RPCs on the write conn, which idles between
        # receipts while proxies drop idle websockets (Cloudflare after ~100 s).
        self.keepalive_secs: float = _read_float_env(
            "MATERIOS_RPC_KEEPALIVE_SECS", 30.0
        )
        # Each ``_call`` dispatches its work on a fresh daemon thread.
        # We can't reuse a single-worker pool — a wedged thread would
        # block all subsequent calls (the very problem we're fixing).
        # We can't bound a multi-worker pool either — wedged threads
        # leak, and a bounded pool would eventually starve. A fresh
        # ``threading.Thread`` per call is the cheapest correct option:
        # Python threads cost ~8KB each, and the wedged-thread count
        # is bounded by the timeout frequency × the OS's open-fd cap
        # (i.e. it's self-limiting in practice — the OS reaps the
        # underlying socket when ``close()`` is called on the SI).
        #
        # Guards ``self.substrate`` replacement during reconnect. RPC
        # calls take this in shared mode (the bookkeeping is read-then-
        # write of a few ints) so the force-replace path can swap the
        # SI without racing.
        self._reconnect_lock: threading.RLock = threading.RLock()
        # Injectable so backoff tests can run instantaneously.
        self._backoff_sleep = time.sleep
        # Set by connect(). A client that never connected (a fixture that
        # wires only the write conn) never dials a read conn.
        self._dialled = False
        # The background reopen of a read conn left without an SI, once started.
        self._read_reopen: Optional[threading.Thread] = None

    @functools.cached_property
    def keypair(self) -> Keypair:
        """Derived on first use, so a client that only reads never holds a signer."""
        return Keypair.create_from_uri(self.config.signer_uri)

    def connect(self) -> bool:
        """Open the write + read SubstrateInterface connections.

        Both target the same RPC URL but are distinct sockets so a
        submit-held write socket can't block poll-loop reads. Subsequent
        forced reconnects go through ``_force_replace`` per conn, sharing
        the same constructor helper. Returns True once the write conn (the
        one that must never be down) is live; a failed read conn is logged,
        and reads run on the write conn until a background reopen succeeds.
        """
        self._ensure_wrapper_state()
        self._dialled = True
        for conn in (self._write, self._read):
            try:
                conn.si = self._create_substrate_interface()
                logger.info(
                    f"Connected {conn.name} conn to {self.config.rpc_url}, "
                    f"chain: {conn.si.chain}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to connect {conn.name} conn to substrate: {e}"
                )
                conn.si = None
        return self._write.si is not None

    def _create_substrate_interface(self) -> SubstrateInterface:
        """Single chokepoint for SubstrateInterface construction.

        Tests patch this method to inject fakes / failure modes without
        dialling a real websocket. Production behavior matches the prior
        inlined construction (``strict_scale_decode=False`` is required so
        the daemon stays forgiving of optional / additive SCALE schema
        fields across chain spec versions).
        """
        return SubstrateInterface(
            url=self.config.rpc_url, config={"strict_scale_decode": False}
        )

    @property
    def connected(self) -> bool:
        return self._write.si is not None

    @property
    def substrate(self) -> Optional[SubstrateInterface]:
        """The write connection's interface, which test fixtures set and read.
        Daemon code reaches the chain only through ``_call``."""
        self._ensure_wrapper_state()
        return self._write.si

    @substrate.setter
    def substrate(self, value: Optional[SubstrateInterface]) -> None:
        self._ensure_wrapper_state()
        self._write.si = value

    # --- Bounded-timeout RPC wrapper (tasks #288 + #156) ---------------------

    def _ensure_wrapper_state(self) -> None:
        """Lazy-init the WSGuard fields if they're missing.

        Some test fixtures construct ``SubstrateClient`` via ``__new__`` to
        bypass ``__init__`` (so they don't need a Keypair / real config);
        such fixtures rely on the wrapper to set up its own state on first
        call. Production startup goes through ``__init__`` and these
        fields are already set — the check is cheap.
        """
        if not hasattr(self, "_reconnect_lock"):
            self._reconnect_lock = threading.RLock()
        if not hasattr(self, "_rpc_timeout_secs"):
            self._rpc_timeout_secs = _read_float_env(
                "MATERIOS_RPC_TIMEOUT_SECS", 30.0
            )
        if not hasattr(self, "_reconnect_after_timeouts"):
            self._reconnect_after_timeouts = _read_int_env(
                "MATERIOS_RPC_RECONNECT_AFTER_TIMEOUTS", 2
            )
        if not hasattr(self, "_reconnect_max_attempts"):
            self._reconnect_max_attempts = _read_int_env(
                "MATERIOS_RPC_RECONNECT_MAX_ATTEMPTS", 8
            )
        if not hasattr(self, "_write"):
            self._write = _WsConn("write")
        if not hasattr(self, "_read"):
            self._read = _WsConn("read")
        if not hasattr(self, "_backoff_sleep"):
            self._backoff_sleep = time.sleep
        if not hasattr(self, "_dialled"):
            self._dialled = False
        if not hasattr(self, "_read_reopen"):
            self._read_reopen = None

    def _call(self, method_name: str, *args, conn: "_WsConn", **kwargs) -> Any:
        """Invoke ``<conn>.si.<method_name>(*args, **kwargs)`` under a bounded
        timeout.

        Every call names its conn. Reads pass ``conn=self._read``, a socket
        that a submit holding the write socket can never block; the write
        conn carries signing, submits, the keepalive, and the reads that must
        see the node a submit goes to. tests/test_conn_routing.py pins each
        call site's conn. If the read conn was never opened (pre-split
        fixtures) or is mid-reconnect, a read degrades to the write conn
        rather than failing; a read conn this client dialled is then reopened
        in the background.

        Nothing a call returns reads the chain afterwards. A submit's
        ``ExtrinsicReceipt`` has its block and events read within the call,
        so its outcome needs no further RPC, and a ``query_map`` returns the
        list of all its rows.

        If the call exceeds the budget we raise :class:`RPCTimeoutError`;
        consecutive timeouts on that conn trigger a force-replace of *its*
        ``SubstrateInterface`` (the other conn is untouched). A call that
        fails on a dead socket replaces that conn's SI at once and runs once
        more on the fresh one; failing there too, it replaces the SI again and
        propagates. A resent submit carries the same signed extrinsic, whose
        nonce the runtime accepts at most once. Every other exception is the
        node's verdict and propagates untouched. On success we bump
        ``health_server.update_metrics(last_poll_timestamp=...)`` so the outer
        watchdog's ``/ready last_poll_age`` reflects the freshest chain
        interaction.

        Threading model: each call runs on a fresh daemonised thread, and one
        call at a time runs on each SI. The worker holds the SI's lock until
        it leaves the socket. When the call wedges, the wrapper raises
        ``RPCTimeoutError`` and abandons the worker, which keeps the lock: no
        other call goes onto that socket until the worker exits, when the OS
        reaps the socket (``close()`` in force-replace) or at process exit.
        A call waiting for the lock that sees its SI replaced runs once more
        on the replacement, as after a dead socket.
        """
        self._ensure_wrapper_state()
        for attempt in (1, 2):
            live, si, lock = self._live_conn(conn, method_name)
            try:
                return self._call_once(live, si, lock, method_name, args, kwargs)
            except Exception as error:
                if not is_transport_dead(error):
                    raise
                self._replace_dead_conn(live, si, method_name, error)
                if attempt == 2 or live.si is None:
                    raise

    def _live_conn(
        self, conn: "_WsConn", method_name: str
    ) -> "tuple[_WsConn, SubstrateInterface, _FifoLock]":
        """The conn a call on ``conn`` runs on, its SI, and that SI's call
        lock. A write conn whose last replace gave up is reconnected here.
        A read conn without an SI sends the read to the write conn at once
        and, once this client has dialled it, starts a background reopen:
        nothing else reopens it while the write conn stays healthy (#70)."""
        with self._reconnect_lock:
            if conn is self._read and conn.si is None:
                if self._dialled and (self._read_reopen is None or not self._read_reopen.is_alive()):
                    logger.error(
                        "the read conn is down: reads run on the write conn, behind its submits, "
                        "until a background reopen succeeds"
                    )
                    self._read_reopen = threading.Thread(
                        target=self._reopen_read_conn, name="substrate-read-reopen", daemon=True
                    )
                    self._read_reopen.start()
                conn = self._write
            si, lock = conn.bound
        if si is None:
            with conn.replace_lock:
                if conn.si is None:
                    self._force_replace(conn)
                si, lock = conn.bound
        if si is None:
            raise RPCTimeoutError(
                f"_call({method_name!r}) found no live SubstrateInterface on "
                f"the {conn.name} conn and could not reconnect it"
            )
        return conn, si, lock

    def _reopen_read_conn(self) -> None:
        """One reconnect cycle for the read conn, with its backoff, off the
        thread whose read found it empty."""
        with self._read.replace_lock:
            if self._read.si is None:
                self._force_replace(self._read)

    def _call_once(
        self, conn: "_WsConn", si: SubstrateInterface, lock: _FifoLock, method_name: str, args: tuple,
        kwargs: dict,
    ) -> Any:
        deadline = time.monotonic() + self._rpc_timeout_secs
        acquired = lock.acquire(self._rpc_timeout_secs, lambda: conn.si is not si)
        # A call granted the socket only past its budget never runs, since its
        # caller has given up on it. One whose SI was replaced while it waited
        # runs again on the replacement instead.
        if acquired and (conn.si is not si or time.monotonic() >= deadline):
            lock.release()
            acquired = False
        if conn.si is not si:
            raise WebSocketConnectionClosedException(
                f"the {conn.name} conn's socket was replaced while {method_name!r} waited for it"
            )
        if not acquired:
            self._on_rpc_timeout(conn, si, method_name)
            raise RPCTimeoutError(
                f"RPC {method_name!r} exceeded {self._rpc_timeout_secs:.1f}s budget on the "
                f"{conn.name} conn, waiting behind a call still on its socket"
            )
        result_holder: dict[str, Any] = {}

        def _invoke():
            try:
                result = getattr(si, method_name)(*args, **kwargs)
                # Both read the chain through ``si`` when first used: a
                # receipt its block and events, a map its next pages. Read
                # them here, within this call's budget.
                if isinstance(result, ExtrinsicReceipt):
                    result.process_events()
                elif isinstance(result, QueryMapResult):
                    result = list(result)
                result_holder["ok"] = result
            except BaseException as e:  # noqa: BLE001
                result_holder["err"] = e
            finally:
                lock.release()

        worker = threading.Thread(
            target=_invoke,
            name=f"substrate-rpc-{conn.name}-{method_name}",
            daemon=True,
        )
        try:
            worker.start()
        except BaseException:
            lock.release()
            raise
        worker.join(timeout=max(0.0, deadline - time.monotonic()))
        if worker.is_alive():
            # Thread still running past the budget — abandon it and report
            # timeout. The thread will die when the OS reaps the socket fd
            # (close() in force-replace) or at process exit.
            self._on_rpc_timeout(conn, si, method_name)
            raise RPCTimeoutError(
                f"RPC {method_name!r} exceeded {self._rpc_timeout_secs:.1f}s "
                f"budget on the {conn.name} conn"
            )
        if "err" in result_holder:
            error = result_holder["err"]
            if (isinstance(error, json.JSONDecodeError)
                    and getattr(getattr(si, "websocket", None), "connected", True) is False):
                # websocket-client answers the peer's close frame by marking
                # itself closed and returning "", which substrate-interface
                # then fails to parse.
                raise WebSocketConnectionClosedException(
                    f"the {conn.name} conn's peer closed the websocket"
                ) from error
            # Any other exception (SubstrateRequestException, ValueError,
            # etc.) is the underlying call's verdict — surface as-is so
            # existing `except` clauses still match. Real exceptions do
            # NOT count toward the consecutive-timeout streak.
            raise error
        self._on_rpc_success(conn)
        return result_holder.get("ok")

    def _replace_dead_conn(
        self, conn: "_WsConn", dead_si: SubstrateInterface, method_name: str, error: BaseException
    ) -> None:
        """Replace ``conn``'s SI after ``dead_si`` failed on a dead socket,
        unless a call that failed alongside already replaced it."""
        with conn.replace_lock:
            if conn.si is not dead_si:
                return
            logger.warning(
                f"force-replace[{conn.name}]: transport error {type(error).__name__} "
                f"in {method_name!r}: {error}"
            )
            health_server.increment_metric("ws_transport_replacements_total")
            self._force_replace(conn)

    def _on_rpc_success(self, conn: "_WsConn") -> None:
        """Reset the conn's consecutive-timeout streak and bump health_last_poll."""
        conn.consecutive_timeouts = 0
        try:
            health_server.update_metrics(last_poll_timestamp=time.time())
        except Exception:
            # Never let a metrics bump kill a successful RPC.
            pass

    def _on_rpc_timeout(self, conn: "_WsConn", si: SubstrateInterface, method_name: str) -> None:
        """Bookkeeping for a call that timed out on ``si``: bump the conn's
        counters, possibly trigger a force-replace of that conn's SI. A conn
        that already replaced ``si`` while the call was wedged on it found that
        socket dead: the timeout was the dead socket's, not a strike against
        its replacement."""
        try:
            health_server.increment_metric("ws_rpc_timeouts_total")
        except Exception:
            pass
        if conn.si is not si:
            return
        conn.consecutive_timeouts += 1
        logger.warning(
            f"substrate RPC {method_name!r} timed out after "
            f"{self._rpc_timeout_secs:.1f}s on the {conn.name} conn "
            f"(consecutive={conn.consecutive_timeouts})"
        )
        if conn.consecutive_timeouts >= self._reconnect_after_timeouts:
            with conn.replace_lock:
                self._force_replace(conn)

    def _force_replace(self, conn: "_WsConn") -> None:
        """Close a conn's websocket and construct a new one, in isolation.

        Exponential backoff between failed reconnects: 1, 2, 4, 8, 16, 30
        (capped). After ``self._reconnect_max_attempts`` failures we return
        without a working SI on that conn: the next call on the write conn
        starts a fresh cycle first, and the next read starts one for the read
        conn in the background. The conn's timeout streak is reset on the way
        out so the next RPC starts clean. The *other* conn is never touched.
        """
        old_si = conn.si
        # Emptied before the close, so a call that the close wakes, or that is
        # waiting for the old SI's lock, finds it replaced and never runs on it.
        conn.si = None
        # Best-effort close on the dead WS. Failures here are ignored —
        # the underlying socket is already in an unknown state.
        if old_si is not None:
            try:
                old_si.close()
            except Exception as e:  # noqa: BLE001
                logger.info(
                    f"force-replace[{conn.name}]: old SI close() raised "
                    f"{type(e).__name__}: {e}; continuing"
                )
        # Reset the timeout streak whether or not the reconnect succeeds —
        # we don't want a still-wedged WS to keep stacking force-replace
        # attempts in a single call.
        conn.consecutive_timeouts = 0
        attempt = 0
        while attempt < self._reconnect_max_attempts:
            try:
                new_si = self._create_substrate_interface()
            except Exception as e:  # noqa: BLE001
                conn.consecutive_reconnect_failures += 1
                try:
                    health_server.update_metrics(
                        ws_consecutive_reconnect_failures=conn.consecutive_reconnect_failures
                    )
                except Exception:
                    pass
                # Backoff: 1, 2, 4, 8, 16, then 30s cap.
                # ``2 ** (n-1)`` for n=1..5 = 1, 2, 4, 8, 16; clamped at 30.
                delay = min(30, 1 << (conn.consecutive_reconnect_failures - 1))
                logger.warning(
                    f"force-replace[{conn.name}]: SubstrateInterface ctor failed "
                    f"(attempt {attempt + 1}/{self._reconnect_max_attempts}, "
                    f"streak={conn.consecutive_reconnect_failures}): "
                    f"{type(e).__name__}: {e}; sleeping {delay}s"
                )
                self._backoff_sleep(delay)
                attempt += 1
                continue
            # Success.
            conn.si = new_si
            try:
                health_server.increment_metric("ws_force_reconnects_total")
                health_server.update_metrics(
                    ws_consecutive_reconnect_failures=0,
                    substrate_connected=True,
                )
            except Exception:
                pass
            conn.consecutive_reconnect_failures = 0
            logger.warning(
                f"force-replace[{conn.name}]: new SubstrateInterface online "
                f"(chain={getattr(new_si, 'chain', '?')})"
            )
            return
        # Bailed without a working SI. The next user-call enters _call and
        # finds this conn's si is None: a read degrades to the write conn and
        # starts a background reopen, a write runs a fresh reconnect cycle first.
        try:
            health_server.update_metrics(
                ws_consecutive_reconnect_failures=conn.consecutive_reconnect_failures,
                substrate_connected=False,
            )
        except Exception:
            pass
        next_step = (
            "reads run on the write conn until a background reopen succeeds"
            if conn is self._read else "the next call on it reconnects first"
        )
        logger.error(
            f"force-replace[{conn.name}]: gave up after {self._reconnect_max_attempts} attempts; "
            f"{next_step} (streak={conn.consecutive_reconnect_failures})"
        )

    def rpc(self, method: str, params: list) -> Any:
        """A raw JSON-RPC read on the read conn, under the bounded timeout."""
        return self._call("rpc_request", method, params, conn=self._read)["result"]

    def keepalive(self) -> None:
        """One cheap RPC on the write conn, through the bounded call."""
        self._call("rpc_request", "system_health", [], conn=self._write)

    def get_finalized_head_number(self) -> int:
        head_hash = self._call("get_chain_finalised_head", conn=self._read)
        header = self._call("get_block_header", head_hash, conn=self._read)
        return header["header"]["number"]

    def get_best_block_number(self) -> int:
        header = self._call("get_block_header", conn=self._read)
        return header["header"]["number"]

    def get_genesis_hash(self) -> str:
        """Return the chain's genesis hash (0x-prefixed lowercase hex). Used to
        detect that we're pointed at a different chain than we were last run
        (e.g. a chain reset) and self-heal stale daemon state."""
        return self._call("get_block_hash", 0, conn=self._read)

    def get_settlement_runtime_context(self) -> dict:
        """Read the slash protocol, supersede support, TTL and minimum finality
        depth from one head's post-state metadata. Refuses any runtime outside
        the reviewed contracts.

        Metadata is a function of the runtime code alone, so its parse is kept
        for the ``:code`` hash at the head and redone once that hash changes.
        Each runtime's selection, or refusal, is logged once and exported as
        the ``settlement_*`` metrics."""
        head = self._call("get_chain_head", conn=self._read)
        version = self._call("get_block_runtime_version", head, conn=self._read)
        block = self._call("get_block_number", head, conn=self._read)
        code_hash = self._call(
            "rpc_request", "state_getStorageHash", [RUNTIME_CODE_KEY, head], conn=self._read
        ).get("result")
        spec = version.get("specVersion") if isinstance(version, dict) else None
        runtime = code_hash or f"spec {spec}"
        try:
            if version["specName"] != "materios":
                raise ValueError("unexpected spec name")
            cached = self._settlement_contract
            if code_hash and cached is not None and cached[0] == code_hash:
                contract = cached[1]
            else:
                contract = self._read_settlement_contract(head)
                if code_hash:
                    self._settlement_contract = (code_hash, contract)
        except (KeyError, TypeError, ValueError, StopIteration, AttributeError) as error:
            health_server.update_metrics(settlement_slash_protocol=0, settlement_supersede_offered=0)
            if self._settlement_reported != runtime:
                self._settlement_reported = runtime
                logger.error("settlement runtime spec %s (code %s) refused: %s: %s; nothing will be signed",
                             spec, code_hash, type(error).__name__, error)
            raise ValueError("unknown settlement runtime metadata or TTL") from error
        health_server.update_metrics(
            settlement_slash_protocol=SLASH_PROTOCOL_GAUGE[contract["protocol"]],
            settlement_supersede_offered=int(contract["supersede"]),
            settlement_runtime_spec_version=spec,
        )
        if self._settlement_reported != runtime:
            self._settlement_reported = runtime
            logger.info(
                "settlement runtime spec=%s (code %s): protocol=%s supersede=%s fingerprint=%s ttl=%d "
                "min_finality_depth=%d", spec, code_hash, contract["protocol"], contract["supersede"],
                contract["fingerprint"], contract["ttl"], contract["min_finality_depth"],
            )
        return dict(block_hash=head, block_number=block, spec_version=spec, **contract)

    def _read_settlement_contract(self, head: str) -> dict:
        # get_constant/get_metadata_call_function select the parent's runtime
        # in substrate-interface 1.x, which is stale at an upgrade block.
        return settlement_contract(self._call("get_block_metadata", head, conn=self._read))

    # --- Bond helpers (OrinqReceipts pallet) --------------------------------
    # These are used by CertDaemon._ensure_bond() to keep the attestor's
    # reserved MATRA at or above `BondRequirement` so `join_committee` doesn't
    # fail with `InsufficientBond`. See README → "Auto-bond on startup".

    def get_bond_requirement(self) -> int:
        """Return `OrinqReceipts.BondRequirement` in MATRA base units (u128).

        Returns 0 when the storage item is absent (pre-runtime-upgrade chains)
        so `_ensure_bond()` falls through to its "nothing to do" branch.
        """
        result = self._call("query", "OrinqReceipts", "BondRequirement", conn=self._read)
        val = result.value
        if val is None:
            return 0
        return int(val)

    def get_attestor_bond(self, address: str) -> int:
        """Return `OrinqReceipts.AttestorBonds(address)` in base units.

        Maps to 0 for accounts that have never bonded (the storage value is
        `ValueQuery` so the runtime returns 0 in that case; we defensively
        handle a missing value anyway).
        """
        result = self._call("query", "OrinqReceipts", "AttestorBonds", [address], conn=self._read)
        val = result.value
        if val is None:
            return 0
        return int(val)

    def get_free_balance(self, address: str) -> int:
        """Return `System.Account(address).data.free` in base units.

        Returns 0 if the account row does not exist (never received MATRA).
        """
        result = self._call("query", "System", "Account", [address], conn=self._read)
        val = result.value
        if val is None:
            return 0
        # substrate-interface decodes this as a dict {"nonce": .., "data": {..}}
        try:
            return int(val["data"]["free"])
        except (KeyError, TypeError):
            return 0

    def get_receipt_content_hash(self, receipt_id_hex: str) -> Optional[bytes]:
        """Return ``OrinqReceipts.Receipts(receipt_id).content_hash`` as raw
        bytes, or None if the receipt isn't on chain yet.

        Used by ``daemon.evidence_submitter`` (task #156) to recover the
        32-byte content_hash for ``TeeAttestation.submit_evidence`` —
        ``receipt_id == sha256(content_hash)`` is one-way so we have to
        read it back from chain.

        Routes through the bounded-timeout wrapper so a wedged WS can't
        strand the evidence-submission path (task #288 / #156). Returns
        None on any error including RPCTimeoutError — the caller's
        next-tick retry path covers transient misses.
        """
        try:
            rid = (
                receipt_id_hex
                if receipt_id_hex.startswith("0x")
                else "0x" + receipt_id_hex
            )
            result = self._call(
                "query",
                module="OrinqReceipts",
                storage_function="Receipts",
                params=[rid],
                conn=self._read,
            )
            if result is None or result.value is None:
                return None
            ch = result.value.get("content_hash")
            if ch is None:
                return None
            if isinstance(ch, str):
                s = ch[2:] if ch.startswith("0x") else ch
                return bytes.fromhex(s)
            if isinstance(ch, (list, tuple)):
                return bytes(ch)
            if isinstance(ch, (bytes, bytearray)):
                return bytes(ch)
            return None
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"get_receipt_content_hash: failed to resolve "
                f"receipt_id={receipt_id_hex[:16]}...: "
                f"{type(e).__name__}: {e}"
            )
            return None

    def compose_call(self, **kwargs):
        """Thin bounded-timeout wrapper over ``self.substrate.compose_call``.

        Exposed for callers in :mod:`daemon.evidence_submitter` that build
        their own extrinsics outside this module's typed helpers (task #156).
        Returns the SCALE-encoded call object as-is.
        """
        return self._call("compose_call", **kwargs, conn=self._write)

    def create_signed_extrinsic(self, **kwargs):
        """Sister wrapper to :meth:`compose_call`; bounded-timeout-wrap."""
        return self._call("create_signed_extrinsic", **kwargs, conn=self._write)

    def submit_extrinsic(self, extrinsic, wait_for_inclusion: bool = True):
        """Sister wrapper to :meth:`compose_call`; bounded-timeout-wrap."""
        return self._call(
            "submit_extrinsic", extrinsic, wait_for_inclusion=wait_for_inclusion, conn=self._write
        )

    def get_committee_members(self):
        """Return ``OrinqReceipts.CommitteeMembers`` storage as-is.

        Cert-daemon's ``_ensure_committee_membership`` uses the result for
        membership-check iteration. Routes through the bounded-timeout
        wrapper so a wedged WS can't strand the join-committee path
        forever — same wedge class as the poll loop (task #288).
        """
        return self._call("query", "OrinqReceipts", "CommitteeMembers", conn=self._read)

    def get_committee_pubkeys(self) -> frozenset[bytes]:
        """The committee keys settlement signatures must come from: the raw
        32 bytes of each ``OrinqReceipts.CommitteeMembers`` account, which the
        runtime's committee adapter uses as the pubkey."""
        members = self.get_committee_members()
        return _account_ids(members.value if hasattr(members, "value") else members)

    def submit_join_committee(self):
        """Submit ``OrinqReceipts.join_committee()`` and return the included
        extrinsic receipt. Returns the receipt unchanged so the caller can
        inspect ``is_success`` + ``error_message`` (the join path needs
        per-error branching that's specific to cert_daemon).

        Routes through the wrapper for the same reason as
        :meth:`get_committee_members` — task #288.
        """
        call = self._call(
            "compose_call",
            call_module="OrinqReceipts",
            call_function="join_committee",
            call_params={},
            conn=self._write,
        )
        extrinsic = self._call(
            "create_signed_extrinsic",
            call=call,
            keypair=self.keypair,
            conn=self._write,
        )
        return self._call(
            "submit_extrinsic", extrinsic, wait_for_inclusion=True, conn=self._write
        )

    def submit_bond(self, amount: int) -> tuple[bool, Optional[str]]:
        """Submit `OrinqReceipts.bond(amount)` as a signed extrinsic.

        Returns `(True, tx_hash_hex)` on successful inclusion, `(False, None)`
        otherwise. Raises on unrecoverable exceptions — callers should wrap in
        try/except to avoid killing daemon startup on transient RPC errors.
        """
        call = self._call(
            "compose_call",
            call_module="OrinqReceipts",
            call_function="bond",
            call_params={"amount": amount},
            conn=self._write,
        )
        extrinsic = self._call(
            "create_signed_extrinsic",
            call=call,
            keypair=self.keypair,
            conn=self._write,
        )
        receipt = self._call("submit_extrinsic", extrinsic, wait_for_inclusion=True, conn=self._write)
        if receipt.is_success:
            logger.info(
                f"Bond of {amount} base units posted successfully, "
                f"block {receipt.block_hash}"
            )
            return True, getattr(receipt, "extrinsic_hash", None) or str(receipt.block_hash)
        else:
            logger.error(f"bond({amount}) failed: {receipt.error_message}")
            return False, None

    # --- Intent-settlement helpers (task #266) --------------------------------
    # These power `daemon.settle_claim_attestor` — the Cardano-tx-confirmed
    # attestation type. They're read/write helpers against
    # `pallet-intent-settlement` storage and extrinsics. Pure RPC; no Cardano
    # I/O happens here (that's in `daemon.settle_claim_attestor.CardanoTxObserver`).

    def list_pending_settlement_requests(self) -> list:
        """Enumerate `IntentSettlement::ClaimSettlementRequests` storage and
        return one dict per pending row, augmented with the corresponding
        `Vouchers[claim_id]` chain-state voucher_digest.

        Returns a list of plain dicts (NOT `PendingSettlementRequest`
        dataclasses — kept loose so the attestor module can be tested
        without import-coupling the schema). The dispatcher converts to
        a dataclass on consumption.

        Missing voucher rows are SILENTLY DROPPED here — the pallet
        invariant is that a settle request can only exist for a
        Vouchered claim, so a request without a voucher is a chain bug
        and the attestor's refusal logic would just produce a noisy
        VOUCHER_DIGEST_MISMATCH. Dropping keeps the noise floor low.

        Output shape per row:
            {
              "claim_id": bytes32,
              "requester": ss58_str,
              "submitted_block": int,
              "settled_direct": bool,
              "cardano_tx_hash": bytes32,
              "observed_at_depth": int,
              "observed_slot": int,
              "beneficiary_addr_hash": bytes28,
              "amount_lovelace": int,
              "mainchain_genesis_hash": bytes32,
              "voucher_digest": bytes32,           # from on-chain Vouchers[claim_id]
              "bond_amount": int,                  # task #84 slash trigger (u128, 0 on pre-#84 rows)
            }

        ``bond_amount`` is sourced from the ``SettlementRequestRecord``
        struct directly — post-spec-225 the record carries this
        u128 field (LAST in the struct, so old 4-field records decode
        as ``bond_amount=0`` via the additive SCALE migration). The
        slash watcher dispatcher gates on ``bond_amount > 0``;
        unbonded rows are skipped before the L1 observation pass.
        """
        out: list[dict] = []
        # A failed read raises: callers must not take it for "no requests"
        # and forget what they track per request.
        rows = self._call(
            "query_map",
            module="IntentSettlement",
            storage_function="ClaimSettlementRequests",
            conn=self._read,
        )
        for key, value in rows:
            try:
                claim_id_b = _to_bytes32(key.value)
            except (ValueError, TypeError):
                continue
            row = self._settlement_request_row(claim_id_b, value)
            if row is not None:
                out.append(row)
        return out

    def get_settlement_request(self, claim_id: bytes) -> Optional[dict]:
        """``ClaimSettlementRequests[claim_id]`` in the row shape of
        :meth:`list_pending_settlement_requests`, or None when no request is
        pending (or its voucher is gone).

        Read at the write conn's best block: callers reread a request under
        the chain-write lock right before a supersede or slash goes to that
        conn's node, and a lagging read conn could still show a request that
        node has replaced."""
        value = self._call(
            "query",
            module="IntentSettlement",
            storage_function="ClaimSettlementRequests",
            params=["0x" + claim_id.hex()],
            conn=self._write,
        )
        if value is None or value.value is None:
            return None
        return self._settlement_request_row(claim_id, value)

    def _settlement_request_row(self, claim_id_b: bytes, value: Any) -> Optional[dict]:
        record = value.value if hasattr(value, "value") else value
        if not isinstance(record, dict):
            return None
        evidence = record.get("evidence") or {}
        if not isinstance(evidence, dict):
            return None
        voucher_digest = self.get_voucher_digest(claim_id_b)
        if voucher_digest is None:
            logger.info(
                f"settle_request {claim_id_b.hex()[:16]}... has no "
                f"matching voucher — dropping (chain invariant "
                f"violation, see pallet docs)"
            )
            return None
        try:
            return {
                "claim_id": claim_id_b,
                "requester": str(record.get("requester")),
                "submitted_block": int(record.get("submitted_block", 0)),
                "settled_direct": bool(record.get("settled_direct", False)),
                "cardano_tx_hash": _to_bytes32(
                    evidence.get("cardano_tx_hash")
                ),
                "observed_at_depth": int(
                    evidence.get("observed_at_depth", 0)
                ),
                "observed_slot": int(evidence.get("observed_slot", 0)),
                "beneficiary_addr_hash": _to_bytes_exact(
                    evidence.get("beneficiary_addr_hash"), 28
                ),
                "amount_lovelace": int(
                    evidence.get("amount_lovelace", 0)
                ),
                "mainchain_genesis_hash": _to_bytes32(
                    evidence.get("mainchain_genesis_hash")
                ),
                "voucher_digest": voucher_digest,
                # Task #84-watcher: surface the bond amount so the
                # slash watcher can gate on `bond_amount > 0`.
                # Pre-spec-225 rows decode as 0 via the additive
                # SCALE migration baked into spec-225.
                "bond_amount": int(record.get("bond_amount", 0) or 0),
            }
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(
                f"list_pending_settlement_requests: malformed row "
                f"for claim {claim_id_b.hex()[:16]}...: "
                f"{type(e).__name__}: {e}"
            )
            return None

    def get_chain_identity(self) -> Optional[ChainIdentity]:
        """Assemble the :class:`ChainIdentity` a voucher digest binds.

        ``materios_chain_id`` is ``chain_getBlockHash(0)``, the id the outer
        STCA payload and the slash watcher already sign over. The runtime's
        ``IntentSettlement.MateriosChainId`` reads ``System.BlockHash(0)``,
        which holds :data:`BLOCK_ZERO_PLACEHOLDER` until block 1, and
        substrate-interface caches metadata per runtime version: a daemon
        that first reads it at block 0 keeps the placeholder until the next
        runtime upgrade. The constant is therefore only a cross-check,
        skipped when it is absent or the placeholder.

        Returns ``None`` if NetworkMagic, AegisPolicyV1ScriptHash or
        SettlementVersion is unreadable (a runtime without
        ``pallet-intent-settlement``). Raises :class:`ChainIdRefused` when
        the genesis hash is malformed, the placeholder or zero, or differs
        from the runtime's ``MateriosChainId``. Callers MUST NOT fabricate a
        default: a wrong chain id produces wrong-domain digests and every
        attest_settle fails with ``InvalidSignature``.

        Read on every call, so a chain reset is picked up at once; the
        constants decode from already-cached metadata.
        """
        try:
            chain_id_const = self._call(
                "get_constant", "IntentSettlement", "MateriosChainId", conn=self._read
            )
            network_magic_const = self._call(
                "get_constant", "IntentSettlement", "NetworkMagic", conn=self._read
            )
            script_hash_const = self._call(
                "get_constant", "IntentSettlement", "AegisPolicyV1ScriptHash", conn=self._read
            )
            settlement_version_const = self._call(
                "get_constant", "IntentSettlement", "SettlementVersion", conn=self._read
            )
        except Exception as e:  # noqa: BLE001
            logger.info(
                f"get_chain_identity: get_constant raised "
                f"{type(e).__name__}: {e}; skipping derive-from-state"
            )
            return None
        consts = (
            network_magic_const,
            script_hash_const,
            settlement_version_const,
        )
        if any(c is None for c in consts):
            return None
        try:
            runtime_chain_id = (
                None if chain_id_const is None
                else _to_bytes_exact(getattr(chain_id_const, "value", chain_id_const), 32)
            )
            network_magic_raw = int(
                getattr(network_magic_const, "value", network_magic_const)
            )
            script_hash_raw = _to_bytes_exact(
                getattr(script_hash_const, "value", script_hash_const), 28
            )
            settlement_version_raw = int(
                getattr(
                    settlement_version_const,
                    "value",
                    settlement_version_const,
                )
            )
        except (ValueError, TypeError) as e:
            logger.warning(
                f"get_chain_identity: failed to decode constants: "
                f"{type(e).__name__}: {e}"
            )
            return None
        genesis = self.get_genesis_hash()
        try:
            chain_id = _to_bytes_exact(genesis, 32)
        except (ValueError, TypeError) as e:
            raise ChainIdRefused(
                f"chain_getBlockHash(0) returned {genesis!r}, not a 32-byte hash"
            ) from e
        if chain_id in (bytes(32), BLOCK_ZERO_PLACEHOLDER):
            raise ChainIdRefused(
                f"chain_getBlockHash(0) returned the unusable chain id 0x{chain_id.hex()}"
            )
        if runtime_chain_id not in (None, BLOCK_ZERO_PLACEHOLDER, chain_id):
            raise ChainIdRefused(
                f"runtime MateriosChainId 0x{runtime_chain_id.hex()} differs from "
                f"chain_getBlockHash(0) 0x{chain_id.hex()}"
            )
        try:
            return ChainIdentity(
                materios_chain_id=chain_id,
                network_magic=network_magic_raw,
                aegis_policy_script_hash=script_hash_raw,
                settlement_version=settlement_version_raw,
            )
        except ValueError as e:
            logger.warning(f"get_chain_identity: invalid constant values: {e}")
            return None

    def get_voucher_digest(self, claim_id: bytes) -> Optional[bytes]:
        """Return the chain-state voucher_digest for ``claim_id`` (the
        binding committed in the STCA preimage, memo §3.2).

        Resolution order:

        1. **Preferred** — dedicated ``IntentSettlement::VoucherDigests``
           storage map. Doesn't exist in the current runtime; kept
           ahead of the fallback for forward-compat with a future
           pallet revision that publishes the digest as a separate
           value.
        2. **Preferred** — ``voucher_digest`` field on the
           ``Vouchers[claim_id]`` row. Doesn't exist either today; same
           forward-compat reason.
        3. **Live fallback (task #278)** — read the full
           ``Vouchers[claim_id]`` row + the chain identity
           (:meth:`get_chain_identity`), then recompute the digest
           via :func:`daemon.voucher_canonicalize.compute_voucher_digest_with_address`.
           This is the only path that actually returns a value on the
           current chain. Byte-for-byte mirrors the pallet's
           ``compute_canonical_voucher_digest`` — the pinned parity
           vector in ``tests/test_voucher_canonicalize.py`` guards
           drift.

        Returns ``None`` when:
          - No voucher row exists for ``claim_id``.
          - The voucher row exists but the chain-identity constants
            aren't readable (e.g. older spec without
            ``pallet-intent-settlement``).
          - The beneficiary address isn't a CIP-0019 type-0 form
            (v1 vouchers don't support enterprise/script addresses).

        ``None`` causes the dispatcher to drop the settle request
        rather than sign a wrong-domain digest — falsifiability is the
        whole point of the STCA preimage (memo §2.4).

        Raises :class:`ChainIdRefused` from :meth:`get_chain_identity`.
        """
        claim_param = "0x" + claim_id.hex()
        # Path 1: dedicated VoucherDigests map (forward-compat).
        try:
            result = self._call(
                "query",
                module="IntentSettlement",
                storage_function="VoucherDigests",
                params=[claim_param],
                conn=self._read,
            )
            if result is not None and result.value is not None:
                try:
                    digest = _to_bytes32(result.value)
                    logger.debug(
                        f"get_voucher_digest: VoucherDigests hit for "
                        f"claim={claim_id.hex()[:16]}..."
                    )
                    return digest
                except (ValueError, TypeError):
                    pass
        except SubstrateRequestException:
            # No dedicated map on this runtime — fall through.
            pass
        except Exception:  # noqa: BLE001
            pass

        # Read Voucher row once; both path-2 (voucher_digest field) and
        # path-3 (derive-from-state) consume it.
        try:
            result = self._call(
                "query",
                module="IntentSettlement",
                storage_function="Vouchers",
                params=[claim_param],
                conn=self._read,
            )
        except SubstrateRequestException as e:
            logger.warning(
                f"get_voucher_digest: Vouchers query raised "
                f"{type(e).__name__}: {e}"
            )
            return None
        except Exception:  # noqa: BLE001
            return None
        if result is None or result.value is None:
            return None
        v = result.value
        if not isinstance(v, dict):
            return None

        # Path 2: explicit voucher_digest field on the Voucher row
        # (forward-compat). batch_fairness_proof_digest is deliberately
        # NOT treated as an alias — it's a different domain hash.
        vd = v.get("voucher_digest")
        if vd is not None:
            try:
                digest = _to_bytes32(vd)
                logger.debug(
                    f"get_voucher_digest: voucher_digest field hit for "
                    f"claim={claim_id.hex()[:16]}..."
                )
                return digest
            except (ValueError, TypeError):
                pass

        # Path 3 (task #278): derive byte-for-byte via the canonicalizer.
        chain_identity = self.get_chain_identity()
        if chain_identity is None:
            logger.warning(
                f"get_voucher_digest: chain-identity constants "
                f"unreadable; cannot derive digest for "
                f"claim={claim_id.hex()[:16]}..."
            )
            return None
        try:
            voucher_fields = _extract_voucher_fields_for_digest(v)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(
                f"get_voucher_digest: voucher row malformed for "
                f"claim={claim_id.hex()[:16]}...: "
                f"{type(e).__name__}: {e}"
            )
            return None
        try:
            digest = compute_voucher_digest_with_address(
                chain_identity=chain_identity,
                claim_id=claim_id,
                policy_id=voucher_fields["policy_id"],
                beneficiary_cardano_addr_raw=voucher_fields[
                    "beneficiary_cardano_addr"
                ],
                amount_ada=voucher_fields["amount_ada"],
                bfpr_digest=voucher_fields["batch_fairness_proof_digest"],
                issued_block=voucher_fields["issued_block"],
                expiry_slot_cardano=voucher_fields["expiry_slot_cardano"],
            )
        except AddressDecodeError as e:
            # Non-type-0 beneficiary address — v1 vouchers only support
            # type-0. Don't fabricate a digest; drop the request.
            logger.warning(
                f"get_voucher_digest: beneficiary address not type-0 for "
                f"claim={claim_id.hex()[:16]}...: {e}"
            )
            return None
        except ValueError as e:
            logger.warning(
                f"get_voucher_digest: derive failed for "
                f"claim={claim_id.hex()[:16]}...: {e}"
            )
            return None
        logger.debug(
            f"get_voucher_digest: derived from chain state for "
            f"claim={claim_id.hex()[:16]}... → "
            f"digest=0x{digest.hex()[:16]}..."
        )
        return digest

    def get_voucher(self, claim_id: bytes) -> Optional[dict]:
        """Return a slimmed-down voucher row for cert-daemon refusal
        cross-checks. Only the fields the attestor compares against
        evidence are surfaced; the rest of the row is ignored.

        Returns None if no voucher row exists.
        """
        claim_param = "0x" + claim_id.hex()
        try:
            result = self._call(
                "query",
                module="IntentSettlement",
                storage_function="Vouchers",
                params=[claim_param],
                conn=self._read,
            )
        except (SubstrateRequestException, Exception):  # noqa: BLE001
            return None
        if result is None or result.value is None:
            return None
        v = result.value
        if not isinstance(v, dict):
            return None
        out: dict = {}
        if "amount_ada" in v:
            try:
                out["amount_lovelace"] = int(v["amount_ada"])
            except (TypeError, ValueError):
                pass
        if "beneficiary_addr_blake2_224" in v:
            try:
                out["beneficiary_addr_blake2_224"] = _to_bytes_exact(
                    v["beneficiary_addr_blake2_224"], 28
                )
            except (ValueError, TypeError):
                pass
        elif "beneficiary_cardano_addr" in v:
            # The payment-key hash request_settle and supersede check: a
            # 57-byte type-0 address, header 0x01. Any other shape is left
            # out rather than guessed.
            try:
                raw = _to_bytes_exact(v["beneficiary_cardano_addr"], 57)
                out["beneficiary_addr_blake2_224"] = split_type0_address_bytes(raw)[0]
            except (ValueError, TypeError, AddressDecodeError) as e:
                logger.warning(
                    f"voucher {claim_id.hex()[:16]}... beneficiary is not a "
                    f"type-0 address ({type(e).__name__}); leaving it out"
                )
        return out

    def get_min_finality_depth(self) -> Optional[int]:
        """Return the runtime constant `IntentSettlement::MinFinalityDepth`.

        Falls back to None on any error — caller should use its env
        default.
        """
        try:
            consts = self._call(
                "get_constant", "IntentSettlement", "MinFinalityDepth", conn=self._read
            )
            if consts is None:
                return None
            val = getattr(consts, "value", consts)
            return int(val) if val is not None else None
        except Exception:  # noqa: BLE001
            return None

    def submit_attest_settle(
        self,
        claim_id: bytes,
        my_pubkey: bytes,
        my_sig: bytes,
    ) -> Optional[str]:
        """Submit ``IntentSettlement::attest_settle(claim_id, [(my_pubkey,
        my_sig)])`` with a 1-sig envelope.

        DEPRECATED for the daemon's autopilot path (task #286). The pallet's
        ``ensure_threshold_signatures`` does NOT accumulate sigs across
        calls — it requires M sigs in ONE envelope. A 1-sig submit against
        a chain with ``MinSignerThreshold >= 2`` always returns
        ``InsufficientSignatures``. Use ``submit_attest_settle_envelope``
        instead, paired with the gateway-mediated aggregator.

        Kept for back-compat with single-threshold test chains
        (``MinSignerThreshold == 1``).
        """
        return self.submit_attest_settle_envelope(claim_id, [(my_pubkey, my_sig)])

    def _submit_settlement_call(self, function: str, params: dict, subject: str) -> Optional[str]:
        """Compose, sign and submit ``IntentSettlement::<function>``. Returns the
        extrinsic hash on inclusion. A dispatch the runtime refuses returns None
        at once: its invariant errors (``SettlementRequestMissing``,
        ``AlreadySettled``, ``FraudProofInvalid`` …) do not change on retry, and
        the caller retries on a later tick. Transport errors retry up to
        ``tx_max_retries`` times."""
        last_error: Optional[str] = None
        for attempt in range(self.config.tx_max_retries):
            try:
                call = self._call(
                    "compose_call",
                    call_module="IntentSettlement",
                    call_function=function,
                    call_params=params,
                    conn=self._write,
                )
                extrinsic = self._call(
                    "create_signed_extrinsic",
                    call=call, keypair=self.keypair, conn=self._write,
                )
                receipt = self._call(
                    "submit_extrinsic",
                    extrinsic, wait_for_inclusion=True, conn=self._write,
                )
                if not receipt.is_success:
                    logger.warning(f"{function} failed for {subject}: {receipt.error_message}")
                    return None
                ext_hash = (
                    getattr(receipt, "extrinsic_hash", None)
                    or getattr(receipt, "block_hash", None)
                    or ""
                )
                return str(ext_hash) if ext_hash else None
            except SubstrateRequestException as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(f"{function} attempt {attempt + 1} raised: {e}")
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"{function} unexpected error for {subject}: {type(e).__name__}: {e}")
                return None
        logger.error(
            f"{function} gave up after {self.config.tx_max_retries} "
            f"attempts for {subject}: {last_error}"
        )
        return None

    # --- Multi-sig envelope variants (task #286 — autopilot path) ------------
    # The pallet's `ensure_threshold_signatures` requires the full M-sig
    # bundle in ONE call. Each cert-daemon assembles the bundle via the
    # gateway-mediated `multisig_aggregator` and then calls one of these
    # envelope methods. The 1-sig methods above remain only for
    # `MinSignerThreshold == 1` test chains.

    def submit_attest_settle_envelope(
        self,
        claim_id: bytes,
        sigs: list,  # list[tuple[bytes, bytes]]: (pubkey, sig)
    ) -> Optional[str]:
        """Submit ``IntentSettlement::attest_settle(claim_id, sigs)`` with
        an M-sig envelope. ``sigs`` is ``[(pubkey, sig), ...]``; the caller
        is responsible for assembling, deduping, and sorting the entries
        (the gateway aggregator does this).

        The caller's own pubkey MUST appear in the envelope or the pallet's
        caller-binding check rejects with ``InsufficientSignatures``. The
        aggregator's ``assemble_envelope`` guarantees this.

        Returns the extrinsic hash (0x-prefixed hex) on inclusion, None on
        any non-success (including ``InsufficientSignatures`` if the caller
        passes too few sigs — that's treated as terminal for the tick;
        retry happens on the next poll if peer sigs land later).
        """
        if len(claim_id) != 32:
            raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
        return self._submit_settlement_call(
            "attest_settle",
            {"claim_id": "0x" + claim_id.hex(), "signatures": _signature_params(sigs)},
            f"{claim_id.hex()[:16]}... (sigs={len(sigs)})",
        )

    def submit_attest_expire_policy_envelope(
        self,
        intent_id: bytes,
        sigs: list,  # list[tuple[bytes, bytes]]
    ) -> Optional[str]:
        """Sister to ``submit_attest_settle_envelope`` for the spec-221
        attested-expire path. Same envelope contract."""
        if len(intent_id) != 32:
            raise ValueError(f"intent_id must be 32 bytes, got {len(intent_id)}")
        return self._submit_settlement_call(
            "attest_expire_policy",
            {"intent_id": "0x" + intent_id.hex(), "signatures": _signature_params(sigs)},
            f"{intent_id.hex()[:16]}... (sigs={len(sigs)})",
        )

    # --- Slash helpers (task #84-watcher) -----------------------------------
    # spec-225 ships `slash_bad_settlement_evidence(claim_id, fraud_proof,
    # signatures)` (call_index 20 in pallet-intent-settlement::lib.rs). The
    # daemon-side slash_watcher module assembles the M-of-N FRAU/FRV2 envelope
    # via the gateway aggregator and calls this helper to dispatch the
    # extrinsic. Encoding maps the daemon's typed FraudProof variant
    # (WrongAmount / TxNotFound / WrongBeneficiary) into the SCALE-encoded
    # enum shape substrate-interface expects.

    def submit_slash_bad_settlement_evidence(
        self,
        claim_id: bytes,
        fraud_proof: Any,
        sigs: list,  # list[tuple[bytes, bytes]]: (pubkey, sig)
    ) -> Optional[str]:
        """Submit ``IntentSettlement::slash_bad_settlement_evidence(
        claim_id, fraud_proof, signatures)``.

        ``fraud_proof`` is the daemon's typed FraudProof dataclass from
        :mod:`daemon.slash_watcher` (one of ``WrongAmount(actual_lovelace)``,
        ``TxNotFound()``, or ``WrongBeneficiary(actual_payment_hash)``).
        It's converted here to the substrate-interface variant-dict shape
        (``{"WrongAmount": {"actual_lovelace": int}}`` etc) — the encoder
        on the wire matches the byte-exact form
        :func:`daemon.slash_watcher.encode_fraud_proof` produces (both
        digest builders use the latter, so the signatures bind to the
        same bytes the runtime decodes).

        ``sigs`` is ``[(pubkey, sig), ...]``; the caller (the slash
        watcher) is responsible for assembling, deduping, and sorting
        the entries. The caller's own pubkey MUST appear in the
        envelope or the pallet's caller-binding check rejects with
        ``InsufficientSignatures``.

        Returns the extrinsic hash on inclusion, None on any
        non-success (including ``FraudThresholdNotMet`` /
        ``InsufficientSignatures`` if too few sigs, or
        ``FraudProofInvalid`` if the daemon's classifier got
        out-of-step with chain state — the latter is a daemon bug
        worth investigating in logs, but it's not retryable so we
        return None for either).
        """
        # Lazy import keeps the slash_watcher module optional at
        # daemon import-time. Older deploys without spec-225 wired
        # can still load substrate_client without forcing the slash
        # module into the import graph.
        from daemon.slash_watcher import (
            TxNotFound,
            WrongAmount,
            WrongBeneficiary,
        )
        if len(claim_id) != 32:
            raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
        sigs_param = _signature_params(sigs)
        # Map the typed FraudProof to substrate-interface's enum-dict
        # form. Variant names match the Rust enum (types.rs lines
        # 448-464) — substrate-interface re-encodes them from the
        # runtime metadata so the on-the-wire bytes match exactly
        # what the pallet's FraudProof::decode expects.
        if isinstance(fraud_proof, WrongAmount):
            fraud_proof_param = {
                "WrongAmount": {"actual_lovelace": int(fraud_proof.actual_lovelace)},
            }
        elif isinstance(fraud_proof, TxNotFound):
            fraud_proof_param = "TxNotFound"
        elif isinstance(fraud_proof, WrongBeneficiary):
            if len(fraud_proof.actual_payment_hash) != 28:
                raise ValueError(
                    f"WrongBeneficiary.actual_payment_hash must be 28 "
                    f"bytes, got {len(fraud_proof.actual_payment_hash)}"
                )
            fraud_proof_param = {
                "WrongBeneficiary": {
                    "actual_payment_hash": "0x"
                    + bytes(fraud_proof.actual_payment_hash).hex(),
                },
            }
        else:
            raise TypeError(
                f"submit_slash_bad_settlement_evidence: unknown "
                f"FraudProof variant {type(fraud_proof).__name__}"
            )
        return self._submit_settlement_call(
            "slash_bad_settlement_evidence",
            {
                "claim_id": "0x" + claim_id.hex(),
                "fraud_proof": fraud_proof_param,
                "signatures": sigs_param,
            },
            f"{claim_id.hex()[:16]}... (sigs={len(sigs)})",
        )

    def submit_supersede_settlement_request(
        self,
        claim_id: bytes,
        settled_direct: bool,
        evidence: Any,
        sigs: list,  # list[tuple[bytes, bytes]]: (pubkey, sig)
    ) -> Optional[str]:
        """Submit ``IntentSettlement::supersede_settlement_request(claim_id,
        settled_direct, evidence, signatures)`` (spec 242, call index 22).

        ``evidence`` is a :class:`daemon.settle_claim_attestor.SettlementEvidence`
        and ``sigs`` the STCA envelope over it, which must include this
        client's own key. Returns the extrinsic hash on inclusion, None when
        the runtime refuses it.
        """
        if len(claim_id) != 32:
            raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
        return self._submit_settlement_call(
            "supersede_settlement_request",
            {
                "claim_id": "0x" + claim_id.hex(),
                "settled_direct": bool(settled_direct),
                "evidence": {
                    "cardano_tx_hash": "0x" + _to_bytes_exact(evidence.cardano_tx_hash, 32).hex(),
                    "observed_at_depth": int(evidence.observed_at_depth),
                    "observed_slot": int(evidence.observed_slot),
                    "beneficiary_addr_hash": "0x" + _to_bytes_exact(evidence.beneficiary_addr_hash, 28).hex(),
                    "amount_lovelace": int(evidence.amount_lovelace),
                    "mainchain_genesis_hash": "0x" + _to_bytes_exact(evidence.mainchain_genesis_hash, 32).hex(),
                },
                "signatures": _signature_params(sigs),
            },
            f"{claim_id.hex()[:16]}... (sigs={len(sigs)})",
        )

    def get_min_signer_threshold(self) -> int:
        """Read ``IntentSettlement::MinSignerThreshold`` (governance-set;
        0 = use ``DefaultMinSignerThreshold``). Returns the effective
        threshold (storage value if >0, else 2 which is the pallet default).
        Used by daemon attestors to know how many envelope sigs to assemble
        before submitting via the multi-sig envelope methods."""
        try:
            stored = self._call(
                "query", "IntentSettlement", "MinSignerThreshold", conn=self._read
            ).value
            stored_int = int(stored or 0)
            return stored_int if stored_int > 0 else 2
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"get_min_signer_threshold: query failed ({type(e).__name__}: {e}); "
                f"falling back to 2"
            )
            return 2

    # --- Expire-policy helpers (task #284 / spec-221) ------------------------
    # These power `daemon.expire_policy_attestor` — the second M-of-N
    # attested expire path (PR #34). They mirror the settle helpers shape
    # for shape (read pending row, fetch chain-state field, submit attest
    # with a single sig) so an auditor can grep both blocks side by side
    # and confirm the same contract.

    def list_pending_expiry_requests(self) -> list:
        """Enumerate `IntentSettlement::PolicyExpireRequests` storage and
        return one dict per pending row. Mirrors
        :meth:`list_pending_settlement_requests` for shape.

        Returns a list of plain dicts (NOT `PendingExpiryRequest`
        dataclasses — kept loose so the attestor module can be tested
        without import-coupling the schema). The dispatcher converts to
        a dataclass on consumption.

        Output shape per row:
            {
              "intent_id": bytes32,
              "requester": ss58_str,
              "submitted_block": int,
              "cardano_tx_hash": bytes32,
              "observed_at_depth": int,
              "observed_slot": int,
              "mainchain_genesis_hash": bytes32,
              "policy_id_witness": bytes32,
            }
        """
        out: list[dict] = []
        try:
            rows = self._call(
                "query_map",
                module="IntentSettlement",
                storage_function="PolicyExpireRequests",
                conn=self._read,
            )
        except SubstrateRequestException as e:
            logger.warning(
                f"list_pending_expiry_requests: query_map raised "
                f"{type(e).__name__}: {e}"
            )
            return out
        except Exception as e:  # noqa: BLE001
            # Older runtime versions without the pallet wired return a
            # "module not found" — that's a soft-disable, not an error.
            # Same shape as list_pending_settlement_requests.
            logger.info(
                f"list_pending_expiry_requests: no IntentSettlement "
                f"module on chain ({type(e).__name__}: {e}); skipping tick"
            )
            return out
        for key, value in rows:
            try:
                intent_id_b = _to_bytes32(key.value)
            except (ValueError, TypeError):
                continue
            record = value.value if hasattr(value, "value") else value
            if not isinstance(record, dict):
                continue
            evidence = record.get("evidence") or {}
            if not isinstance(evidence, dict):
                continue
            try:
                out.append({
                    "intent_id": intent_id_b,
                    "requester": str(record.get("requester")),
                    "submitted_block": int(record.get("submitted_block", 0)),
                    "cardano_tx_hash": _to_bytes32(
                        evidence.get("cardano_tx_hash")
                    ),
                    "observed_at_depth": int(
                        evidence.get("observed_at_depth", 0)
                    ),
                    "observed_slot": int(evidence.get("observed_slot", 0)),
                    "mainchain_genesis_hash": _to_bytes32(
                        evidence.get("mainchain_genesis_hash")
                    ),
                    "policy_id_witness": _to_bytes32(
                        evidence.get("policy_id_witness")
                    ),
                })
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(
                    f"list_pending_expiry_requests: malformed row "
                    f"for intent {intent_id_b.hex()[:16]}...: "
                    f"{type(e).__name__}: {e}"
                )
                continue
        return out

    def get_intent_status(self, intent_id: bytes) -> Optional[str]:
        """Return the `IntentStatus` for an on-chain intent as a string
        (one of: ``"Pending"``, ``"Attested"``, ``"Vouchered"``,
        ``"Settled"``, ``"Expired"``, ``"Refunded"``).

        Returns ``None`` when the intent row is absent — the
        ``expire_policy_attestor`` treats that as ``INTENT_NOT_FOUND``
        and refuses to sign.

        substrate-interface decodes a SCALE enum as either a bare string
        (variant name) or a dict ``{"VariantName": null}``. We normalize
        both shapes to a plain string.
        """
        intent_param = "0x" + intent_id.hex()
        try:
            result = self._call(
                "query",
                module="IntentSettlement",
                storage_function="Intents",
                params=[intent_param],
                conn=self._read,
            )
        except SubstrateRequestException as e:
            logger.warning(
                f"get_intent_status: Intents query raised "
                f"{type(e).__name__}: {e}"
            )
            return None
        except Exception:  # noqa: BLE001
            return None
        if result is None or result.value is None:
            return None
        intent = result.value
        if not isinstance(intent, dict):
            return None
        status = intent.get("status")
        if status is None:
            return None
        if isinstance(status, str):
            return status
        if isinstance(status, dict):
            # SCALE-decoded enum variant is sometimes shaped as
            # {"VariantName": None}; first key wins.
            keys = list(status.keys())
            if keys:
                return str(keys[0])
        # Numeric encoding (rare); map to enum order
        # 0=Pending, 1=Attested, 2=Vouchered, 3=Settled, 4=Expired, 5=Refunded.
        if isinstance(status, int):
            mapping = {
                0: "Pending",
                1: "Attested",
                2: "Vouchered",
                3: "Settled",
                4: "Expired",
                5: "Refunded",
            }
            return mapping.get(status)
        return None

    def get_policy_id_for_intent(self, intent_id: bytes) -> Optional[bytes]:
        """Return the chain-state-resolved 32-byte policy id for an
        on-chain intent, mirroring the pallet's
        ``Pallet::<T>::resolve_intent_policy_id`` (see
        ``materios-intent-settlement/pallets/intent-settlement/src/lib.rs``).

        Mapping:
          * ``BuyPolicy { product_id, .. }`` → ``product_id`` (the
            product_id IS the policy_id from the Aegis-side perspective).
          * ``RequestPayout { policy_id, .. }`` → ``policy_id``.
          * ``RefundCredit { .. }`` → ``None`` (no Cardano-side policy
            — `expire_policy_attestor` refuses these with
            ``POLICY_ID_WITNESS_MISMATCH`` / detail
            ``intent_kind_has_no_policy_id``).

        Returns ``None`` also when the intent row is absent or the
        decoded ``kind`` field is malformed.
        """
        intent_param = "0x" + intent_id.hex()
        try:
            result = self._call(
                "query",
                module="IntentSettlement",
                storage_function="Intents",
                params=[intent_param],
                conn=self._read,
            )
        except SubstrateRequestException as e:
            logger.warning(
                f"get_policy_id_for_intent: Intents query raised "
                f"{type(e).__name__}: {e}"
            )
            return None
        except Exception:  # noqa: BLE001
            return None
        if result is None or result.value is None:
            return None
        intent = result.value
        if not isinstance(intent, dict):
            return None
        kind = intent.get("kind")
        if kind is None:
            return None
        # substrate-interface decodes a SCALE enum like
        # IntentKind as a dict {"VariantName": <inner_dict>} where the
        # inner dict carries the variant fields.
        if isinstance(kind, dict):
            for variant_name, payload in kind.items():
                if variant_name == "BuyPolicy":
                    if isinstance(payload, dict):
                        product_id = payload.get("product_id")
                        if product_id is not None:
                            try:
                                return _to_bytes32(product_id)
                            except (ValueError, TypeError):
                                return None
                elif variant_name == "RequestPayout":
                    if isinstance(payload, dict):
                        policy_id = payload.get("policy_id")
                        if policy_id is not None:
                            try:
                                return _to_bytes32(policy_id)
                            except (ValueError, TypeError):
                                return None
                elif variant_name == "RefundCredit":
                    # No Cardano-side policy — refund-credit intents
                    # are not expire-able via this path.
                    return None
                # Unknown variant — same effect as "no policy" (the
                # daemon refuses).
                return None
        return None

    def submit_attest_expire_policy(
        self,
        intent_id: bytes,
        my_pubkey: bytes,
        my_sig: bytes,
    ) -> Optional[str]:
        """Submit ``IntentSettlement::attest_expire_policy(intent_id,
        [(my_pubkey, my_sig)])`` with a 1-sig envelope.

        DEPRECATED for the daemon's autopilot path (task #286) — same
        reasoning as ``submit_attest_settle``. Use
        ``submit_attest_expire_policy_envelope`` paired with the gateway
        aggregator for chains where ``MinSignerThreshold >= 2``.
        """
        return self.submit_attest_expire_policy_envelope(intent_id, [(my_pubkey, my_sig)])

    def get_block_events(self, block_number: int) -> list:
        block_hash = self._call("get_block_hash", block_number, conn=self._read)
        events = self._call("get_events", block_hash=block_hash, conn=self._read)
        receipt_events = []
        for event in events:
            if (event.value["module_id"] == "OrinqReceipts" and
                event.value["event_id"] == "ReceiptSubmitted"):
                attrs = event.value["attributes"]
                receipt_events.append({
                    "receipt_id": attrs["receipt_id"],
                    "content_hash": attrs["content_hash"],
                    "submitter": attrs["submitter"],
                })
        return receipt_events

    def get_block_certified_events(self, block_number: int) -> list:
        """Scan a block for AvailabilityCertified events."""
        block_hash = self._call("get_block_hash", block_number, conn=self._read)
        events = self._call("get_events", block_hash=block_hash, conn=self._read)
        certified = []
        for event in events:
            if (event.value["module_id"] == "OrinqReceipts" and
                event.value["event_id"] == "AvailabilityCertified"):
                attrs = event.value["attributes"]
                certified.append({
                    "receipt_id": attrs["receipt_id"],
                    "cert_hash": attrs["cert_hash"],
                })
        return certified

    def get_receipt(self, receipt_id: str) -> Optional[ReceiptRecord]:
        """The receipt at the read conn's best block."""
        result = self._call(
            "query",
            module="OrinqReceipts",
            storage_function="Receipts",
            params=[receipt_id],
            conn=self._read,
        )
        if result.value is None:
            return None
        r = result.value
        return ReceiptRecord(
            receipt_id=receipt_id,
            content_hash=_to_bytes32(r["content_hash"]),
            base_root_sha256=_to_bytes32(r["base_root_sha256"]),
            storage_locator_hash=_to_bytes32(r["storage_locator_hash"]),
            schema_hash=_to_bytes32(r["schema_hash"]),
            base_manifest_hash=_to_bytes32(r["base_manifest_hash"]),
            safety_manifest_hash=_to_bytes32(r["safety_manifest_hash"]),
            monitor_config_hash=_to_bytes32(r["monitor_config_hash"]),
            attestation_evidence_hash=_to_bytes32(r["attestation_evidence_hash"]),
            zk_root_poseidon=_to_bytes32(r["zk_root_poseidon"]) if r.get("zk_root_poseidon") else None,
            poseidon_params_hash=_to_bytes32(r["poseidon_params_hash"]) if r.get("poseidon_params_hash") else None,
            availability_cert_hash=_to_bytes32(r["availability_cert_hash"]),
            created_at_millis=r["created_at_millis"],
            submitter=str(r["submitter"]),
        )

    def submit_availability_cert(
        self, receipt_id: str, cert_hash: bytes
    ) -> SubmitCertOutcome:
        """Submit `attest_availability_cert` directly (no Sudo).

        Returns a `SubmitCertOutcome` describing the verdict. Post-spec-219
        the runtime returns `Ok(())` on cert-hash mismatch (so strike + slash
        writes persist), so we MUST scan `triggered_events` for our signer's
        `BadAttestStrike` / `AutoSlashedForBadAttest` markers — `is_success`
        alone would falsely report acceptance and silently rack up strikes
        until auto-ejection.

        A refused attest is settled by :meth:`attestation_on_chain`: done when
        the attest landed through a reply the socket lost, or other members
        certified the receipt under the same hash first; a failure when the
        chain can never count it; otherwise :class:`AttestUnsettled`
        propagates, as does a dead socket that ``_call``'s retry did not get
        past, and the caller retries the receipt later.
        """
        last_error: Optional[str] = None
        for attempt in range(self.config.tx_max_retries):
            try:
                call = self._call(
                    "compose_call",
                    call_module="OrinqReceipts",
                    call_function="attest_availability_cert",
                    call_params={
                        "receipt_id": receipt_id,
                        "claimed_hash": list(cert_hash),
                    },
                    conn=self._write,
                )
                extrinsic = self._call(
                    "create_signed_extrinsic",
                    call=call,
                    keypair=self.keypair,
                    conn=self._write,
                )
                receipt = self._call(
                    "submit_extrinsic", extrinsic, wait_for_inclusion=True, conn=self._write
                )
                if not receipt.is_success:
                    last_error = str(receipt.error_message)
                    logger.error(f"Cert tx failed for {receipt_id}: {last_error}")
                    continue
                # Dispatch landed. Now check whether the runtime accepted our
                # claim or struck us for misattestation.
                strike_info = self._scan_for_bad_attest(receipt, receipt_id)
                if strike_info is not None:
                    return strike_info
                logger.info(f"Cert attested for {receipt_id}, block {receipt.block_hash}")
                return SubmitCertOutcome(success=True)
            except SubstrateRequestException as e:
                settled = self.attestation_on_chain(receipt_id, cert_hash)
                if settled is None:
                    raise AttestUnsettled(receipt_id, cert_hash, e) from e
                if settled:
                    logger.info(
                        f"Cert tx for {receipt_id} refused ({e}); already on chain: the receipt "
                        f"is certified under this hash or carries this signer's attest of it"
                    )
                    return SubmitCertOutcome(success=True, already_on_chain=True)
                logger.error(
                    f"Cert tx for {receipt_id} refused ({e}); the chain can never count it: this "
                    f"signer is not a committee member, or the receipt is certified or attested "
                    f"under another hash"
                )
                return SubmitCertOutcome(success=False, error_message=f"{type(e).__name__}: {e}")
            except Exception as e:
                if is_transport_dead(e):
                    raise
                last_error = f"{type(e).__name__}: {e}"
                logger.error(f"Unexpected error submitting cert for {receipt_id}: {e}")
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
        return SubmitCertOutcome(success=False, error_message=last_error)

    def attestation_on_chain(self, receipt_id: str, cert_hash: Optional[bytes]) -> Optional[bool]:
        """Whether this signer's attest of ``receipt_id`` is settled on chain:
        True once it is done, False when it can never count, None while
        neither. ``cert_hash`` is the hash the signer claimed, None before it
        has claimed one.

        Spec 242's pool refuses as 1010 "Transaction call is not expected" an
        attest by a non-member, of an unknown or certified receipt, of a
        non-canonical hash, or that the receipt already records. It judges at
        its node's best block, so the receipt, the committee and the receipt's
        attestation are read together at one best block of the write conn's
        node: a lagging node, or reads at two blocks, can miss a certification,
        which removes the attestation. The attest is done when the signer is a
        member and the receipt is certified under ``cert_hash``, or records
        this signer's attest under it. It can never count when the signer is
        not a member, or the receipt is certified or attested under another
        hash.

        Callers hold the chain-write lock, so no submit holds the write
        socket while it reads."""
        head = self._call("get_chain_head", conn=self._write)
        members = self._call(
            "query", "OrinqReceipts", "CommitteeMembers", block_hash=head, conn=self._write
        ).value
        if self.keypair.public_key not in _account_ids(members):
            return False
        receipt = self._call(
            "query", "OrinqReceipts", "Receipts", [receipt_id], block_hash=head, conn=self._write
        ).value
        if receipt is None:
            return None
        certified_under = _to_bytes32(receipt["availability_cert_hash"])
        if certified_under != bytes(32):
            return cert_hash is None or cert_hash == certified_under
        entry = self._call(
            "query", "OrinqReceipts", "Attestations", [receipt_id], block_hash=head, conn=self._write
        ).value
        if entry is None:
            return None
        if cert_hash is not None and cert_hash != _to_bytes32(entry[0]):
            return False
        return True if self.keypair.public_key in _account_ids(entry[1]) else None

    def _scan_for_bad_attest(
        self, receipt, receipt_id: str
    ) -> Optional[SubmitCertOutcome]:
        """Return a strike/slash outcome iff the included tx struck OUR signer.

        Two events are relevant (per pallet-orinq-receipts spec-219):
        - `BadAttestStrike { attester, receipt_id, claimed, canonical, strikes }`
          — claim disagreed with runtime's canonical hash; strike persisted.
        - `AutoSlashedForBadAttest { attester, amount, remaining_bond }`
          — strike count crossed threshold; full bond slashed + committee
          ejection. Always co-emitted with the final `BadAttestStrike`.

        Both events carry an `attester: AccountId32`; we compare against
        `self.keypair.ss58_address`. A foreign attester's strike in the
        same block (rare but possible if two extrinsics share an inclusion
        block) MUST be ignored — only our own strike should fail us.
        """
        our_ss58 = self.keypair.ss58_address
        strike: Optional[dict] = None
        slash: Optional[dict] = None
        try:
            triggered = receipt.triggered_events
        except Exception:
            # If substrate-interface failed to decode events we can't be
            # sure — assume Ok dispatch == real success rather than fail
            # closed (which would re-submit and double-strike).
            logger.warning(
                f"Could not enumerate triggered_events for {receipt_id}; "
                "assuming Ok dispatch == accepted attest."
            )
            return None
        for entry in triggered:
            try:
                ev = entry.value.get("event", entry.value)
                if ev.get("module_id") != "OrinqReceipts":
                    continue
                event_id = ev.get("event_id")
                attrs = ev.get("attributes") or {}
                if event_id == "BadAttestStrike" and str(attrs.get("attester")) == our_ss58:
                    strike = attrs
                elif event_id == "AutoSlashedForBadAttest" and str(attrs.get("attester")) == our_ss58:
                    slash = attrs
            except (AttributeError, KeyError, TypeError):
                # Defensive: skip any event we can't decode rather than
                # falsely reporting success or failure on a parse error.
                continue
        if strike is None and slash is None:
            return None
        # Strike fired — surface the verdict structurally + log loudly. The
        # caller (cert_daemon) is responsible for operator-facing Discord +
        # health metrics; we log at error/critical so journalctl alone
        # captures the incident even if Discord is down.
        claimed = _maybe_bytes32(strike.get("claimed") if strike else None)
        canonical = _maybe_bytes32(strike.get("canonical") if strike else None)
        strikes = int(strike.get("strikes", 0)) if strike else 0
        slashed_amount = int(slash.get("amount", 0)) if slash else 0
        if slash is not None:
            logger.critical(
                f"AUTO-SLASHED for bad attest: receipt={receipt_id} "
                f"strikes={strikes} amount={slashed_amount} "
                f"claimed={claimed.hex() if claimed else '?'} "
                f"canonical={canonical.hex() if canonical else '?'} "
                f"— signer ejected from committee, re-bond required."
            )
        else:
            logger.error(
                f"BadAttestStrike for {receipt_id}: strikes={strikes} "
                f"claimed={claimed.hex() if claimed else '?'} "
                f"canonical={canonical.hex() if canonical else '?'} "
                f"— check chain_id / cert_builder inputs."
            )
        return SubmitCertOutcome(
            success=False,
            bad_attest_strike=True,
            auto_slashed=slash is not None,
            strikes=strikes,
            claimed=claimed,
            canonical=canonical,
            slashed_amount=slashed_amount,
        )
