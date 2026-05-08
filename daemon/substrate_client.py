import logging
import socket
import threading
import time
from functools import wraps
from typing import Optional, Callable, Any
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
from websocket import WebSocketException

from daemon.config import DaemonConfig
from daemon.models import ReceiptRecord

logger = logging.getLogger(__name__)


# Errors that mean "the WS layer is broken; reconnect and retry once".
# Don't list SubstrateRequestException here — that's a chain-side error
# (e.g. "no such storage key"); reconnecting wouldn't help and would mask bugs.
_WS_TRANSIENT = (
    socket.timeout,
    WebSocketException,
    ConnectionError,
    BrokenPipeError,
    OSError,
    EOFError,
)


def _to_bytes32(val) -> bytes:
    """Convert SCALE-decoded [u8; 32] to bytes. Handles hex strings, lists, and bytes."""
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return bytes.fromhex(val.removeprefix("0x"))
    if isinstance(val, (list, tuple)):
        return bytes(val)
    return bytes(val)


def _rpc(method: Callable) -> Callable:
    """Decorator: route a SubstrateClient method through `_call_with_retry`.

    Every public RPC method on SubstrateClient is wrapped so that:
      1. socket-level recv timeout fires within `ws_recv_timeout` seconds
         (set via `ws_options={'timeout': N}` on connect),
      2. transient WS errors trigger one reconnect + retry,
      3. `_last_ok_at` is bumped on every success so the `connected`
         property reports honest state to heartbeat/metrics consumers.

    Substrate-side errors (SubstrateRequestException) propagate untouched —
    those are real chain responses and must not be silently retried.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        return self._call_with_retry(method, self, *args, **kwargs)
    return wrapper


class SubstrateClient:
    def __init__(self, config: DaemonConfig):
        self.config = config
        self.substrate: Optional[SubstrateInterface] = None
        self.keypair = Keypair.create_from_uri(config.signer_uri)
        # Serialize all RPC calls + reconnects. substrate-interface internally
        # uses a single WS connection; concurrent callers (poll loop +
        # evidence_submitter + bond/heartbeat helpers) must NOT issue
        # interleaved requests on the same socket — the response queue
        # is keyed by request id and concurrent send/recv pairs race.
        self._lock = threading.RLock()
        # Monotonic timestamp of the last successful RPC. Defaults to 0.0
        # so the freshly-constructed client is reported as NOT connected
        # until the first call lands. (`connect()` immediately bumps it
        # on a successful WS handshake.)
        self._last_ok_at: float = 0.0

    # ─── connection lifecycle ────────────────────────────────────────────

    @property
    def _ws_recv_timeout(self) -> int:
        # Honoured by `websocket.create_connection(timeout=N)` → sets
        # `socket.settimeout(N)` on the underlying TCP socket so blocking
        # `.recv()` raises `socket.timeout` instead of hanging forever.
        return getattr(self.config, "ws_recv_timeout", 30)

    @property
    def _connected_freshness(self) -> int:
        # `connected` returns True only if the most recent successful RPC
        # was within this many seconds. Independent of `ws_recv_timeout`
        # so a long-running call doesn't immediately mark us disconnected.
        return getattr(self.config, "ws_connected_freshness", 90)

    def connect(self) -> bool:
        """(Re-)open the WS connection. Idempotent + safe to call from any
        thread. On success bumps `_last_ok_at` so `connected` flips to True."""
        with self._lock:
            # Tear down a stale connection before opening a new one.
            if self.substrate is not None:
                try:
                    # close() may itself raise on a dead socket; suppress.
                    self.substrate.close()
                except Exception:
                    pass
                self.substrate = None
            try:
                self.substrate = SubstrateInterface(
                    url=self.config.rpc_url,
                    config={"strict_scale_decode": False},
                    ws_options={"timeout": self._ws_recv_timeout},
                )
                # Probe the connection so a half-open socket is caught at
                # connect time, not on first business-logic call.
                _ = self.substrate.chain
                self._last_ok_at = time.monotonic()
                logger.info(
                    f"Connected to {self.config.rpc_url}, chain: {self.substrate.chain} "
                    f"(ws_recv_timeout={self._ws_recv_timeout}s)"
                )
                return True
            except Exception as e:
                logger.error(f"Failed to connect to substrate: {e}")
                self.substrate = None
                return False

    @property
    def connected(self) -> bool:
        """Truthful liveness check.

        Returns True only when the WS object exists AND a successful RPC
        landed within `_connected_freshness` seconds. Avoids the original
        bug where `substrate is not None` reported True even after the
        underlying socket had been silently dead for hours.
        """
        if self.substrate is None:
            return False
        return (time.monotonic() - self._last_ok_at) < self._connected_freshness

    # ─── retry shell ─────────────────────────────────────────────────────

    def _call_with_retry(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run `fn(*args, **kwargs)` with one auto-reconnect retry on a
        WS-transport error. Substrate-side errors propagate untouched."""
        last_exc: Optional[BaseException] = None
        for attempt in (1, 2):
            with self._lock:
                if self.substrate is None:
                    if not self.connect():
                        # Couldn't even open a fresh socket — let the
                        # caller's outer except handle the outage.
                        raise ConnectionError(
                            f"substrate-client: cannot reach {self.config.rpc_url}"
                        )
                try:
                    result = fn(*args, **kwargs)
                    self._last_ok_at = time.monotonic()
                    return result
                except SubstrateRequestException:
                    # Real chain-side error (e.g. unknown storage). The
                    # WS is fine; bumping the freshness stamp is correct.
                    self._last_ok_at = time.monotonic()
                    raise
                except _WS_TRANSIENT as e:
                    last_exc = e
                    logger.warning(
                        f"substrate-client: WS error on attempt {attempt} "
                        f"({type(e).__name__}: {e}); reconnecting"
                    )
                    # Force a reconnect on next iteration.
                    try:
                        if self.substrate is not None:
                            self.substrate.close()
                    except Exception:
                        pass
                    self.substrate = None
                    continue
        # Fell through both attempts.
        raise last_exc if last_exc is not None else ConnectionError(
            "substrate-client: retry shell exhausted"
        )

    # ─── chain-state queries (all WS-wrapped via @_rpc) ──────────────────

    @_rpc
    def get_finalized_head_number(self) -> int:
        head_hash = self.substrate.get_chain_finalised_head()
        header = self.substrate.get_block_header(head_hash)
        return header["header"]["number"]

    @_rpc
    def get_best_block_number(self) -> int:
        header = self.substrate.get_block_header()
        return header["header"]["number"]

    @_rpc
    def get_genesis_hash(self) -> str:
        """Return the chain's genesis hash (0x-prefixed lowercase hex). Used to
        detect that we're pointed at a different chain than we were last run
        (e.g. a chain reset) and self-heal stale daemon state."""
        return self.substrate.get_block_hash(0)

    # --- Bond helpers (OrinqReceipts pallet) --------------------------------
    # These are used by CertDaemon._ensure_bond() to keep the attestor's
    # reserved MATRA at or above `BondRequirement` so `join_committee` doesn't
    # fail with `InsufficientBond`. See README → "Auto-bond on startup".

    @_rpc
    def get_bond_requirement(self) -> int:
        """Return `OrinqReceipts.BondRequirement` in MATRA base units (u128).

        Returns 0 when the storage item is absent (pre-runtime-upgrade chains)
        so `_ensure_bond()` falls through to its "nothing to do" branch.
        """
        result = self.substrate.query("OrinqReceipts", "BondRequirement")
        val = result.value
        if val is None:
            return 0
        return int(val)

    @_rpc
    def get_attestor_bond(self, address: str) -> int:
        """Return `OrinqReceipts.AttestorBonds(address)` in base units.

        Maps to 0 for accounts that have never bonded (the storage value is
        `ValueQuery` so the runtime returns 0 in that case; we defensively
        handle a missing value anyway).
        """
        result = self.substrate.query("OrinqReceipts", "AttestorBonds", [address])
        val = result.value
        if val is None:
            return 0
        return int(val)

    @_rpc
    def get_free_balance(self, address: str) -> int:
        """Return `System.Account(address).data.free` in base units.

        Returns 0 if the account row does not exist (never received MATRA).
        """
        result = self.substrate.query("System", "Account", [address])
        val = result.value
        if val is None:
            return 0
        # substrate-interface decodes this as a dict {"nonce": .., "data": {..}}
        try:
            return int(val["data"]["free"])
        except (KeyError, TypeError):
            return 0

    @_rpc
    def submit_bond(self, amount: int) -> tuple[bool, Optional[str]]:
        """Submit `OrinqReceipts.bond(amount)` as a signed extrinsic.

        Returns `(True, tx_hash_hex)` on successful inclusion, `(False, None)`
        otherwise. Raises on unrecoverable exceptions — callers should wrap in
        try/except to avoid killing daemon startup on transient RPC errors.
        """
        call = self.substrate.compose_call(
            call_module="OrinqReceipts",
            call_function="bond",
            call_params={"amount": amount},
        )
        extrinsic = self.substrate.create_signed_extrinsic(
            call=call,
            keypair=self.keypair,
        )
        receipt = self.substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)
        if receipt.is_success:
            logger.info(
                f"Bond of {amount} base units posted successfully, "
                f"block {receipt.block_hash}"
            )
            return True, getattr(receipt, "extrinsic_hash", None) or str(receipt.block_hash)
        else:
            logger.error(f"bond({amount}) failed: {receipt.error_message}")
            return False, None

    @_rpc
    def get_block_events(self, block_number: int) -> list:
        block_hash = self.substrate.get_block_hash(block_number)
        events = self.substrate.get_events(block_hash=block_hash)
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

    @_rpc
    def get_block_certified_events(self, block_number: int) -> list:
        """Scan a block for AvailabilityCertified events."""
        block_hash = self.substrate.get_block_hash(block_number)
        events = self.substrate.get_events(block_hash=block_hash)
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

    @_rpc
    def get_receipt(self, receipt_id: str) -> Optional[ReceiptRecord]:
        result = self.substrate.query(
            module="OrinqReceipts",
            storage_function="Receipts",
            params=[receipt_id],
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

    def submit_availability_cert(self, receipt_id: str, cert_hash: bytes) -> bool:
        """Submit attest_availability_cert directly (no Sudo). Returns True on finalization.

        Note: this method has its OWN retry loop (over chain-side
        SubstrateRequestException for transient nonce/inclusion races) that
        is distinct from the WS-transport retry. The single-shot inner
        chain call is wrapped via `_call_with_retry` so a WS wedge mid-tx
        triggers exactly one reconnect, but the outer loop still attempts
        `tx_max_retries` chain-side resubmits.
        """
        for attempt in range(self.config.tx_max_retries):
            try:
                return self._call_with_retry(self._submit_cert_inner, receipt_id, cert_hash)
            except SubstrateRequestException as e:
                logger.error(f"Cert tx attempt {attempt + 1} failed for {receipt_id}: {e}")
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error submitting cert for {receipt_id}: {e}")
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
        return False

    def _submit_cert_inner(self, receipt_id: str, cert_hash: bytes) -> bool:
        call = self.substrate.compose_call(
            call_module="OrinqReceipts",
            call_function="attest_availability_cert",
            call_params={
                "receipt_id": receipt_id,
                "cert_hash": list(cert_hash),
            },
        )
        extrinsic = self.substrate.create_signed_extrinsic(
            call=call,
            keypair=self.keypair,
        )
        receipt = self.substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)
        if receipt.is_success:
            logger.info(f"Cert attested for {receipt_id}, block {receipt.block_hash}")
            return True
        logger.error(f"Cert tx failed for {receipt_id}: {receipt.error_message}")
        # Returning False (rather than raising) preserves the original
        # caller contract: True = on-chain success, False = chain rejected.
        return False
