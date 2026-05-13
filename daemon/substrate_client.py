import logging
import time
from dataclasses import dataclass
from typing import Optional
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException

from daemon.config import DaemonConfig
from daemon.models import ReceiptRecord

logger = logging.getLogger(__name__)


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
    """
    success: bool
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


class SubstrateClient:
    def __init__(self, config: DaemonConfig):
        self.config = config
        self.substrate: Optional[SubstrateInterface] = None
        self.keypair = Keypair.create_from_uri(config.signer_uri)

    def connect(self) -> bool:
        try:
            self.substrate = SubstrateInterface(url=self.config.rpc_url, config={'strict_scale_decode': False})
            logger.info(f"Connected to {self.config.rpc_url}, chain: {self.substrate.chain}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to substrate: {e}")
            self.substrate = None
            return False

    @property
    def connected(self) -> bool:
        return self.substrate is not None

    def get_finalized_head_number(self) -> int:
        head_hash = self.substrate.get_chain_finalised_head()
        header = self.substrate.get_block_header(head_hash)
        return header["header"]["number"]

    def get_best_block_number(self) -> int:
        header = self.substrate.get_block_header()
        return header["header"]["number"]

    def get_genesis_hash(self) -> str:
        """Return the chain's genesis hash (0x-prefixed lowercase hex). Used to
        detect that we're pointed at a different chain than we were last run
        (e.g. a chain reset) and self-heal stale daemon state."""
        return self.substrate.get_block_hash(0)

    # --- Bond helpers (OrinqReceipts pallet) --------------------------------
    # These are used by CertDaemon._ensure_bond() to keep the attestor's
    # reserved MATRA at or above `BondRequirement` so `join_committee` doesn't
    # fail with `InsufficientBond`. See README → "Auto-bond on startup".

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
        """
        last_error: Optional[str] = None
        for attempt in range(self.config.tx_max_retries):
            try:
                call = self.substrate.compose_call(
                    call_module="OrinqReceipts",
                    call_function="attest_availability_cert",
                    call_params={
                        "receipt_id": receipt_id,
                        "claimed_hash": list(cert_hash),
                    },
                )
                extrinsic = self.substrate.create_signed_extrinsic(
                    call=call,
                    keypair=self.keypair,
                )
                receipt = self.substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)
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
                last_error = f"{type(e).__name__}: {e}"
                logger.error(f"Cert tx attempt {attempt + 1} failed for {receipt_id}: {e}")
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.error(f"Unexpected error submitting cert for {receipt_id}: {e}")
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
        return SubmitCertOutcome(success=False, error_message=last_error)

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
