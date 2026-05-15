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
            }
        """
        out: list[dict] = []
        try:
            rows = self.substrate.query_map(
                module="IntentSettlement",
                storage_function="ClaimSettlementRequests",
            )
        except SubstrateRequestException as e:
            logger.warning(
                f"list_pending_settlement_requests: query_map raised "
                f"{type(e).__name__}: {e}"
            )
            return out
        except Exception as e:  # noqa: BLE001
            # Older runtime versions without the pallet wired return a
            # "module not found" — that's a soft-disable, not an error.
            logger.info(
                f"list_pending_settlement_requests: no IntentSettlement "
                f"module on chain ({type(e).__name__}: {e}); skipping tick"
            )
            return out
        for key, value in rows:
            try:
                claim_id_b = _to_bytes32(key.value)
            except (ValueError, TypeError):
                continue
            record = value.value if hasattr(value, "value") else value
            if not isinstance(record, dict):
                continue
            evidence = record.get("evidence") or {}
            if not isinstance(evidence, dict):
                continue
            voucher_digest = self.get_voucher_digest(claim_id_b)
            if voucher_digest is None:
                logger.info(
                    f"settle_request {claim_id_b.hex()[:16]}... has no "
                    f"matching voucher — dropping (chain invariant "
                    f"violation, see pallet docs)"
                )
                continue
            try:
                out.append({
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
                })
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(
                    f"list_pending_settlement_requests: malformed row "
                    f"for claim {claim_id_b.hex()[:16]}...: "
                    f"{type(e).__name__}: {e}"
                )
                continue
        return out

    def get_voucher_digest(self, claim_id: bytes) -> Optional[bytes]:
        """Return the chain-state voucher_digest for ``claim_id`` (the
        binding committed in the STCA preimage, memo §3.2).

        The pallet computes this digest at voucher-mint time and stores
        it as part of the ``Voucher`` row (or on a dedicated
        ``VoucherDigests`` map). We try the dedicated map first (the
        zero-RPC-cost path), then fall back to deriving from
        ``Vouchers[claim_id]`` if needed. Returns None when no voucher
        is on chain for this claim.
        """
        claim_param = "0x" + claim_id.hex()
        # Preferred path: dedicated `VoucherDigests` map (most pallets
        # surface the digest separately for cheap reads). If the storage
        # is absent we fall through to the Voucher row.
        try:
            result = self.substrate.query(
                module="IntentSettlement",
                storage_function="VoucherDigests",
                params=[claim_param],
            )
            if result is not None and result.value is not None:
                try:
                    return _to_bytes32(result.value)
                except (ValueError, TypeError):
                    pass
        except SubstrateRequestException:
            # No dedicated map — fall through to Voucher row.
            pass
        except Exception:  # noqa: BLE001
            pass
        # Fallback: read Voucher row + recompute the digest using the
        # pallet's `voucher_digest` field if present.
        try:
            result = self.substrate.query(
                module="IntentSettlement",
                storage_function="Vouchers",
                params=[claim_param],
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
        # A `voucher_digest` field is the canonical place if the
        # pallet exposes it directly.
        vd = v.get("voucher_digest")
        if vd is not None:
            try:
                return _to_bytes32(vd)
            except (ValueError, TypeError):
                pass
        # `batch_fairness_proof_digest` is NOT the voucher digest —
        # different field with different semantics. We deliberately do
        # NOT fall back to it: returning a wrong-domain hash here
        # would silently produce mis-bound STCA signatures.
        return None

    def get_voucher(self, claim_id: bytes) -> Optional[dict]:
        """Return a slimmed-down voucher row for cert-daemon refusal
        cross-checks. Only the fields the attestor compares against
        evidence are surfaced; the rest of the row is ignored.

        Returns None if no voucher row exists.
        """
        claim_param = "0x" + claim_id.hex()
        try:
            result = self.substrate.query(
                module="IntentSettlement",
                storage_function="Vouchers",
                params=[claim_param],
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
        # If the pallet stores the raw address but not the hash, the
        # cert-daemon's cross-check skips this voucher field rather
        # than computing a hash that might disagree with the pallet's
        # canonical form. The chain-state voucher digest binding
        # (fact 8) is the authoritative guard; this fact 6
        # cross-check is purely advisory.
        return out

    def get_min_finality_depth(self) -> Optional[int]:
        """Return the runtime constant `IntentSettlement::MinFinalityDepth`.

        Falls back to None on any error — caller should use its env
        default.
        """
        try:
            consts = self.substrate.get_constant(
                "IntentSettlement", "MinFinalityDepth"
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
        """Submit `IntentSettlement::attest_settle(claim_id, [(my_pubkey,
        my_sig)])`. Each attestor calls this with a single-element sig
        list — the pallet accumulates sigs across calls until the
        threshold is reached and the claim settles (memo §3.1).

        Returns the extrinsic hash (0x-prefixed hex) on inclusion, None
        otherwise. Retries on transport failure are bounded by
        ``tx_max_retries`` (same knob receipt-cert path uses).
        """
        if len(claim_id) != 32:
            raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
        if len(my_pubkey) != 32:
            raise ValueError(
                f"my_pubkey must be 32 bytes, got {len(my_pubkey)}"
            )
        if len(my_sig) != 64:
            raise ValueError(f"my_sig must be 64 bytes, got {len(my_sig)}")
        last_error: Optional[str] = None
        for attempt in range(self.config.tx_max_retries):
            try:
                call = self.substrate.compose_call(
                    call_module="IntentSettlement",
                    call_function="attest_settle",
                    call_params={
                        "claim_id": "0x" + claim_id.hex(),
                        "signatures": [
                            (
                                "0x" + my_pubkey.hex(),
                                "0x" + my_sig.hex(),
                            )
                        ],
                    },
                )
                extrinsic = self.substrate.create_signed_extrinsic(
                    call=call, keypair=self.keypair,
                )
                receipt = self.substrate.submit_extrinsic(
                    extrinsic, wait_for_inclusion=True
                )
                if not receipt.is_success:
                    last_error = str(receipt.error_message)
                    logger.warning(
                        f"attest_settle failed for "
                        f"{claim_id.hex()[:16]}...: {last_error}"
                    )
                    # Non-retryable: the pallet's invariant errors
                    # (SettlementRequestMissing, AlreadySettled, etc.)
                    # won't resolve on retry — return None so the
                    # caller treats this attempt as terminal for the
                    # tick.
                    return None
                ext_hash = (
                    getattr(receipt, "extrinsic_hash", None)
                    or getattr(receipt, "block_hash", None)
                    or ""
                )
                return str(ext_hash) if ext_hash else None
            except SubstrateRequestException as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(
                    f"attest_settle attempt {attempt + 1} raised: {e}"
                )
                if attempt < self.config.tx_max_retries - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:  # noqa: BLE001
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(
                    f"attest_settle unexpected error for "
                    f"{claim_id.hex()[:16]}...: {e}"
                )
                return None
        logger.warning(
            f"attest_settle gave up after {self.config.tx_max_retries} "
            f"attempts for {claim_id.hex()[:16]}...: {last_error}"
        )
        return None

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
