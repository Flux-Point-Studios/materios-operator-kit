"""Cert-daemon module that lifts gateway-stored TEE attestation evidence to
chain via `TeeAttestation.submit_evidence` (task #143).

Why this exists
---------------
The gateway's `POST /v2/attestation_evidence` route accepts evidence from
attestors (Acurast Android phones for Wave 3 Phase 2; SEV-SNP / TDX hosts
for Phase 3.x), validates the signature + nonce binding, and persists the
row in `receipt_attestation_evidence.db`. It does NOT call the on-chain
`TeeAttestation.submit_evidence` extrinsic — without that step the pallet's
`CompositeTrustScores` storage stays at the default 0 forever, the
gateway's `/billing/usage` route surfaces `composite_trust_score: 0` on
every record, and the Phase 2 Path C smoke harness's headline test 4
times out.

This module closes that loop. It polls the gateway's
`GET /v2/attestation_evidence/pending` endpoint (read-only, behind the
shared `SPONSORED_RECEIPT_SUBMITTER_TOKEN` Bearer), composes
`TeeAttestation.submit_evidence(receipt_id, content_hash, EvidenceEntry)`
for each row, signs it with the cert-daemon's existing keypair, submits
via the SHARED substrate-interface session (no second WS connection — see
`feedback_substrate_rpc_cap_exhaustion.md`), and acks the gateway via
`POST /v2/attestation_evidence/:row_id/mark_submitted` on success.

Design choices
--------------
- **Pull model.** The daemon owns its cursor; the gateway's `pending`
  endpoint replies with rows id > cursor. Restart safety + backfill come
  for free. The gateway returns `next_since` so we don't need to compute
  it from row IDs ourselves.
- **Reuse cert-daemon's chain-write lock.** Mirrors the
  `_chain_write_lock` pattern from `cert_daemon.py::process_receipt` so
  parallel submits stay nonce-monotonic. See
  `feedback_polkadot_nonce_race_on_burst.md`.
- **Idempotent attestor registration.** On startup we POST to
  `/admin/attestation-evidence-attestors` with our pubkey. A 200 (created)
  is fine; a 409 (already-registered) is fine. Anything else logs WARN
  and the loop continues — manual ops can reconcile.
- **Per-evidence-type payload assembly.** Only `arm_trustzone` is
  implemented (Phase 2 fully-supported variant). Other evidence types
  are typed-skipped with a WARN log so the cursor still advances; once
  Phase 3+ verifiers ship we add their wire-payload assembly here.
- **Defensive against PalletDisabled.** Spec-214 flipped the kill-switch
  off, but if a future deploy re-enables it the daemon must NOT
  hot-loop on the same row forever. The submitter's `_translate_*` helper
  converts pallet-side errors into a stable taxonomy and we mark
  `PalletDisabled` rows as a no-retry-this-tick failure (we still don't
  ack — operator intervention required).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from typing import Any, Literal, Optional

import aiohttp

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pallet-side payload size cap. PINNED to
# `partnerchain/pallets/tee-attestation/src/types.rs::MAX_EVIDENCE_PAYLOAD_BYTES`.
# Any payload exceeding this cap is rejected by SCALE bounds-check at decode
# time and returns a structurally-bad failure — no point burning the tx fee.
# ---------------------------------------------------------------------------
MAX_EVIDENCE_PAYLOAD_BYTES = 16 * 1024


# ---------------------------------------------------------------------------
# Terminal-error classification.
#
# Maps known pallet-side errors and pre-flight failure reasons into either
# "terminal" (won't succeed on retry — record locally so the cursor can move
# past) or "retryable" (transient — let the next tick try again).
#
# Source of truth for the pallet variants is
# `partnerchain/pallets/tee-attestation/src/lib.rs` (search `pub enum Error`):
#   - PalletDisabled   → kill-switch on; operator may flip back off       (RETRY)
#   - VerificationFailed → verifier rejected the bytes; resubmitting is a
#     no-op + burns weight                                                (TERMINAL)
#   - TooManyEntries   → cap reached for this receipt; further submits
#     for the same row will keep hitting the cap                          (TERMINAL)
#
# Pre-flight failure reasons we synthesise client-side:
#   - PayloadTooLarge       → exceeds MAX_EVIDENCE_PAYLOAD_BYTES           (TERMINAL)
#   - PayloadAssemblyError  → gateway shape couldn't be translated         (TERMINAL)
#   - UnsupportedEvidenceType → no Phase 2 verifier for this variant       (TERMINAL)
#
# `AttestorNotRegistered` is NOT in this map intentionally — the pallet
# doesn't surface that today (registration is gateway-side), but if it
# ever does, leave it retryable so `ensure_registered` re-runs.
# ---------------------------------------------------------------------------
TERMINAL_ERROR_REASONS: frozenset[str] = frozenset({
    "VerificationFailed",
    "TooManyEntries",
    "PayloadTooLarge",
    "PayloadAssemblyError",
    "UnsupportedEvidenceType",
})


def _classify_chain_error(err: Optional[str]) -> Literal["terminal", "retryable"]:
    """Map a pallet-side `error_message` (or a synthesized client-side reason
    label) to either "terminal" or "retryable". Substring match against the
    known terminal reasons; anything else (including PalletDisabled) stays
    retryable.

    The matching is forgiving — substrate-interface stringifies pallet
    errors as JSON-shaped dicts that always include the variant name. A
    plain identifier substring is sufficient.
    """
    if not err:
        return "retryable"
    s = str(err)
    # PalletDisabled is explicitly retryable — operator may flip the
    # kill-switch back off without a chain reset.
    if "PalletDisabled" in s:
        return "retryable"
    for terminal in TERMINAL_ERROR_REASONS:
        if terminal in s:
            return "terminal"
    return "retryable"


# ---------------------------------------------------------------------------
# Evidence-type → pallet `EvidenceType` discriminant.
#
# PINNED to match `pallets/tee-attestation/src/types.rs::EvidenceType`. Indices
# are append-only (per `feedback_pallet_index_shift.md`); same convention as
# the gateway's `EVIDENCE_TYPE_DISCRIMINANT` map in
# `services/blob-gateway/src/schemas/compute_metering_v2.ts`.
#
# The on-wire SCALE encoding of the EvidenceType enum is a single byte equal
# to the discriminant. substrate-interface accepts the variant name (case-
# sensitive) when composing extrinsics from metadata, so we map to the Rust
# variant identifiers below — NOT the snake_case wire labels the gateway
# uses.
# ---------------------------------------------------------------------------
EVIDENCE_TYPE_TO_PALLET_VARIANT: dict[str, str] = {
    "amd_sev_snp": "AmdSevSnp",
    "intel_tdx": "IntelTdx",
    "arm_trustzone": "ArmTrustZone",
    "reproducible_build": "ReproducibleBuild",
    "zk_vm_execution": "ZkVmExecution",
}


def _hex_to_h256_bytes(hex_str: str) -> list[int]:
    """Convert a 64-hex string (with or without 0x prefix) to a list of 32
    integers — substrate-interface's expected wire form for `[u8; 32]`."""
    s = hex_str.lower()
    if s.startswith("0x"):
        s = s[2:]
    if len(s) != 64:
        raise ValueError(f"expected 64 hex chars, got {len(s)}: {hex_str[:16]}…")
    raw = bytes.fromhex(s)
    return list(raw)


def _content_hash_from_receipt_id_via_chain(
    substrate: Any, receipt_id_hex: str
) -> Optional[bytes]:
    """Resolve a 64-hex `receipt_id` back to the receipt's `content_hash` by
    reading `OrinqReceipts.Receipts(receipt_id)` from chain.

    Returns the 32-byte content_hash on success; None if the receipt isn't
    on chain yet (the daemon should wait + retry next tick).

    Why we need this: the pallet's `submit_evidence(receipt_id, content_hash,
    entry)` requires both. The gateway's pending row carries `receipt_id`
    only; the canonical relation is `receipt_id == sha256(content_hash)`,
    so we recover content_hash from the on-chain Receipt struct (NOT by
    re-deriving — that's not invertible).
    """
    try:
        rid = receipt_id_hex if receipt_id_hex.startswith("0x") else "0x" + receipt_id_hex
        result = substrate.query(
            module="OrinqReceipts", storage_function="Receipts", params=[rid]
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
    except Exception as e:  # noqa: BLE001 — broad catch is intentional
        logger.warning(
            f"Failed to resolve content_hash for receipt_id={receipt_id_hex[:16]}…: "
            f"{type(e).__name__}: {e}"
        )
        return None


def _build_arm_trustzone_payload_bytes(
    substrate: Any, payload: dict
) -> bytes:
    """Translate the gateway's `arm_trustzone` payload shape into the
    SCALE-encoded `Vec<Vec<u8>>` byte string the pallet expects.

    Gateway shape (from `_phase2_helpers.build_arm_trustzone_payload`):
        {
          "cert_chain_b64": [<root_b64>, …, <leaf_b64>],
          "device_model": "Pixel-strongbox-test-vector",
          "security_level": "StrongBox",
        }

    Pallet shape (from `pallets/tee-attestation/src/types.rs` / verifier.rs):
        SCALE-encoded `Vec<Vec<u8>>` — list of DER-encoded X.509 certs in
        chain order (root → leaf).

    Implementation: base64-decode each entry, then SCALE-encode the resulting
    list via substrate-interface's `encode_scale("Vec<Bytes>", ...)`.
    """
    chain = payload.get("cert_chain_b64")
    if not isinstance(chain, list) or len(chain) == 0:
        raise ValueError(
            "arm_trustzone payload missing or empty `cert_chain_b64` (must be non-empty list)"
        )
    der_certs: list[bytes] = []
    for i, item in enumerate(chain):
        if not isinstance(item, str):
            raise ValueError(
                f"arm_trustzone cert_chain_b64[{i}] must be a base64 string, got {type(item).__name__}"
            )
        try:
            der_certs.append(base64.b64decode(item, validate=True))
        except Exception as e:
            raise ValueError(
                f"arm_trustzone cert_chain_b64[{i}] is not valid base64: {e}"
            ) from e
    # `Vec<Bytes>` is the substrate-interface alias for `Vec<Vec<u8>>` — each
    # inner `Bytes` is a length-prefixed byte string.
    encoded = substrate.encode_scale(type_string="Vec<Bytes>", value=der_certs)
    if hasattr(encoded, "data"):
        # Older substrate-interface returned a `ScaleBytes`; .data is the raw
        # bytestring without the leading length prefix expected by encode.
        return bytes(encoded.data)
    if isinstance(encoded, (bytes, bytearray)):
        return bytes(encoded)
    # Newer versions return a hex string with leading '0x'.
    s = str(encoded)
    if s.startswith("0x"):
        return bytes.fromhex(s[2:])
    return bytes.fromhex(s)


def _build_evidence_payload_bytes(
    substrate: Any, evidence_type: str, payload: dict
) -> bytes:
    """Dispatch to the per-evidence-type payload builder. Only `arm_trustzone`
    is implemented in Phase 2 (the only fully-supported variant). Other
    types raise NotImplementedError — the caller logs WARN and skips ack.
    """
    if evidence_type == "arm_trustzone":
        return _build_arm_trustzone_payload_bytes(substrate, payload)
    raise NotImplementedError(
        f"evidence_type {evidence_type!r} payload assembly is Phase 3+ — "
        f"verifier in pallet returns NotImplemented for this variant. Skipping."
    )


def _is_pallet_disabled_error(err: Optional[str]) -> bool:
    """Detect the `TeeAttestation.PalletDisabled` error in the receipt's
    error_message. Substrate stringifies it as a JSON-shaped dict including
    the module name + variant name, so a substring check is sufficient.
    """
    if not err:
        return False
    s = str(err)
    return "PalletDisabled" in s or "Disabled" in s


class EvidenceSubmitter:
    """Polls gateway's pending evidence + lifts each row to chain.

    This class is intentionally NOT instantiated by `cert_daemon.py`'s
    `__init__`. The daemon's main loop calls `start()` to schedule a
    background task once substrate is connected, then `stop()` on shutdown.
    Decoupling matches the existing `start_heartbeat_sender()` shape in
    `daemon/heartbeat.py` so the operational footprint is familiar.
    """

    def __init__(
        self,
        config: Any,
        substrate_client: Any,
        chain_write_lock: asyncio.Lock,
        gateway_url: str,
        submitter_token: str,
        admin_token: Optional[str] = None,
        poll_interval: int = 30,
        page_size: int = 50,
        failed_state_path: Optional[str] = None,
    ):
        self.config = config
        self.client = substrate_client
        self._chain_write_lock = chain_write_lock
        self.gateway_url = gateway_url.rstrip("/")
        self._submitter_token = submitter_token
        # Admin token defaults to the same secret — preprod's `daemon-notify`
        # token doubles as the admin token in dev compose. Operators in
        # prod can split them via env (`EVIDENCE_ATTESTOR_ADMIN_TOKEN`).
        self._admin_token = (admin_token or submitter_token).strip()
        self._poll_interval = max(5, int(poll_interval))
        self._page_size = max(1, min(1000, int(page_size)))
        self._cursor = 0
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # Local skip-bit store for rows whose pallet error is structurally
        # terminal (VerificationFailed, TooManyEntries, PayloadTooLarge, …).
        # Without this, the gateway query (`WHERE submitted_to_chain_at IS
        # NULL`) keeps surfacing the row every tick, the daemon resubmits, the
        # pallet rejects again — and each rejection still consumes the
        # declared submit_evidence weight (1B ref_time / 32KB proof) charged
        # as fees. We persist by row id + reason to a JSON file alongside the
        # cert-daemon's existing state files so the skip survives restart.
        self._failed_state_path: str = (
            failed_state_path
            or os.path.join(
                getattr(config, "data_dir", "/data"), "evidence-failed.json"
            )
        )
        self._failed_rows: dict[int, dict] = self._load_failed_rows()

    # -- local terminal-failure tracking -------------------------------------

    def _load_failed_rows(self) -> dict[int, dict]:
        """Load the failed-row map from disk on construction. Tolerates
        missing or malformed files — returns `{}` and keeps moving.
        """
        path = self._failed_state_path
        if not path or not os.path.exists(path):
            return {}
        try:
            with open(path) as f:
                blob = json.load(f)
            entries = blob.get("rows") if isinstance(blob, dict) else blob
            if not isinstance(entries, list):
                return {}
            out: dict[int, dict] = {}
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                rid = entry.get("row_id")
                if rid is None:
                    continue
                try:
                    rid_int = int(rid)
                except (TypeError, ValueError):
                    continue
                out[rid_int] = {
                    "row_id": rid_int,
                    "reason": str(entry.get("reason") or "unknown"),
                    "failed_at": entry.get("failed_at") or "",
                    "receipt_id": entry.get("receipt_id") or "",
                    "evidence_type": entry.get("evidence_type") or "",
                }
            return out
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"evidence_submitter: failed to load failed-row state from "
                f"{path}: {type(e).__name__}: {e}"
            )
            return {}

    def _save_failed_rows(self) -> None:
        """Atomically persist the failed-row map. Called only on mutation."""
        path = self._failed_state_path
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        except OSError:
            # If we can't create the parent dir we still try the write so
            # the underlying error propagates clearly.
            pass
        try:
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(
                    {"rows": list(self._failed_rows.values())},
                    f,
                    sort_keys=True,
                )
            os.replace(tmp, path)
        except Exception as e:  # noqa: BLE001
            logger.error(
                f"evidence_submitter: failed to persist failed-row state to "
                f"{path}: {type(e).__name__}: {e}"
            )

    def _record_failed_row(
        self,
        row_id: int,
        reason: str,
        *,
        receipt_id: str = "",
        evidence_type: str = "",
    ) -> None:
        """Mark a row id as terminally failed so the next tick skips it.

        Why this exists: a pallet `VerificationFailed` (or any other terminal
        error) won't go away on retry — re-submitting the same bytes
        deterministically lands the same rejection. Without skipping locally
        the daemon hot-loops on the row every poll interval, each loop costing
        the declared `submit_evidence` weight (`1B ref_time / 32KB proof`)
        charged as fees.

        We log WARN with row_id + reason + first 64 hex chars of the receipt
        id so an operator can reconcile against the gateway DB by-hand. The
        on-disk record is keyed by row_id.
        """
        if row_id in self._failed_rows:
            return  # already recorded — do not bump failed_at
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self._failed_rows[row_id] = {
            "row_id": int(row_id),
            "reason": reason,
            "failed_at": ts,
            "receipt_id": receipt_id[:64] if receipt_id else "",
            "evidence_type": evidence_type,
        }
        logger.warning(
            f"evidence_submitter: row {row_id} marked TERMINAL "
            f"(reason={reason!r}, receipt_id={receipt_id[:16]}..., "
            f"type={evidence_type or 'n/a'}) — local skip-bit set, "
            f"will not be resubmitted"
        )
        self._save_failed_rows()

    # -- registration on startup ---------------------------------------------

    async def ensure_registered(self) -> bool:
        """POST our pubkey to `/admin/attestation-evidence-attestors`. 200
        (created) and 409 (already-registered) are both success outcomes;
        anything else returns False and the caller logs.
        """
        if not self._admin_token:
            logger.info(
                "evidence_submitter: no admin token; skipping attestor self-registration"
            )
            return False
        pubkey_hex = self.client.keypair.public_key.hex()
        url = f"{self.gateway_url}/admin/attestation-evidence-attestors"
        body = {
            "pubkey": pubkey_hex,
            "label": f"cert-daemon-{self.client.keypair.ss58_address[:12]}",
            "notes": "auto-registered by daemon/evidence_submitter.py",
        }
        headers = {"authorization": f"Bearer {self._admin_token}"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status in (200, 201):
                        logger.info(
                            f"evidence_submitter: attestor pubkey "
                            f"{pubkey_hex[:16]}… registered"
                        )
                        return True
                    if resp.status == 409:
                        logger.info(
                            f"evidence_submitter: attestor pubkey "
                            f"{pubkey_hex[:16]}… already registered"
                        )
                        return True
                    text = await resp.text()
                    logger.warning(
                        f"evidence_submitter: attestor registration HTTP "
                        f"{resp.status}: {text[:200]}"
                    )
                    return False
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"evidence_submitter: registration request failed: "
                f"{type(e).__name__}: {e}"
            )
            return False

    # -- gateway polling -----------------------------------------------------

    async def fetch_pending(self) -> tuple[list[dict], int]:
        """GET /v2/attestation_evidence/pending. Returns `(rows, next_since)`.
        On HTTP failure, logs WARN and returns `([], current_cursor)` so
        the loop continues without advancing.
        """
        url = (
            f"{self.gateway_url}/v2/attestation_evidence/pending"
            f"?since={self._cursor}&limit={self._page_size}"
        )
        headers = {"authorization": f"Bearer {self._submitter_token}"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.warning(
                            f"evidence_submitter: pending HTTP {resp.status}: "
                            f"{text[:200]}"
                        )
                        return [], self._cursor
                    body = await resp.json()
                    rows = body.get("rows") or []
                    next_since = int(body.get("next_since", self._cursor))
                    return rows, next_since
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"evidence_submitter: pending fetch failed: {type(e).__name__}: {e}"
            )
            return [], self._cursor

    async def mark_submitted(self, row_id: int, extrinsic_hash: str) -> bool:
        """POST /v2/attestation_evidence/:row_id/mark_submitted. Returns True
        on either `marked` or `already-marked`. False on HTTP error so the
        caller can retry later — the on-chain state is the source of truth
        in any case.
        """
        url = (
            f"{self.gateway_url}/v2/attestation_evidence/{row_id}/mark_submitted"
        )
        headers = {"authorization": f"Bearer {self._submitter_token}"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json={"chain_extrinsic_hash": extrinsic_hash},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        return True
                    text = await resp.text()
                    logger.warning(
                        f"evidence_submitter: mark_submitted({row_id}) HTTP "
                        f"{resp.status}: {text[:200]}"
                    )
                    return False
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"evidence_submitter: mark_submitted({row_id}) failed: "
                f"{type(e).__name__}: {e}"
            )
            return False

    # -- per-row chain submission --------------------------------------------

    def _compose_submit_evidence_call(
        self, receipt_id_hex: str, content_hash_bytes: bytes,
        evidence_type: str, payload_bytes: bytes,
    ) -> Any:
        """Compose the `TeeAttestation.submit_evidence` call. Pure SCALE +
        substrate-interface plumbing; no I/O.

        receipt_id_hex: 64 hex chars (we 0x-prefix internally).
        content_hash_bytes: 32 raw bytes.
        evidence_type: gateway's snake_case wire label
            (mapped to pallet variant name internally).
        payload_bytes: pre-encoded inner payload bytes
            (e.g. SCALE-encoded `Vec<Vec<u8>>` for arm_trustzone).
        """
        variant = EVIDENCE_TYPE_TO_PALLET_VARIANT.get(evidence_type)
        if variant is None:
            raise ValueError(f"unknown evidence_type: {evidence_type!r}")
        # The pallet's `EvidenceEntry { evidence_type: EvidenceType, payload:
        # BoundedVec<u8, ConstU32<16384>> }` — substrate-interface accepts
        # BoundedVec<u8, …> as a hex string OR a raw bytes value. We pass the
        # SCALE-encoded payload as bytes (NOT hex-prefixed) and rely on the
        # type registry's `Bytes` decoder to handle the bounded length cap.
        receipt_id_param = (
            receipt_id_hex if receipt_id_hex.startswith("0x") else "0x" + receipt_id_hex
        )
        call = self.client.substrate.compose_call(
            call_module="TeeAttestation",
            call_function="submit_evidence",
            call_params={
                "receipt_id": receipt_id_param,
                "content_hash": list(content_hash_bytes),
                "entry": {
                    "evidence_type": variant,
                    # BoundedVec<u8, …> wire form via substrate-interface is
                    # a hex-prefixed string OR a list/bytes of u8 values.
                    "payload": "0x" + payload_bytes.hex(),
                },
            },
        )
        return call

    async def submit_one(self, row: dict) -> Optional[str]:
        """Process one pending row.

        Returns the chain extrinsic hash (hex, 0x-prefixed) on success;
        None on any non-success path. Caller skips the gateway ack on None.

        Terminal failures (pallet `VerificationFailed`, `TooManyEntries`,
        a pre-flight payload-size cap miss, …) are recorded into the local
        skip-bit store via `_record_failed_row` so the row is filtered out
        on the next tick. See `_classify_chain_error` for the taxonomy.
        """
        row_id = int(row["id"])
        receipt_id_hex = str(row["receipt_id"])
        evidence_type = str(row["evidence_type"])
        payload = row.get("payload") or {}

        # 1. Resolve content_hash from chain receipt.
        content_hash = _content_hash_from_receipt_id_via_chain(
            self.client.substrate, receipt_id_hex
        )
        if content_hash is None:
            logger.info(
                f"evidence_submitter: row {row_id} receipt "
                f"{receipt_id_hex[:16]}… not yet on chain — skip this tick"
            )
            return None

        # 2. Translate gateway payload → pallet payload bytes. Both
        # NotImplementedError and shape-validation ValueError are TERMINAL —
        # the gateway row's bytes won't morph into something the pallet
        # accepts on retry, so record the local skip-bit.
        try:
            payload_bytes = _build_evidence_payload_bytes(
                self.client.substrate, evidence_type, payload
            )
        except NotImplementedError as e:
            logger.warning(
                f"evidence_submitter: row {row_id} {evidence_type} unsupported: {e}"
            )
            self._record_failed_row(
                row_id,
                "UnsupportedEvidenceType",
                receipt_id=receipt_id_hex,
                evidence_type=evidence_type,
            )
            return None
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"evidence_submitter: row {row_id} payload assembly failed: "
                f"{type(e).__name__}: {e}"
            )
            self._record_failed_row(
                row_id,
                "PayloadAssemblyError",
                receipt_id=receipt_id_hex,
                evidence_type=evidence_type,
            )
            return None

        # 2b. Pre-flight payload-size check — saves the round-trip + tx fee
        # for rows that exceed the pallet's BoundedVec cap. The pallet
        # would reject these at SCALE-decode time anyway. PINNED to
        # MAX_EVIDENCE_PAYLOAD_BYTES (16 KiB).
        if len(payload_bytes) > MAX_EVIDENCE_PAYLOAD_BYTES:
            logger.warning(
                f"evidence_submitter: row {row_id} payload {len(payload_bytes)}B "
                f"exceeds cap {MAX_EVIDENCE_PAYLOAD_BYTES}B — terminal"
            )
            self._record_failed_row(
                row_id,
                "PayloadTooLarge",
                receipt_id=receipt_id_hex,
                evidence_type=evidence_type,
            )
            return None

        # 3. Compose + sign + submit. Hold the chain-write lock for the full
        # nonce + sign + submit triplet (per
        # feedback_polkadot_nonce_race_on_burst.md).
        async with self._chain_write_lock:
            try:
                call = self._compose_submit_evidence_call(
                    receipt_id_hex, content_hash, evidence_type, payload_bytes
                )
                extrinsic = self.client.substrate.create_signed_extrinsic(
                    call=call, keypair=self.client.keypair
                )
                receipt = await asyncio.to_thread(
                    self.client.substrate.submit_extrinsic,
                    extrinsic,
                    True,  # wait_for_inclusion
                )
            except Exception as e:  # noqa: BLE001
                # Compose/submit raise on transport / nonce / encoding
                # failures. These are usually transient (RPC hiccup, txpool
                # full, etc.) — keep them retryable.
                logger.warning(
                    f"evidence_submitter: row {row_id} compose/submit raised: "
                    f"{type(e).__name__}: {e}"
                )
                return None

        if not getattr(receipt, "is_success", False):
            err = getattr(receipt, "error_message", None)
            classification = _classify_chain_error(err)
            if classification == "terminal":
                # `VerificationFailed`, `TooManyEntries`, … — resubmitting will
                # land the same rejection and burn fees. Record locally so the
                # next tick filters this row out before composing.
                logger.warning(
                    f"evidence_submitter: row {row_id} TERMINAL pallet error "
                    f"{err!r} — recording local skip-bit "
                    f"(receipt_id={receipt_id_hex[:16]}..., "
                    f"type={evidence_type})"
                )
                # Try to extract a stable variant name for the reason field.
                reason = "VerificationFailed"
                if err and "TooManyEntries" in str(err):
                    reason = "TooManyEntries"
                self._record_failed_row(
                    row_id,
                    reason,
                    receipt_id=receipt_id_hex,
                    evidence_type=evidence_type,
                )
            elif _is_pallet_disabled_error(err):
                logger.warning(
                    f"evidence_submitter: row {row_id} dispatched while "
                    f"pallet kill-switch ON ({err}) — leaving NOT-acked + "
                    f"NOT-skipped (retryable), operator must flip "
                    f"TeeAttestation.Disabled=false"
                )
            else:
                logger.warning(
                    f"evidence_submitter: row {row_id} submit_evidence failed "
                    f"(retryable): {err}"
                )
            return None

        # Read both fields independently so we can detect the all-empty case.
        ext_hash_attr = getattr(receipt, "extrinsic_hash", None)
        block_hash_attr = getattr(receipt, "block_hash", None)
        ext_hash_str = str(ext_hash_attr) if ext_hash_attr else ""
        block_hash_str = str(block_hash_attr) if block_hash_attr else ""
        ext_hash = ext_hash_str or block_hash_str
        if not ext_hash:
            # P2 #6: do NOT silently fall back to an all-zero placeholder. The
            # gateway uses `chain_extrinsic_hash` for forensics + idempotency;
            # writing zeros has burned us before (see
            # `feedback_v2_contract_drift_chain_break.md`). Treat as a
            # retryable miss — the chain submission may already have landed
            # but we couldn't extract the hash. The next tick will see the
            # row again (gateway query is `submitted_to_chain_at IS NULL`)
            # and retry. The pallet's `submit_evidence` is idempotent at the
            # `(receipt_id, attest_key_hash)` level so a duplicate landing
            # is safe.
            logger.error(
                f"evidence_submitter: row {row_id} submit_evidence reported "
                f"is_success but receipt has no extrinsic_hash or block_hash "
                f"(receipt={receipt!r}) — refusing to ack with zero "
                f"placeholder, will retry on next tick"
            )
            return None
        logger.info(
            f"evidence_submitter: row {row_id} submit_evidence OK "
            f"(receipt_id={receipt_id_hex[:16]}…, type={evidence_type}, "
            f"ext_hash={str(ext_hash)[:18]}…)"
        )
        return ext_hash

    # -- main loop -----------------------------------------------------------

    async def _tick(self) -> None:
        """Single poll tick: fetch pending, submit each, ack each.

        Cursor advancement (post-P1-fix). The gateway query is
        `WHERE id > ? AND submitted_to_chain_at IS NULL`. Old logic advanced
        the cursor to `next_since` whenever ANY row in the batch acked,
        which permanently filtered earlier-failed rows out of the daemon's
        view (cursor=0, rows=[10,11], 10 fails + 11 succeeds → cursor=11
        → row 10 stranded). The fix: track per-row outcomes and clamp the
        cursor to `min(next_since, first_unacked_id - 1)`.

        Also: rows already in the local terminal-failure store
        (`_failed_rows`) are skipped before any chain work, so they don't
        keep burning weight. Their ids ARE treated as "acked" for cursor
        purposes — we want the cursor to step over them since the next tick
        would skip them again anyway.
        """
        rows, next_since = await self.fetch_pending()
        if not rows:
            return

        # Filter out any rows we've already classified as terminally failed.
        # Their gateway state (`submitted_to_chain_at IS NULL`) means they'll
        # keep coming back forever; the local skip-bit is the only thing
        # keeping us from re-burning fees on them.
        eligible: list[dict] = []
        skipped_terminal: list[int] = []
        for r in rows:
            try:
                rid = int(r["id"])
            except (KeyError, TypeError, ValueError):
                continue
            if rid in self._failed_rows:
                skipped_terminal.append(rid)
                continue
            eligible.append(r)
        if skipped_terminal:
            logger.info(
                f"evidence_submitter: skipping {len(skipped_terminal)} "
                f"row(s) flagged terminal locally: {skipped_terminal[:8]}"
                + ("..." if len(skipped_terminal) > 8 else "")
            )
        if not eligible and not skipped_terminal:
            return

        logger.info(
            f"evidence_submitter: {len(eligible)} eligible "
            f"({len(skipped_terminal)} skipped) of {len(rows)} pending row(s)"
        )

        # Per-row outcome: True = "may advance past" (acked OR locally
        # marked terminal — both mean the gateway query won't re-surface a
        # workable row), False = "must NOT advance past" (hash returned but
        # ack failed, or chain submit returned None for a retryable reason
        # and the row remains pending in the gateway).
        outcomes: list[tuple[int, bool]] = []
        # Pre-record terminal-skips as advanceable so they don't anchor the
        # cursor. They're still in `_failed_rows` and will be filtered again
        # next tick — but we don't want them to permanently stall the cursor.
        for rid in skipped_terminal:
            outcomes.append((rid, True))

        for row in eligible:
            if not self._running:
                break
            row_id = int(row["id"])
            ext_hash = await self.submit_one(row)
            if ext_hash is not None:
                acked = await self.mark_submitted(row_id, ext_hash)
                outcomes.append((row_id, bool(acked)))
            else:
                # submit_one returned None. Either it recorded the row as
                # locally terminal (in which case it's now in `_failed_rows`
                # and we treat it as advanceable), OR it's a retryable miss
                # we want the cursor to stop short of.
                if row_id in self._failed_rows:
                    outcomes.append((row_id, True))
                else:
                    outcomes.append((row_id, False))

        if not outcomes:
            return

        # Find the earliest row id we could NOT advance past — that's the
        # cursor's hard ceiling for this tick. P1 fix: previously we keyed
        # off the LAST ack only, which let earlier-row failures slip through.
        unadvanceable = [rid for (rid, ok) in outcomes if not ok]
        if unadvanceable:
            first_unacked = min(unadvanceable)
            new_cursor = first_unacked - 1
        else:
            # Every row was acked or terminal-skipped. We can advance to the
            # max of the row ids we processed AND the gateway's `next_since`
            # (whichever is larger).
            highest_processed = max(rid for (rid, _) in outcomes)
            new_cursor = max(highest_processed, next_since)

        # Cursor is monotonically non-decreasing — never go backwards even if
        # the gateway re-surfaces an older row id (e.g. retroactive insert
        # during testing). Going backwards would just re-emit work we've
        # already either acked or terminally-failed.
        self._cursor = max(self._cursor, new_cursor)

    async def _run_forever(self) -> None:
        """Outer loop. Continues until `stop()` is called. Catches per-tick
        exceptions so the daemon doesn't crash on one bad tick.
        """
        # Best-effort registration on startup. Failures are logged inside
        # ensure_registered; we don't bail because operators may pre-
        # register the attestor manually.
        try:
            await self.ensure_registered()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"evidence_submitter: ensure_registered raised: "
                f"{type(e).__name__}: {e}"
            )
        while self._running:
            try:
                await self._tick()
            except Exception as e:  # noqa: BLE001
                logger.error(
                    f"evidence_submitter: tick error: {type(e).__name__}: {e}",
                    exc_info=True,
                )
            try:
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break

    def start(self) -> None:
        """Schedule the background loop on the running event loop. Idempotent
        — calling twice does NOT start two loops.
        """
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        logger.info(
            f"evidence_submitter: started "
            f"(poll_interval={self._poll_interval}s, "
            f"page_size={self._page_size}, gateway={self.gateway_url})"
        )

    def stop(self) -> None:
        """Signal the loop to exit on its next sleep boundary."""
        self._running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()


# ---------------------------------------------------------------------------
# Factory: build the submitter from env vars.
# ---------------------------------------------------------------------------

def maybe_create_evidence_submitter(
    config: Any, substrate_client: Any, chain_write_lock: asyncio.Lock
) -> Optional[EvidenceSubmitter]:
    """Construct an `EvidenceSubmitter` if the required env vars are set;
    return None otherwise (older deploys / nodes that aren't acting as
    evidence submitters).

    Required env vars:
      EVIDENCE_SUBMITTER_GATEWAY_URL  — base URL (e.g. https://materios.../preprod-blobs)
                                        falls back to BLOB_GATEWAY_URL → config.blob_base_url.
      EVIDENCE_SUBMITTER_TOKEN        — `SPONSORED_RECEIPT_SUBMITTER_TOKEN` shared secret.
                                        falls back to SPONSORED_RECEIPT_SUBMITTER_TOKEN env.

    Optional:
      EVIDENCE_SUBMITTER_ADMIN_TOKEN  — admin token for /admin/attestation-evidence-attestors.
                                        Defaults to EVIDENCE_SUBMITTER_TOKEN.
      EVIDENCE_SUBMITTER_POLL_INTERVAL — seconds between polls (default 30).
      EVIDENCE_SUBMITTER_PAGE_SIZE     — max rows per poll (default 50, max 1000).

    A missing gateway URL or token is a soft-disable — the daemon logs INFO
    and continues without the submitter. This keeps existing operator
    deployments backward-compatible until they opt in.
    """
    gateway_url = (
        os.environ.get("EVIDENCE_SUBMITTER_GATEWAY_URL")
        or os.environ.get("BLOB_GATEWAY_URL")
        or getattr(config, "blob_base_url", "")
        or getattr(config, "blob_gateway_url", "")
    ).strip()
    submitter_token = (
        os.environ.get("EVIDENCE_SUBMITTER_TOKEN")
        or os.environ.get("SPONSORED_RECEIPT_SUBMITTER_TOKEN")
        or ""
    ).strip()
    if not gateway_url or not submitter_token:
        logger.info(
            "evidence_submitter: not configured "
            f"(gateway_url={'set' if gateway_url else 'missing'}, "
            f"submitter_token={'set' if submitter_token else 'missing'}) "
            f"— evidence pipeline will not lift to chain on this node."
        )
        return None
    admin_token = (
        os.environ.get("EVIDENCE_SUBMITTER_ADMIN_TOKEN") or submitter_token
    ).strip()
    try:
        poll_interval = int(os.environ.get("EVIDENCE_SUBMITTER_POLL_INTERVAL", "30"))
    except ValueError:
        poll_interval = 30
    try:
        page_size = int(os.environ.get("EVIDENCE_SUBMITTER_PAGE_SIZE", "50"))
    except ValueError:
        page_size = 50
    failed_state_path = (
        os.environ.get("EVIDENCE_SUBMITTER_FAILED_STATE_PATH") or None
    )
    return EvidenceSubmitter(
        config=config,
        substrate_client=substrate_client,
        chain_write_lock=chain_write_lock,
        gateway_url=gateway_url,
        submitter_token=submitter_token,
        admin_token=admin_token,
        poll_interval=poll_interval,
        page_size=page_size,
        failed_state_path=failed_state_path,
    )
