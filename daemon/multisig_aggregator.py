"""Gateway-mediated M-of-N signature aggregator for pallet-intent-settlement.

`pallet-intent-settlement::ensure_threshold_signatures` requires the
`attest_settle` / `attest_expire_policy` extrinsic carry ONE envelope
with M sigs. There is no cross-call accumulation. Each cert-daemon used
to submit a 1-sig envelope and the pallet rejected every call with
`InsufficientSignatures` (task #286).

This module talks to the blob-gateway's `/v2/multisig_sigs/{kind}/{key}`
endpoints. Each daemon:

  1. computes the canonical digest (STCA for settle, EXPP for expire)
     from chain state,
  2. signs it with its committee key → ``(pubkey, sig)``,
  3. POSTs the tuple here,
  4. GETs the union of peer sigs filtered to its locally-computed
     digest,
  5. builds an M-sig envelope and submits ``attest_settle`` /
     ``attest_expire_policy`` once.

Trust model: each sig is self-authenticating (gateway runs sr25519Verify
before storing) so junk can't poison the bulletin board. The pallet
remains the source of truth — it re-checks every sig + committee
membership at submit-time. The gateway is "best effort", not load-
bearing for security; if it's down, the daemon falls back to the
existing 1-sig path (a no-op until manual aggregation runs).
"""

from __future__ import annotations
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# Wire-form `kind` values accepted by the gateway. Keep in sync with
# `services/blob-gateway/src/multisig_sigs_store.ts::MULTISIG_KINDS`.
KIND_SETTLE = "settle"
KIND_EXPIRE = "expire"


class MultisigAggregator:
    """Async client for the gateway-mediated sig bulletin board.

    Hold one instance per daemon (settle + expire share the same
    aggregator — they use distinct `kind` values to keep the bulletin-
    board namespaces disjoint).

    Sessions: methods take an ``aiohttp.ClientSession`` per call to match
    the existing attestor pattern (each ``_process_one_locked`` opens its
    own short-lived session). Aggregator is therefore session-agnostic;
    callers just hand it the session they already have.
    """

    def __init__(
        self,
        *,
        gateway_url: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        # Strip trailing slash so we can join with `/v2/...` cleanly.
        self.base_url = gateway_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    def _url(self, kind: str, key: bytes) -> str:
        if kind not in (KIND_SETTLE, KIND_EXPIRE):
            raise ValueError(f"kind must be {KIND_SETTLE!r} or {KIND_EXPIRE!r}, got {kind!r}")
        if len(key) != 32:
            raise ValueError(f"key must be 32 bytes, got {len(key)}")
        return f"{self.base_url}/v2/multisig_sigs/{kind}/{key.hex()}"

    async def share_sig(
        self,
        session: aiohttp.ClientSession,
        *,
        kind: str,
        key: bytes,
        digest: bytes,
        pubkey: bytes,
        sig: bytes,
    ) -> bool:
        """POST one (pubkey, sig, digest) triple. Returns True on 200.

        Any transport / 4xx / 5xx returns False — the caller treats it
        as a transient failure and retries on the next tick. The pallet
        still requires the envelope at submit-time so a lost POST just
        defers settlement, never causes a wrong outcome.
        """
        if len(digest) != 32:
            raise ValueError(f"digest must be 32 bytes, got {len(digest)}")
        if len(pubkey) != 32:
            raise ValueError(f"pubkey must be 32 bytes, got {len(pubkey)}")
        if len(sig) != 64:
            raise ValueError(f"sig must be 64 bytes, got {len(sig)}")
        url = self._url(kind, key)
        payload = {
            "pubkey": pubkey.hex(),
            "sig": sig.hex(),
            "digest": digest.hex(),
        }
        try:
            async with session.post(url, json=payload, timeout=self._timeout) as resp:
                if resp.status == 200:
                    return True
                # 401 = invalid sig (won't recover); 4xx = our bug; 5xx = gateway problem.
                body_preview = (await resp.text())[:200]
                logger.warning(
                    f"multisig_aggregator: share_sig HTTP {resp.status} "
                    f"for {kind}/{key.hex()[:16]}…: {body_preview}"
                )
                return False
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"multisig_aggregator: share_sig transport error "
                f"for {kind}/{key.hex()[:16]}…: {type(e).__name__}: {e}"
            )
            return False

    async def fetch_envelope(
        self,
        session: aiohttp.ClientSession,
        *,
        kind: str,
        key: bytes,
        digest: bytes,
    ) -> list[tuple[bytes, bytes]]:
        """GET peer sigs filtered to entries matching `digest`. Returns
        [(pubkey_bytes, sig_bytes), ...] deduped by pubkey.

        Empty list on any error — the caller treats that as "no peer
        sigs yet" and skips the submit, retrying next tick.
        """
        if len(digest) != 32:
            raise ValueError(f"digest must be 32 bytes, got {len(digest)}")
        url = self._url(kind, key)
        params = {"digest": digest.hex()}
        try:
            async with session.get(url, params=params, timeout=self._timeout) as resp:
                if resp.status != 200:
                    body_preview = (await resp.text())[:200]
                    logger.warning(
                        f"multisig_aggregator: fetch_envelope HTTP {resp.status} "
                        f"for {kind}/{key.hex()[:16]}…: {body_preview}"
                    )
                    return []
                data = await resp.json()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"multisig_aggregator: fetch_envelope transport error "
                f"for {kind}/{key.hex()[:16]}…: {type(e).__name__}: {e}"
            )
            return []
        sigs_field = data.get("sigs") if isinstance(data, dict) else None
        if not isinstance(sigs_field, list):
            logger.warning(
                f"multisig_aggregator: fetch_envelope unexpected shape "
                f"for {kind}/{key.hex()[:16]}…: {type(sigs_field).__name__}"
            )
            return []
        seen: set[bytes] = set()
        out: list[tuple[bytes, bytes]] = []
        for entry in sigs_field:
            if not isinstance(entry, dict):
                continue
            pub_hex = entry.get("pubkey")
            sig_hex = entry.get("sig")
            dig_hex = entry.get("digest")
            if not (isinstance(pub_hex, str) and isinstance(sig_hex, str)
                    and isinstance(dig_hex, str)):
                continue
            # Defense-in-depth: even though we filtered server-side, only
            # trust rows whose digest matches what we asked for.
            if dig_hex.lower() != digest.hex():
                continue
            try:
                pub_b = bytes.fromhex(pub_hex)
                sig_b = bytes.fromhex(sig_hex)
            except ValueError:
                continue
            if len(pub_b) != 32 or len(sig_b) != 64:
                continue
            if pub_b in seen:
                continue
            seen.add(pub_b)
            out.append((pub_b, sig_b))
        return out

    async def assemble_envelope(
        self,
        session: aiohttp.ClientSession,
        *,
        kind: str,
        key: bytes,
        digest: bytes,
        my_pubkey: bytes,
        my_sig: bytes,
    ) -> list[tuple[bytes, bytes]]:
        """Convenience: POST our own sig, GET peer sigs, fold our own
        entry in (in case the gateway is temporarily inconsistent),
        dedupe by pubkey, sort by pubkey for deterministic envelope.

        Returns the final envelope. Caller checks ``len(envelope) >=
        threshold`` before submitting.
        """
        await self.share_sig(
            session, kind=kind, key=key, digest=digest,
            pubkey=my_pubkey, sig=my_sig,
        )
        peer = await self.fetch_envelope(session, kind=kind, key=key, digest=digest)
        # Always include our own entry — the gateway might not have
        # persisted yet (transport bounce), or the GET might race the
        # POST. Cheap to be defensive.
        seen = {pub for pub, _ in peer}
        if my_pubkey not in seen:
            peer.append((my_pubkey, my_sig))
        # Deterministic order: pallet rejects on `DuplicateSigner` already
        # (we deduped by pubkey above) but a canonical ordering also helps
        # peer daemons that re-derive the same envelope land on the same
        # extrinsic hash.
        peer.sort(key=lambda kv: kv[0])
        return peer

    def find_my_sig(
        self,
        envelope: list[tuple[bytes, bytes]],
        my_pubkey: bytes,
    ) -> Optional[bytes]:
        """Return our own sig from a fetched envelope. Useful for sanity-
        checking that our POST actually landed (envelope contains my
        pubkey) before submitting the extrinsic — if it doesn't, the
        pallet will reject with `InsufficientSignatures` because the
        caller-binding check requires the on-chain origin's pubkey to
        appear in the bundle.
        """
        for pub, sig in envelope:
            if pub == my_pubkey:
                return sig
        return None
