#!/usr/bin/env python3
"""Run targeted slash-safety mutants in isolated copies of the test suite."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TESTS = ['tests/test_slash_runtime.py', 'tests/test_frv2_vectors.py', 'tests/test_settle_supersede.py',
         'tests/test_slash_proof.py', 'tests/test_slash_watcher.py', 'tests/test_multisig_aggregator.py',
         'tests/test_settlement_submits.py']
MUTANTS = [
    ('wrong-chain-accepted', 'daemon/substrate_client.py', 'if version["specName"] != "materios":', 'if False:'),
    ('unknown-contract-uses-frau', 'daemon/substrate_client.py', 'if SLASH_PROTOCOLS.get(fingerprint) != expected:', 'if fingerprint in SLASH_PROTOCOLS and SLASH_PROTOCOLS[fingerprint] != expected:'),
    ('tag-skips-contract-pin', 'daemon/substrate_client.py', 'if SLASH_PROTOCOLS.get(fingerprint) != expected:', 'if False:'),
    ('unknown-tag-signs-frv2', 'daemon/substrate_client.py', 'elif tag == b"FRV2":', 'elif tag is not None:'),
    ('tag-ignored', 'daemon/substrate_client.py', 'constants.get("SlashSettlementTag")),', 'None),'),
    ('tagless-runtime-trusts-fingerprint', 'daemon/substrate_client.py', '        expected = "FRAU"\n', '        expected = SLASH_PROTOCOLS.get(fingerprint)\n'),
    ('supersede-args-unchecked', 'daemon/substrate_client.py', 'if [field["name"] for field in call["fields"]] != SUPERSEDE_ARGS:', 'if False:'),
    ('supersede-tag-unchecked', 'daemon/substrate_client.py', 'if attest_tag != b"STCA":', 'if False:'),
    ('supersede-always-offered', 'daemon/substrate_client.py', 'if call is None:\n        return False', 'if call is None:\n        return True'),
    ('min-depth-hardcoded', 'daemon/substrate_client.py', 'min_finality_depth=_u32_constant(constants, "MinFinalityDepth"),', 'min_finality_depth=15,'),
    ('contract-hashes-docs', 'daemon/substrate_client.py', 'fields(call["fields"])],', 'fields(call["fields"]), call["docs"]],'),
    ('contract-ignores-call-index', 'daemon/substrate_client.py', '[call["index"], call["name"],', '[call["name"],'),
    ('contract-ignores-field-types', 'daemon/substrate_client.py', 'return [[field["name"], resolve(field["type"], seen)] for field in entries]', 'return [[field["name"]] for field in entries]'),
    ('contract-ignores-variant-index', 'daemon/substrate_client.py', '[[v["index"], v["name"], fields(v["fields"], seen)] for v in variants]', '[[v["name"], fields(v["fields"], seen)] for v in variants]'),
    ('contract-ignores-array-length', 'daemon/substrate_client.py', 'return {kind: [body["len"], resolve(body["type"], seen)]}', 'return {kind: resolve(body["type"], seen)}'),
    ('contract-ignores-request-record', 'daemon/substrate_client.py', '"requests": map_types(storage["ClaimSettlementRequests"]),', '"requests": None,'),
    ('contract-ignores-request-scope', 'daemon/substrate_client.py', '"request_slashes": None if request_slashes is None else map_types(request_slashes),', '"request_slashes": None,'),
    ('contract-ignores-ttl-type', 'daemon/substrate_client.py', '"ttl": [ttl["name"], resolve(ttl["type"])],', '"ttl": [ttl["name"]],'),
    ('contract-recursion-unchecked', 'daemon/substrate_client.py', 'if type_id in seen:', 'if False:'),
    ('contract-unsupported-type-accepted', 'daemon/substrate_client.py', 'raise ValueError(f"unsupported {kind} type in slash contract")', 'return {kind: body}'),
    ('ttl-hardcoded', 'daemon/substrate_client.py', 'ttl=_u32_constant(constants, "SettlementRequestTtl"),', 'ttl=2400,'),
    ('u32-width-unchecked', 'daemon/substrate_client.py', 'if len(raw) != 4:', 'if False:'),
    ('unknown-protocol-accepted', 'daemon/settle_claim_attestor.py', 'if context["protocol"] not in ("FRAU", "FRV2"):', 'if False:'),
    ('malformed-supersede-accepted', 'daemon/settle_claim_attestor.py', 'if type(context["supersede"]) is not bool:', 'if False:'),
    ('malformed-context-accepted', 'daemon/settle_claim_attestor.py', 'if type(context[name]) is not int or not 0 <= context[name] < limit:', 'if False:'),
    ('runtime-error-fallback', 'daemon/slash_watcher.py', '            return None\n\n    async def _request_context', '            return dict(protocol="FRAU", block_number=101, ttl=2400, spec_version=239)\n\n    async def _request_context'),
    ('expiry-disabled', 'daemon/settle_claim_attestor.py', 'return request.submitted_block + context["ttl"] < context["block_number"]', 'return False'),
    ('expiry-equality-rejected', 'daemon/settle_claim_attestor.py', 'context["ttl"] < context["block_number"]', 'context["ttl"] <= context["block_number"]'),
    ('expired-tick-filter-disabled', 'daemon/slash_watcher.py', 'if request_expired(req, context):', 'if False:'),
    ('post-observe-check-disabled', 'daemon/slash_watcher.py', 'context = await self._request_context(request, verdict)\n        if context is None:', 'context = dict(protocol="FRAU", block_number=101, ttl=10)\n        if context is None:'),
    ('post-lock-check-disabled', 'daemon/slash_watcher.py', 'current = await self._request_context(request, verdict)', 'current = context'),
    ('upgrade-bundle-accepted', 'daemon/slash_watcher.py', 'if current["protocol"] != context["protocol"]:', 'if False:'),
    ('legacy-signing-after-upgrade', 'daemon/slash_watcher.py', 'if context["protocol"] == "FRAU"', 'if True'),
    ('peer-digest-filter-disabled', 'daemon/multisig_aggregator.py', 'if dig_hex.lower() != digest.hex():', 'if False:'),
    ('request-identity-unbound', 'daemon/slash_watcher.py', 'request_id = hashlib.blake2b(encode_request_identity(request), digest_size=32).digest()', 'request_id = bytes(32)'),
    ('request-block-u32', 'daemon/slash_watcher.py', '(request.submitted_block, 8)', '(request.submitted_block, 4)'),
    ('request-block-big-endian', 'daemon/slash_watcher.py', 'fields.append(value.to_bytes(width, "little"))', 'fields.append(value.to_bytes(width, "big"))'),
    ('supersede-on-unsupported-runtime', 'daemon/settle_claim_attestor.py', 'if not context["supersede"]:', 'if False:'),
    ('supersede-expired-request', 'daemon/settle_claim_attestor.py', 'elif request_expired(request, context):', 'elif False:'),
    ('supersede-backoff-ignored', 'daemon/settle_claim_attestor.py', 'elif context["block_number"] < self._supersede_backoff.get(request.claim_id, (0, 0))[1]:', 'elif False:'),
    ('supersede-depth-unverified', 'daemon/settle_claim_attestor.py', 'if obs.depth is None or obs.depth < floor:', 'if obs.depth is None:'),
    ('supersede-local-floor-ignored', 'daemon/settle_claim_attestor.py', 'floor = max(context["min_finality_depth"], self.min_finality_depth)', 'floor = context["min_finality_depth"]'),
    ('supersede-voucher-unverified', 'daemon/settle_claim_attestor.py', 'if voucher is None or (', 'if False and ('),
    ('supersede-signs-requested-slot', 'daemon/settle_claim_attestor.py', 'observed_slot=obs.observed_slot,', 'observed_slot=request.observed_slot,'),
    ('supersede-signs-local-depth', 'daemon/settle_claim_attestor.py', 'observed_at_depth=context["min_finality_depth"],', 'observed_at_depth=obs.depth,'),
    ('supersede-without-quorum', 'daemon/settle_claim_attestor.py', 'if len(envelope) < self.min_signer_threshold:\n            verdict.supersede =', 'if False:\n            verdict.supersede ='),
    ('supersede-lock-recheck-skipped', 'daemon/settle_claim_attestor.py', 'current = await self._supersede_context(request, verdict)', 'current = context'),
    ('supersede-request-reread-skipped', 'daemon/settle_claim_attestor.py', 'unchanged = pending is not None and PendingSettlementRequest(**pending) == request', 'unchanged = True'),
    ('supersede-backoff-not-doubling', 'daemon/settle_claim_attestor.py', 'delay = min(SUPERSEDE_BACKOFF_BLOCKS * 2 ** (failures - 1), current["ttl"])', 'delay = SUPERSEDE_BACKOFF_BLOCKS'),
    ('supersede-backoff-uncapped', 'daemon/settle_claim_attestor.py', 'delay = min(SUPERSEDE_BACKOFF_BLOCKS * 2 ** (failures - 1), current["ttl"])', 'delay = SUPERSEDE_BACKOFF_BLOCKS * 2 ** (failures - 1)'),
    ('supersede-backoff-never-pruned', 'daemon/settle_claim_attestor.py', 'del self._supersede_backoff[claim]', 'pass'),
    ('supersede-mode-dropped', 'daemon/substrate_client.py', '"settled_direct": bool(settled_direct),', '"settled_direct": False,'),
    ('voucher-beneficiary-is-stake-key', 'daemon/substrate_client.py', 'split_type0_address_bytes(raw)[0]', 'split_type0_address_bytes(raw)[1]'),
    ('contract-cache-ignores-code', 'daemon/substrate_client.py', 'if code_hash and cached is not None and cached[0] == code_hash:', 'if cached is not None:'),
    ('cache-hit-skips-spec-name', 'daemon/substrate_client.py', 'if version["specName"] != "materios":', 'if version["specName"] != "materios" and not (self._settlement_contract and self._settlement_contract[0] == code_hash):'),
    ('failed-lookup-is-absence', 'daemon/settle_claim_attestor.py', '            if matches is None:\n                obs.mismatches.append("kupo_matches_unavailable")', '            if False:\n                obs.mismatches.append("kupo_matches_unavailable")'),
    ('shape-check-skipped', 'daemon/slash_watcher.py', 'if kupo_shape_error(base.outputs, expected_beneficiary_blake2_224):', 'if False:'),
    ('unproven-absence-is-proof', 'daemon/slash_watcher.py', 'return TxNotFound() if absence_proven else None', 'return TxNotFound()'),
    ('absence-single-proof', 'daemon/slash_watcher.py', 'if height - first < TX_ABSENCE_CONFIRM_BLOCKS:', 'if False:'),
    ('absence-kept-after-tx-seen', 'daemon/slash_watcher.py', 'if reason == "tx_found":', 'if False:'),
    ('absence-key-ignores-request', 'daemon/slash_watcher.py', 'key = astuple(request)', 'key = request.claim_id'),
    ('absences-never-cleared', 'daemon/slash_watcher.py', 'self._absences.clear()', 'pass'),
    ('freshness-gate-off', 'daemon/slash_watcher.py', 'if checkpoint < tip["slot"] - KUPO_TIP_LAG_MAX_SLOTS:', 'if False:'),
    ('freshness-bound-exclusive', 'daemon/slash_watcher.py', 'if checkpoint < tip["slot"] - KUPO_TIP_LAG_MAX_SLOTS:', 'if checkpoint <= tip["slot"] - KUPO_TIP_LAG_MAX_SLOTS:'),
    ('lookup-before-checkpoint', 'daemon/slash_watcher.py', '            checkpoint = await self.kupo_checkpoint_slot()\n', '            await self._kupo_get(session, f"/matches/*@{cardano_tx_hash_hex}", {})\n            checkpoint = await self.kupo_checkpoint_slot()\n'),
    ('node-tip-as-checkpoint', 'daemon/slash_watcher.py', 'checkpoint = payload.get("most_recent_checkpoint")', 'checkpoint = payload.get("most_recent_checkpoint", payload.get("most_recent_node_tip"))'),
    ('patterns-unchecked', 'daemon/slash_watcher.py', 'if "*" not in patterns:', 'if False:'),
    ('spent-canary-unchecked', 'daemon/slash_watcher.py', 'if not any(isinstance(row, dict) and row.get("spent_at") for row in rows):', 'if False:'),
    ('network-without-canary-allowed', 'daemon/slash_watcher.py', 'return "no_spent_canary_for_network"', 'return None'),
    ('coverage-skipped-for-present-tx', 'daemon/slash_watcher.py', 'gap = await self.observer.kupo_coverage_gap()', 'gap = None'),
    ('network-check-off', 'daemon/slash_watcher.py', 'and observation.mainchain_genesis_hash != request.mainchain_genesis_hash', 'and False'),
    ('tx-lookup-scans', 'daemon/settle_claim_attestor.py', 'session, f"/matches/*@{cardano_tx_hash_hex}", {},', 'session, "/matches/*", {"transaction_id": cardano_tx_hash_hex},'),
    ('envelope-admits-non-members', 'daemon/multisig_aggregator.py', 'if pub in committee and _verifies(pub, sig, digest):', 'if _verifies(pub, sig, digest):'),
    ('envelope-admits-unverified', 'daemon/multisig_aggregator.py', 'if pub in committee and _verifies(pub, sig, digest):', 'if pub in committee:'),
    ('envelope-wrapped-bytes-accepted', 'daemon/multisig_aggregator.py', 'return sr25519.verify(sig, digest, pubkey)', 'return sr25519.verify(sig, digest, pubkey) or sr25519.verify(sig, b"<Bytes>" + digest + b"</Bytes>", pubkey)'),
    ('unreadable-committee-admits-all', 'daemon/multisig_aggregator.py', '            committee = frozenset()\n', '            committee = frozenset(pub for pub, _ in await self.fetch_envelope(session, kind=kind, key=key, digest=digest))\n'),
    ('committee-from-stake-key', 'daemon/substrate_client.py', 'raw = bytes.fromhex(account[2:] if account.startswith("0x") else ss58_decode(account))', 'raw = bytes(32)'),
]


def run(directory):
    return subprocess.run([sys.executable, '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider', *TESTS],
                          cwd=directory, capture_output=True, text=True,
                          env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}, timeout=90)


if __name__ == '__main__':
    baseline = run(ROOT)
    if baseline.returncode:
        sys.exit(baseline.stdout + baseline.stderr)
    for name, filename, before, after in MUTANTS:
        with tempfile.TemporaryDirectory(prefix='operator-kit-frv2-mutant-') as scratch:
            scratch = Path(scratch)
            for directory in ('daemon', 'tests'):
                shutil.copytree(ROOT / directory, scratch / directory, ignore=shutil.ignore_patterns('__pycache__'))
            source = scratch / filename
            text = source.read_text()
            if text.count(before) != 1:
                sys.exit(f'{name}: expected exactly one mutation site')
            source.write_text(text.replace(before, after))
            result = run(scratch)
            if result.returncode != 1 or ' failed' not in result.stdout or 'ERROR collecting' in result.stdout:
                sys.exit(f'{name}: SURVIVED or invalid mutant\n{result.stdout}\n{result.stderr}')
            print(f'KILLED {name}', flush=True)
    print(f'{len(MUTANTS)}/{len(MUTANTS)} mutants killed')
