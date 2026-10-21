#!/usr/bin/env python3
"""Run targeted safety mutants in isolated copies of the test suite: slash
signing, and the attest path's recovery from dead RPC sockets. Each suite runs
only its own tests, and first proves they pass unmutated in that same layout."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SLASH_TESTS = ['tests/test_slash_runtime.py', 'tests/test_frv2_vectors.py', 'tests/test_settle_supersede.py',
               'tests/test_slash_proof.py', 'tests/test_slash_watcher.py', 'tests/test_multisig_aggregator.py',
               'tests/test_settlement_submits.py', 'tests/test_poll_loop.py']
SLASH_MUTANTS = [
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
    ('runtime-error-fallback', 'daemon/slash_watcher.py', '                             type(error).__name__)\n            return None\n', '                             type(error).__name__)\n            return dict(protocol="FRAU", block_number=101, ttl=2400, spec_version=239)\n'),
    ('expiry-disabled', 'daemon/settle_claim_attestor.py', 'return request.submitted_block + context["ttl"] < context["block_number"]', 'return False'),
    ('expiry-equality-rejected', 'daemon/settle_claim_attestor.py', 'context["ttl"] < context["block_number"]', 'context["ttl"] <= context["block_number"]'),
    ('expired-tick-filter-disabled', 'daemon/slash_watcher.py', 'if past_slash_window(req, context):', 'if False:'),
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
    ('supersede-lock-recheck-skipped', 'daemon/settle_claim_attestor.py', '        current = await self._supersede_context(request, verdict)\n        if current is None:\n            return None\n', '        current = {"ttl": 2400, "block_number": 0}\n'),
    ('supersede-request-reread-skipped', 'daemon/settle_claim_attestor.py', 'if pending is None or PendingSettlementRequest(**pending) != request:', 'if False:'),
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
    ('absence-single-proof', 'daemon/slash_watcher.py', 'if blocks < TX_ABSENCE_CONFIRM_BLOCKS:', 'if False:'),
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
    ('coverage-skipped-for-present-tx', 'daemon/slash_watcher.py', '            gap = await self.observer.kupo_coverage_gap()\n            if gap is not None:\n                verdict.outcome', '            gap = None\n            if gap is not None:\n                verdict.outcome'),
    ('network-check-off', 'daemon/slash_watcher.py', 'and observation.mainchain_genesis_hash != request.mainchain_genesis_hash', 'and False'),
    ('tx-lookup-scans', 'daemon/settle_claim_attestor.py', 'session, f"/matches/*@{cardano_tx_hash_hex}", {},', 'session, "/matches/*", {"transaction_id": cardano_tx_hash_hex},'),
    ('envelope-admits-non-members', 'daemon/multisig_aggregator.py', 'if pub in committee and _verifies(pub, sig, digest):', 'if _verifies(pub, sig, digest):'),
    ('envelope-admits-unverified', 'daemon/multisig_aggregator.py', 'if pub in committee and _verifies(pub, sig, digest):', 'if pub in committee:'),
    ('envelope-wrapped-bytes-accepted', 'daemon/multisig_aggregator.py', 'return sr25519.verify(sig, digest, pubkey)', 'return sr25519.verify(sig, digest, pubkey) or sr25519.verify(sig, b"<Bytes>" + digest + b"</Bytes>", pubkey)'),
    ('unreadable-committee-admits-all', 'daemon/multisig_aggregator.py', '            committee = frozenset()\n', '            committee = frozenset(pub for pub, _ in await self.fetch_envelope(session, kind=kind, key=key, digest=digest))\n'),
    ('committee-from-stake-key', 'daemon/substrate_client.py', 'raw = bytes.fromhex(account[2:] if account.startswith("0x") else ss58_decode(account))', 'raw = bytes(32)'),
    ('runtime-announced-every-read', 'daemon/substrate_client.py', '        if self._settlement_reported != runtime:\n            self._settlement_reported = runtime\n            logger.info(', '        if True:\n            logger.info('),
    ('refusal-reported-every-read', 'daemon/substrate_client.py', '            if self._settlement_reported != runtime:\n                self._settlement_reported = runtime\n                logger.error(', '            if True:\n                logger.error('),
    ('refusal-metric-stale', 'daemon/substrate_client.py', 'health_server.update_metrics(settlement_slash_protocol=0, settlement_supersede_offered=0)', 'pass'),
    ('coverage-metric-constant', 'daemon/slash_watcher.py', 'slash_kupo_coverage_ok=int(gap is None)', 'slash_kupo_coverage_ok=1'),
    ('status-not-polled', 'daemon/slash_watcher.py', '            await self._report_status()\n', '            pass\n'),
    ('supersede-skips-silent', 'daemon/settle_claim_attestor.py', '            logger.info("settle_attestor: not superseding claim_id=%s...: %s",', '            logger.debug("settle_attestor: not superseding claim_id=%s...: %s",'),
    ('genesis-read-unguarded', 'daemon/settle_claim_attestor.py', '            failures += 1\n', '            raise\n'),
    ('genesis-backoff-flat', 'daemon/settle_claim_attestor.py', 'delay = min(poll_interval * 2 ** failures, GENESIS_RETRY_MAX_SECONDS)', 'delay = poll_interval'),
    ('genesis-backoff-uncapped', 'daemon/settle_claim_attestor.py', 'delay = min(poll_interval * 2 ** failures, GENESIS_RETRY_MAX_SECONDS)', 'delay = poll_interval * 2 ** failures'),
    ('listing-swallows-errors', 'daemon/substrate_client.py', '        rows = self._call(\n            "query_map",\n            module="IntentSettlement",\n            storage_function="ClaimSettlementRequests",\n        )\n', '        try:\n            rows = self._call(\n                "query_map",\n                module="IntentSettlement",\n                storage_function="ClaimSettlementRequests",\n            )\n        except Exception:\n            return out\n'),
    ('slash-sent-against-replacement', 'daemon/slash_watcher.py', 'if pending is None or PendingBondedRequest(**pending) != request:', 'if pending is None:'),
    ('slash-reread-ignored', 'daemon/slash_watcher.py', 'if pending is None or PendingBondedRequest(**pending) != request:', 'if False:'),
    ('frau-expiry-filtered', 'daemon/slash_watcher.py', 'return context["protocol"] == "FRV2" and request_expired(request, context)', 'return request_expired(request, context)'),
    ('wallclock-gate-off', 'daemon/slash_watcher.py', 'if tip["lag"] > CARDANO_TIP_LAG_MAX_SLOTS:', 'if False:'),
    ('wallclock-bound-exclusive', 'daemon/slash_watcher.py', 'if tip["lag"] > CARDANO_TIP_LAG_MAX_SLOTS:', 'if tip["lag"] >= CARDANO_TIP_LAG_MAX_SLOTS:'),
    ('wallclock-ignores-shelley-slot', 'daemon/slash_watcher.py', 'lag = shelley_slot + int(self.wall_clock() - shelley_unix) - tip["slot"]', 'lag = int(self.wall_clock() - shelley_unix) - tip["slot"]'),
    ('unknown-network-clock-allowed', 'daemon/slash_watcher.py', '            return None, "no_network_clock"\n', '            clock = (0, 0)\n'),
    ('density-gate-off', 'daemon/slash_watcher.py', 'if slot - first_slot > CHAIN_SLOTS_PER_BLOCK_MAX * blocks:', 'if False:'),
    ('density-bound-exclusive', 'daemon/slash_watcher.py', 'if slot - first_slot > CHAIN_SLOTS_PER_BLOCK_MAX * blocks:', 'if slot - first_slot >= CHAIN_SLOTS_PER_BLOCK_MAX * blocks:'),
    ('sparse-chain-keeps-first-proof', 'daemon/slash_watcher.py', '            self._absences[key] = proof\n', ''),
    ('sighting-keeps-absence', 'daemon/slash_watcher.py', '        if observation.outputs:\n            self._absences.pop(astuple(request), None)\n', ''),
    ('tip-lag-not-exported', 'daemon/slash_watcher.py', 'health_server.update_metrics(slash_cardano_tip_lag_slots=lag)', 'pass'),
    ('slash-reread-unbounded', 'daemon/slash_watcher.py', 'current = await asyncio.wait_for(self._still_current(request, verdict, context),\n                                                 LOCKED_REREAD_SECONDS)', 'current = await self._still_current(request, verdict, context)'),
    ('supersede-reread-unbounded', 'daemon/settle_claim_attestor.py', 'current = await asyncio.wait_for(self._still_supersedable(request, verdict), LOCKED_REREAD_SECONDS)', 'current = await self._still_supersedable(request, verdict)'),
]

TRANSPORT_TESTS = ['tests/test_dead_write_conn.py', 'tests/test_receipt_rpc_retry.py',
                   'tests/test_write_conn_keepalive.py', 'tests/test_substrate_client_ws_retry.py',
                   'daemon/tests/test_read_write_ws_split.py', 'tests/test_attest_refusal_verdict.py',
                   'tests/test_deferred_receipt_restart.py']
TRANSPORT_MUTANTS = [
    ('transport-branch-deleted', 'daemon/substrate_client.py', '                if not is_transport_dead(error):\n                    raise\n                self._replace_dead_conn', '                raise\n                self._replace_dead_conn'),
    ('transport-set-broken-pipe-only', 'daemon/substrate_client.py', 'TRANSPORT_DEAD_ERRORS = (\n    BrokenPipeError,\n    ConnectionResetError,\n    ConnectionAbortedError,\n    WebSocketConnectionClosedException,\n    WebSocketTimeoutException,\n    ssl.SSLEOFError,\n)', 'TRANSPORT_DEAD_ERRORS = (BrokenPipeError,)'),
    ('transport-set-websocket-only', 'daemon/substrate_client.py', 'TRANSPORT_DEAD_ERRORS = (\n    BrokenPipeError,\n    ConnectionResetError,\n    ConnectionAbortedError,\n    WebSocketConnectionClosedException,\n    WebSocketTimeoutException,\n    ssl.SSLEOFError,\n)', 'TRANSPORT_DEAD_ERRORS = (WebSocketConnectionClosedException,)'),
    ('kernel-timeout-not-transport', 'daemon/substrate_client.py', '    WebSocketConnectionClosedException,\n    WebSocketTimeoutException,\n', '    WebSocketConnectionClosedException,\n'),
    ('fd-errnos-not-transport', 'daemon/substrate_client.py', 'isinstance(e, TRANSPORT_DEAD_ERRORS) or (isinstance(e, OSError) and e.errno in TRANSPORT_DEAD_ERRNOS)', 'isinstance(e, TRANSPORT_DEAD_ERRORS)'),
    ('ebadf-not-transport', 'daemon/substrate_client.py', 'frozenset({errno.EBADF, errno.ENOTSOCK, errno.ENOTCONN})', 'frozenset({errno.ENOTSOCK, errno.ENOTCONN})'),
    ('ebadf-the-only-fd-errno', 'daemon/substrate_client.py', 'frozenset({errno.EBADF, errno.ENOTSOCK, errno.ENOTCONN})', 'frozenset({errno.EBADF})'),
    ('wrapped-transport-missed', 'daemon/substrate_client.py', '    return any(\n        isinstance(e, TRANSPORT_DEAD_ERRORS) or (isinstance(e, OSError) and e.errno in TRANSPORT_DEAD_ERRNOS)\n        for e in _error_chain(error)\n    )', '    return isinstance(error, TRANSPORT_DEAD_ERRORS) or (isinstance(error, OSError) and error.errno in TRANSPORT_DEAD_ERRNOS)'),
    ('transport-not-retried', 'daemon/substrate_client.py', '                if attempt == 2 or conn.si is None:\n                    raise\n', '                raise\n'),
    ('second-failure-swallowed', 'daemon/substrate_client.py', '                if attempt == 2 or conn.si is None:\n', '                if conn.si is None:\n'),
    ('retried-on-the-dead-socket', 'daemon/substrate_client.py', '            health_server.increment_metric("ws_transport_replacements_total")\n            self._force_replace(conn)\n', '            health_server.increment_metric("ws_transport_replacements_total")\n'),
    ('replacement-not-counted', 'daemon/substrate_client.py', '            health_server.increment_metric("ws_transport_replacements_total")\n', ''),
    ('one-dead-socket-replaced-per-call', 'daemon/substrate_client.py', '            if conn.si is not dead_si:\n                return\n', ''),
    ('timeout-replace-unserialized', 'daemon/substrate_client.py', '            with conn.replace_lock:\n                self._force_replace(conn)\n', '            self._force_replace(conn)\n'),
    ('stale-timeout-strikes-replacement', 'daemon/substrate_client.py', '        if conn.si is not si:\n            return\n        conn.consecutive_timeouts += 1\n', '        conn.consecutive_timeouts += 1\n'),
    ('no-reconnect-on-use', 'daemon/substrate_client.py', '                if conn.si is None:\n                    self._force_replace(conn)\n', '                pass\n'),
    ('close-frame-not-transport', 'daemon/substrate_client.py', 'if (isinstance(error, json.JSONDecodeError)', 'if False and (isinstance(error, json.JSONDecodeError)'),
    ('submit-swallows-dead-socket', 'daemon/substrate_client.py', '                if is_transport_dead(e):\n                    raise\n', ''),
    ('refusal-never-done', 'daemon/substrate_client.py', '                if settled:\n                    logger.info(', '                if False:\n                    logger.info('),
    ('refusal-always-done', 'daemon/substrate_client.py', '                settled = self.attestation_on_chain(receipt_id, cert_hash)\n', '                settled = True\n'),
    ('unsettled-refusal-fails', 'daemon/substrate_client.py', '                if settled is None:\n                    raise AttestUnsettled', '                if False:\n                    raise AttestUnsettled'),
    ('refusal-for-good-resubmitted', 'daemon/substrate_client.py', '                return SubmitCertOutcome(success=False, error_message=f"{type(e).__name__}: {e}")\n', '                last_error = f"{type(e).__name__}: {e}"\n'),
    ('verdict-reads-finalized', 'daemon/substrate_client.py', 'head = self._call("get_chain_head")', 'head = self._call("get_chain_finalised_head")'),
    ('verdict-on-the-read-conn', 'daemon/substrate_client.py', '"Attestations", [receipt_id], block_hash=head).value', '"Attestations", [receipt_id], conn=self._read).value'),
    ('verdict-reads-unpinned', 'daemon/substrate_client.py', '"Attestations", [receipt_id], block_hash=head).value', '"Attestations", [receipt_id]).value'),
    ('non-member-counts-done', 'daemon/substrate_client.py', '        if self.keypair.public_key not in _account_ids(members):\n            return False\n', ''),
    ('certified-under-any-hash-done', 'daemon/substrate_client.py', 'return cert_hash is None or cert_hash == receipt.availability_cert_hash', 'return True'),
    ('attested-under-another-hash-open', 'daemon/substrate_client.py', '        if cert_hash is not None and cert_hash != _to_bytes32(entry[0]):\n            return False\n', ''),
    ('any-attester-counts-as-ours', 'daemon/substrate_client.py', 'return True if self.keypair.public_key in _account_ids(entry[1]) else None', 'return True if entry[1] else None'),
    ('timeouts-not-deferred', 'daemon/substrate_client.py', 'isinstance(e, (RPCTimeoutError, AttestUnsettled)) for e in _error_chain(error)', 'isinstance(e, AttestUnsettled) for e in _error_chain(error)'),
    ('unsettled-refusal-dropped', 'daemon/substrate_client.py', 'isinstance(e, (RPCTimeoutError, AttestUnsettled)) for e in _error_chain(error)', 'isinstance(e, RPCTimeoutError) for e in _error_chain(error)'),
    ('keepalive-interval-past-proxy-window', 'daemon/substrate_client.py', '"MATERIOS_RPC_KEEPALIVE_SECS", 30.0', '"MATERIOS_RPC_KEEPALIVE_SECS", 120.0'),
    ('rpc-failure-dropped-with-its-block', 'daemon/cert_daemon.py', 'if isinstance(res, BaseException) and should_defer(res):', 'if False:'),
    ('rpc-retries-unbounded', 'daemon/cert_daemon.py', 'if entry.retries > MAX_RECEIPT_RPC_RETRIES:', 'if False:'),
    ('rpc-retry-count-reset', 'daemon/cert_daemon.py', 'await self._defer_receipt(receipt_id, error, pending)', 'await self._defer_receipt(receipt_id, error)'),
    ('deferred-receipt-not-retried', 'daemon/cert_daemon.py', '            if pending.last_error:\n', '            if False:\n'),
    ('deferred-retry-signs-again', 'daemon/cert_daemon.py', '                    settled = await asyncio.to_thread(\n                        self.client.attestation_on_chain, receipt_id, pending.cert_hash\n                    )\n', '                    settled = None\n'),
    ('recheck-ignores-claimed-hash', 'daemon/cert_daemon.py', 'self.client.attestation_on_chain, receipt_id, pending.cert_hash', 'self.client.attestation_on_chain, receipt_id, None'),
    ('recheck-refusal-counted-done', 'daemon/cert_daemon.py', '                    elif settled:\n', '                    elif True:\n'),
    ('recheck-done-not-counted', 'daemon/cert_daemon.py', 'skipped: already on chain")\n                        health_server.increment_metric("certs_already_on_chain_total")\n', 'skipped: already on chain")\n'),
    ('cert-stored-before-submit', 'daemon/cert_daemon.py', '        cert_hash = hashlib.sha256(cert_bytes).digest()\n', '        cert_hash = hashlib.sha256(cert_bytes).digest()\n        self.cert_store.save(receipt_id, cert_bytes)\n'),
    ('done-reported-as-submitted', 'daemon/cert_daemon.py', '            if outcome.already_on_chain:\n', '            if False:\n'),
    ('done-not-counted', 'daemon/cert_daemon.py', '            if outcome.already_on_chain:\n                health_server.increment_metric("certs_already_on_chain_total")\n', '            if outcome.already_on_chain:\n                pass\n'),
    ('refused-hash-not-kept', 'daemon/cert_daemon.py', '        if refused:\n            entry.cert_hash = error.cert_hash\n', ''),
    ('unsettled-drop-silent', 'daemon/cert_daemon.py', '            if refused:\n                await self._cert_failed(receipt_id, entry.last_error)\n', ''),
    ('deferred-unbounded', 'daemon/cert_daemon.py', 'if sum(1 for waiting in self.pending.values() if waiting.last_error) >= MAX_DEFERRED_RECEIPTS:', 'if False:'),
    ('deferred-not-persisted', 'daemon/cert_daemon.py', '                    for receipt_id, entry in self.pending.items()\n                    if entry.last_error\n', '                    for receipt_id, entry in []\n'),
    ('locator-waits-persisted', 'daemon/cert_daemon.py', '                    for receipt_id, entry in self.pending.items()\n                    if entry.last_error\n', '                    for receipt_id, entry in self.pending.items()\n'),
    ('deferred-not-restored', 'daemon/cert_daemon.py', '                self.pending.update(deferred)\n', ''),
    ('retries-reset-on-restart', 'daemon/cert_daemon.py', 'retries=entry["retries"],', 'retries=0,'),
    ('claimed-hash-not-persisted', 'daemon/cert_daemon.py', '"cert_hash": entry.cert_hash.hex() if entry.cert_hash else None,', '"cert_hash": None,'),
    ('claimed-hash-not-restored', 'daemon/cert_daemon.py', 'cert_hash=bytes.fromhex(entry["cert_hash"]) if entry["cert_hash"] else None,', 'cert_hash=None,'),
    ('deferral-state-not-saved', 'daemon/cert_daemon.py', '        if snapshot:\n            self.save_state()\n', ''),
    ('late-locator-refusal-escapes', 'daemon/cert_daemon.py', '                if not should_defer(error):\n                    raise\n                await self._defer_receipt(receipt_id, error)\n', '                raise\n'),
    ('late-locator-deferral-inherits-polls', 'daemon/cert_daemon.py', '                await self._defer_receipt(receipt_id, error)\n                continue\n', '                await self._defer_receipt(receipt_id, error, pending)\n                continue\n'),
    ('pre-upgrade-state-rejected', 'daemon/cert_daemon.py', 'state.get("deferred", {}).items()', 'state["deferred"].items()'),
    ('keepalive-off', 'daemon/cert_daemon.py', '                    await asyncio.to_thread(self.client.keepalive)\n', '                    pass\n'),
    ('keepalive-waits-behind-submit', 'daemon/cert_daemon.py', '            if self._chain_write_lock.locked():\n                continue\n', ''),
    ('keepalive-never-started', 'daemon/cert_daemon.py', '        keepalive = asyncio.create_task(self._keep_write_conn_alive())\n', '        keepalive = asyncio.create_task(asyncio.Event().wait())\n'),
    ('keepalive-outlives-the-daemon', 'daemon/cert_daemon.py', '        keepalive.cancel()\n', ''),
]
SUITES = [(SLASH_TESTS, SLASH_MUTANTS), (TRANSPORT_TESTS, TRANSPORT_MUTANTS)]


def in_copy(tests, filename=None, before=None, after=None):
    """``tests`` run in an isolated copy of daemon/ and tests/, with one
    mutation applied when ``filename`` is given."""
    with tempfile.TemporaryDirectory(prefix='operator-kit-mutant-') as scratch:
        scratch = Path(scratch)
        for directory in ('daemon', 'tests'):
            shutil.copytree(ROOT / directory, scratch / directory, ignore=shutil.ignore_patterns('__pycache__'))
        if filename:
            source = scratch / filename
            source.write_text(source.read_text().replace(before, after))
        return subprocess.run([sys.executable, '-B', '-m', 'pytest', '-x', '-q', '-p', 'no:cacheprovider', *tests],
                              cwd=scratch, capture_output=True, text=True,
                              env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}, timeout=180)


def mutate(tests, name, filename, before, after):
    """None when the suite kills the mutant, else why not."""
    result = in_copy(tests, filename, before, after)
    if result.returncode != 1 or ' failed' not in result.stdout or 'ERROR collecting' in result.stdout:
        return f'{name}: SURVIVED or invalid mutant\n{result.stdout}\n{result.stderr}'
    return None


if __name__ == '__main__':
    mutants = [(tests, *mutant) for tests, suite in SUITES for mutant in suite]
    for _, name, filename, before, _ in mutants:
        if (ROOT / filename).read_text().count(before) != 1:
            sys.exit(f'{name}: expected exactly one mutation site')
    for tests, _ in SUITES:
        baseline = in_copy(tests)
        if baseline.returncode:
            sys.exit(baseline.stdout + baseline.stderr)
    with ThreadPoolExecutor(max_workers=min(4, os.cpu_count() or 1)) as pool:
        for (_, name, *_), failure in zip(mutants, pool.map(lambda mutant: mutate(*mutant), mutants)):
            if failure:
                sys.exit(failure)
            print(f'KILLED {name}', flush=True)
    print(f'{len(mutants)}/{len(mutants)} mutants killed')
