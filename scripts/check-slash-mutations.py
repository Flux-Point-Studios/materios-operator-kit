#!/usr/bin/env python3
"""Run targeted slash-safety mutants in isolated copies of the test suite."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TESTS = ['tests/test_slash_runtime.py', 'tests/test_frv2_vectors.py']
MUTANTS = [
    ('wrong-chain-accepted', 'daemon/substrate_client.py', 'if version["specName"] != "materios":', 'if False:'),
    ('unknown-contract-uses-frau', 'daemon/substrate_client.py', 'if SLASH_PROTOCOLS.get(fingerprint) != expected:', 'if fingerprint in SLASH_PROTOCOLS and SLASH_PROTOCOLS[fingerprint] != expected:'),
    ('tag-skips-contract-pin', 'daemon/substrate_client.py', 'if SLASH_PROTOCOLS.get(fingerprint) != expected:', 'if False:'),
    ('unknown-tag-signs-frv2', 'daemon/substrate_client.py', 'elif tag == b"FRV2":', 'elif tag is not None:'),
    ('tag-ignored', 'daemon/substrate_client.py', 'constants.get("SlashSettlementTag"))', 'None)'),
    ('tagless-runtime-trusts-fingerprint', 'daemon/substrate_client.py', '        expected = "FRAU"\n', '        expected = SLASH_PROTOCOLS.get(fingerprint)\n'),
    ('supersede-args-unchecked', 'daemon/substrate_client.py', 'if [field["name"] for field in call["fields"]] != SUPERSEDE_ARGS:', 'if False:'),
    ('supersede-tag-unchecked', 'daemon/substrate_client.py', 'if attest_tag != b"STCA":', 'if False:'),
    ('supersede-always-offered', 'daemon/substrate_client.py', 'if call is None:\n        return False', 'if call is None:\n        return True'),
    ('min-depth-hardcoded', 'daemon/substrate_client.py', 'min_finality_depth = _u32_constant(constants, "MinFinalityDepth")', 'min_finality_depth = 15'),
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
    ('ttl-hardcoded', 'daemon/substrate_client.py', 'ttl = _u32_constant(constants, "SettlementRequestTtl")', 'ttl = 2400'),
    ('u32-width-unchecked', 'daemon/substrate_client.py', 'if len(raw) != 4:', 'if False:'),
    ('unknown-protocol-accepted', 'daemon/slash_watcher.py', 'if context["protocol"] not in ("FRAU", "FRV2"):', 'if False:'),
    ('malformed-context-accepted', 'daemon/slash_watcher.py', 'if type(context[field]) is not int or not 0 <= context[field] < limit:', 'if False:'),
    ('runtime-error-fallback', 'daemon/slash_watcher.py', '            return None\n\n    async def _request_context', '            return dict(protocol="FRAU", block_number=101, ttl=2400, spec_version=239)\n\n    async def _request_context'),
    ('expiry-disabled', 'daemon/slash_watcher.py', 'return request.submitted_block + context["ttl"] < context["block_number"]', 'return False'),
    ('expiry-equality-rejected', 'daemon/slash_watcher.py', 'context["ttl"] < context["block_number"]', 'context["ttl"] <= context["block_number"]'),
    ('expired-tick-filter-disabled', 'daemon/slash_watcher.py', 'if request_expired(req, context):', 'if False:'),
    ('post-observe-check-disabled', 'daemon/slash_watcher.py', 'context = await self._request_context(request, verdict)\n        if context is None:', 'context = dict(protocol="FRAU", block_number=101, ttl=10)\n        if context is None:'),
    ('post-lock-check-disabled', 'daemon/slash_watcher.py', 'current = await self._request_context(request, verdict)', 'current = context'),
    ('upgrade-bundle-accepted', 'daemon/slash_watcher.py', 'if current["protocol"] != context["protocol"]:', 'if False:'),
    ('legacy-signing-after-upgrade', 'daemon/slash_watcher.py', 'if context["protocol"] == "FRAU"', 'if True'),
    ('peer-digest-filter-disabled', 'daemon/multisig_aggregator.py', 'if dig_hex.lower() != digest.hex():', 'if False:'),
    ('request-identity-unbound', 'daemon/slash_watcher.py', 'request_id = hashlib.blake2b(encode_request_identity(request), digest_size=32).digest()', 'request_id = bytes(32)'),
    ('request-block-u32', 'daemon/slash_watcher.py', '(request.submitted_block, 8)', '(request.submitted_block, 4)'),
    ('request-block-big-endian', 'daemon/slash_watcher.py', 'fields.append(value.to_bytes(width, "little"))', 'fields.append(value.to_bytes(width, "big"))'),
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
