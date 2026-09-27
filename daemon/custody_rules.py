"""Classify custody and authority moves on Materios and Cardano.

Pure functions over decoded chain data; ``custody_watch`` does the fetching.

Materios: every extrinsic of a finalized block is decoded against runtime
metadata and its call tree walked through Sudo, Multisig, Utility and Recovery
wrappers. Root calls, runtime-code changes, forced balance moves, recovery,
treasury spends, finality overrides and committee-gate levers are CRITICAL.

Cardano: a transaction is classified by what it spends and produces at watched
addresses and what it mints under watched policies. A surrender-pool spend is
INFO only when it has every mark of a surrender (the surrender redeemer, the
pool's continuing output keeping its inline datum, cMATRA leaving the pool only
to the wallets that funded the transaction, and a payout equal to the rate-table
entitlement for the legacy assets deposited at the quarantine address). An
underpayment or an asset outside the rate table is an ALERT; any other departure
is CRITICAL.
"""

from __future__ import annotations

import enum
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from scalecodec.base import RuntimeConfigurationObject, ScaleBytes
from scalecodec.type_registry import load_type_registry_preset
from substrateinterface.utils.ss58 import ss58_decode, ss58_encode

from daemon.cardano_address import decode_cardano_address

SS58_FORMAT = 42


class Severity(enum.IntEnum):
    INFO = 0
    ALERT = 1
    CRITICAL = 2


INFO, ALERT, CRITICAL = Severity.INFO, Severity.ALERT, Severity.CRITICAL


@dataclass(frozen=True)
class Finding:
    severity: Severity
    key: str
    headline: str
    details: tuple[str, ...] = ()
    kind: str = "event"
    amount: int = 0

    def render(self) -> str:
        return "\n".join([f"[{self.severity.name}] {self.headline}", *(f"  {d}" for d in self.details)])


# --- configuration -------------------------------------------------------------


@dataclass(frozen=True)
class WatchedAddress:
    label: str
    address: str
    role: str
    severity: Severity = ALERT


@dataclass(frozen=True)
class WatchedPolicy:
    label: str
    policy_id: str
    severity: Severity = CRITICAL


@dataclass(frozen=True)
class Redemption:
    key: str
    numerator: int
    denominator: int
    unit: str | None = None
    policy_id: str | None = None


@dataclass(frozen=True)
class SurrenderPool:
    label: str
    address: str
    cmatra_unit: str
    cmatra_decimals: int
    quarantine_address: str | None
    redeemers: dict
    surrender_redeemer: str
    redemptions: tuple[Redemption, ...]


@dataclass(frozen=True)
class CardanoNetwork:
    name: str
    blockfrost_url: str
    project_id_file: str
    poll_seconds: int
    reorg_depth_blocks: int
    addresses: tuple[WatchedAddress, ...]
    policies: tuple[WatchedPolicy, ...]
    pool: SurrenderPool | None


@dataclass(frozen=True)
class MateriosConfig:
    name: str
    rpc_url: str
    poll_seconds: int
    state_pruning_blocks: int
    start_block: int | None


@dataclass(frozen=True)
class WatchConfig:
    state_db: str
    digest_hour_utc: int
    source_stale_seconds: int
    materios: MateriosConfig | None
    cardano: tuple[CardanoNetwork, ...]


ADDRESS_ROLES = ("custody", "contract")


def parse_config(doc: dict) -> WatchConfig:
    materios = doc.get("materios")
    return WatchConfig(
        state_db=doc["state_db"],
        digest_hour_utc=int(doc.get("digest_hour_utc", 13)),
        source_stale_seconds=int(doc.get("source_stale_seconds", 900)),
        materios=MateriosConfig(
            name=materios["name"],
            rpc_url=materios["rpc_url"],
            poll_seconds=int(materios.get("poll_seconds", 6)),
            state_pruning_blocks=int(materios.get("state_pruning_blocks", 256)),
            start_block=materios.get("start_block"),
        ) if materios else None,
        cardano=tuple(_parse_network(n) for n in doc.get("cardano", [])),
    )


def _parse_network(doc: dict) -> CardanoNetwork:
    addresses = []
    for a in doc.get("addresses", []):
        if a["role"] not in ADDRESS_ROLES:
            raise ValueError(f"{doc['name']}: address {a['label']} has unknown role {a['role']!r}")
        addresses.append(WatchedAddress(a["label"], a["address"], a["role"],
                                        Severity[a.get("severity", "ALERT")]))
    pool = doc.get("surrender_pool")
    return CardanoNetwork(
        name=doc["name"],
        blockfrost_url=doc["blockfrost_url"].rstrip("/"),
        project_id_file=doc["project_id_file"],
        poll_seconds=int(doc.get("poll_seconds", 60)),
        reorg_depth_blocks=int(doc.get("reorg_depth_blocks", 30)),
        addresses=tuple(addresses),
        policies=tuple(WatchedPolicy(p["label"], p["policy_id"], Severity[p.get("severity", "CRITICAL")])
                       for p in doc.get("policies", [])),
        pool=SurrenderPool(
            label=pool["label"],
            address=pool["address"],
            cmatra_unit=pool["cmatra_unit"],
            cmatra_decimals=int(pool.get("cmatra_decimals", 6)),
            quarantine_address=pool.get("quarantine_address"),
            redeemers={int(k): v for k, v in pool["redeemers"].items()},
            surrender_redeemer=pool["surrender_redeemer"],
            redemptions=tuple(Redemption(r["key"], int(r["numerator"]), int(r["denominator"]),
                                         r.get("unit"), r.get("policy_id"))
                              for r in pool.get("redemptions", [])),
        ) if pool else None,
    )


# --- Materios --------------------------------------------------------------------


class RuntimeDecoder:
    """Decodes extrinsics and ``System.Events`` against one runtime's metadata.

    Holding the metadata here, rather than inside a websocket client, lets a block
    whose state the node has already pruned be decoded against a pinned runtime.
    """

    def __init__(self, metadata_hex: str):
        self._config = RuntimeConfigurationObject(ss58_format=SS58_FORMAT)
        self._config.update_type_registry(load_type_registry_preset("core"))
        self._metadata = self._config.create_scale_object("MetadataVersioned", data=ScaleBytes(metadata_hex))
        self._metadata.decode()
        self._config.add_portable_registry(self._metadata)
        self._events_type = (self._metadata.get_metadata_pallet("System")
                             .get_storage_function("Events").get_value_type_string())

    def extrinsics(self, extrinsics_hex: list[str]) -> list[dict]:
        """Each extrinsic decoded, or ``{"undecodable": hex, "error": ...}`` for one this
        runtime's metadata cannot read, so a call the watcher does not know is reported
        rather than silently skipped."""
        decoded = []
        for x in extrinsics_hex:
            try:
                decoded.append(self._decode("Extrinsic", x))
            except Exception as e:  # scalecodec raises any type on bytes it cannot place
                decoded.append({"undecodable": x, "error": f"{type(e).__name__}: {e}"[:200]})
        return decoded

    def events(self, events_hex: str) -> dict[int, list[str]]:
        grouped: dict[int, list[str]] = defaultdict(list)
        for record in self._decode(self._events_type, events_hex):
            if record.get("phase") == "ApplyExtrinsic":
                grouped[record["extrinsic_idx"]].append(_event_text(record))
        return dict(grouped)

    def _decode(self, type_string: str, data_hex: str):
        obj = self._config.create_scale_object(type_string, data=ScaleBytes(data_hex), metadata=self._metadata)
        return obj.decode(check_remaining=True)


def _event_text(record: dict) -> str:
    name = f"{record['module_id']}.{record['event_id']}"
    attributes = record.get("attributes")
    if not isinstance(attributes, dict):
        return name
    results = {k: v for k, v in attributes.items() if k.endswith("result") or k == "dispatch_error"}
    if not results:
        return name
    return name + "(" + ", ".join(f"{k}={_result_text(v)}" for k, v in results.items()) + ")"


def _result_text(value) -> str:
    if isinstance(value, dict) and set(value) == {"Ok"}:
        return "Ok"
    if isinstance(value, dict) and set(value) == {"Err"}:
        return "Err:" + json.dumps(value["Err"], default=str)[:120]
    return json.dumps(value, default=str)[:120]


EVENT_NOISE = ("Motra.", "TransactionPayment.", "Balances.Withdraw", "Balances.Deposit")

_ANY = "*"
_CALL_SEVERITY: dict[tuple[str, str], Severity] = {
    ("Sudo", _ANY): CRITICAL,
    ("Recovery", _ANY): CRITICAL,
    **{("System", f): CRITICAL for f in (
        "set_heap_pages", "set_code", "set_code_without_checks", "set_storage", "kill_storage",
        "kill_prefix", "authorize_upgrade", "authorize_upgrade_without_checks", "apply_authorized_upgrade")},
    **{("Balances", f): CRITICAL for f in (
        "force_transfer", "force_unreserve", "force_set_balance", "force_adjust_total_issuance")},
    **{("Treasury", f): CRITICAL for f in ("spend_local", "remove_approval", "spend", "payout", "void_spend")},
    **{("Vesting", f): CRITICAL for f in ("force_vested_transfer", "force_remove_vesting_schedule")},
    ("Grandpa", "note_stalled"): CRITICAL,
    ("Grandpa", "report_equivocation"): ALERT,
    ("Grandpa", "report_equivocation_unsigned"): ALERT,
    ("NativeTokenManagement", "transfer_tokens"): ALERT,
    ("SessionCommitteeManagement", "set_main_chain_scripts"): CRITICAL,
    ("NativeTokenManagement", "set_main_chain_scripts"): CRITICAL,
    ("PalletSession", "set_keys"): ALERT,
    ("PalletSession", "purge_keys"): ALERT,
    **{("OrinqReceipts", f): CRITICAL for f in (
        "set_availability_cert", "set_committee", "join_committee", "leave_committee",
        "set_attestation_reward_per_signer", "set_era_cap_base", "set_era_cap_baseline_attestor_count",
        "slash_attestor", "set_bond_requirement", "set_receipt_submission_fee",
        "set_receipt_submission_fee_floor", "set_receipt_expiry_blocks", "set_bad_attest_slash_threshold",
        "reset_candidate_liveness", "set_core_eviction_enabled", "set_contribution_window_enabled",
        "set_break_glass_floor_enabled", "set_slack_invariant_enabled", "set_break_glass_aura_keys",
        "set_pinned_committee", "clear_pinned_committee")},
    ("Motra", "set_params"): CRITICAL,
    ("TeeAttestation", "set_disabled"): CRITICAL,
    ("Billing", "governance_set_endpoint_price"): CRITICAL,
    ("Billing", "governance_set_debits_enabled"): CRITICAL,
    ("PerpEngine", "governance_set_market"): CRITICAL,
    ("Oracle", "register_attestor"): CRITICAL,
    ("IntentSettlement", "set_pool_utilization"): CRITICAL,
    ("IntentSettlement", "set_min_signer_threshold"): CRITICAL,
    ("Utility", "dispatch_as"): CRITICAL,
    ("Utility", "with_weight"): CRITICAL,
}
_MULTISIG_CALLS = {"as_multi", "as_multi_threshold_1", "approve_as_multi", "cancel_as_multi"}
_BATCH_CALLS = {"batch", "batch_all", "force_batch"}


def account_bytes(account: str) -> bytes:
    if account.startswith("0x"):
        return bytes.fromhex(account[2:])
    return bytes.fromhex(ss58_decode(account))


def render_account(account: bytes) -> str:
    return ss58_encode(account.hex(), SS58_FORMAT)


def _compact(n: int) -> bytes:
    if n < 1 << 6:
        return bytes([n << 2])
    if n < 1 << 14:
        return ((n << 2) | 1).to_bytes(2, "little")
    raise ValueError(f"signatory count {n} exceeds any multisig limit")


def multisig_account(signatories: list[bytes], threshold: int) -> bytes:
    """``pallet_multisig::multi_account_id``: blake2_256 over the sorted signatories."""
    ordered = sorted(signatories)
    preimage = b"modlpy/utilisuba" + _compact(len(ordered)) + b"".join(ordered) + threshold.to_bytes(2, "little")
    return hashlib.blake2b(preimage, digest_size=32).digest()


def _args(call: dict) -> dict:
    return {a["name"]: a["value"] for a in call.get("call_args", [])}


def _is_call(value) -> bool:
    return isinstance(value, dict) and "call_module" in value


def _render_value(value) -> str:
    if isinstance(value, str) and value.startswith("0x") and len(value) > 2 + 2 * 64:
        raw = bytes.fromhex(value[2:])
        return f"<{len(raw)} bytes blake2_256 0x{hashlib.blake2b(raw, digest_size=32).hexdigest()}>"
    if isinstance(value, str):
        return value if len(value) <= 200 else json.dumps(value)[:200] + "\u2026"
    text = json.dumps(value, default=str)
    return text if len(text) <= 400 else text[:400] + "\u2026"


def _signer(ext: dict) -> bytes | None:
    address = ext.get("address")
    if isinstance(address, dict):
        address = address.get("Id")
    return account_bytes(address) if address else None


def _walk(call: dict, origin: bytes | None, sudo_key: bytes | None, depth: int, lines: list[str]) -> Severity | None:
    module, function = call["call_module"], call["call_function"]
    args = _args(call)
    rendered = ", ".join(f"{k}={_render_value(v)}" for k, v in args.items()
                         if not _is_call(v) and not (isinstance(v, list) and v and all(_is_call(x) for x in v)))
    lines.append(f"{'  ' * depth}{module}.{function}({rendered})")
    severity = _CALL_SEVERITY.get((module, function), _CALL_SEVERITY.get((module, _ANY)))

    # The origin an inner call dispatches with; None stands for Root or an account
    # this walk cannot name, where a multisig account cannot be derived.
    inner_origin = None
    if module == "Utility" and function in _BATCH_CALLS:
        inner_origin = origin
    elif module == "Sudo" and function == "sudo_as":
        inner_origin = account_bytes(args["who"])
    elif module == "Recovery" and function == "as_recovered":
        inner_origin = account_bytes(args["account"])
    elif module == "Multisig" and function in _MULTISIG_CALLS and origin is not None:
        threshold = 1 if function == "as_multi_threshold_1" else args["threshold"]
        signatories = [origin, *(account_bytes(s) for s in args["other_signatories"])]
        inner_origin = multisig_account(signatories, threshold)
        if sudo_key is not None and inner_origin == sudo_key:
            severity = CRITICAL
            lines.append(f"{'  ' * depth}  multisig account is Sudo.Key {render_account(sudo_key)}")
        else:
            lines.append(f"{'  ' * depth}  multisig account {render_account(inner_origin)}")

    for value in args.values():
        children = [value] if _is_call(value) else (value if isinstance(value, list) else [])
        for child in children:
            if _is_call(child):
                inner = _walk(child, inner_origin, sudo_key, depth + 1, lines)
                if inner is not None and (severity is None or inner > severity):
                    severity = inner
    return severity


RUNTIME_ENVIRONMENT_UPDATED = "0x08"


def runtime_upgraded(header: dict) -> bool:
    """Whether this block changed the runtime, so the blocks after it need new metadata."""
    return RUNTIME_ENVIRONMENT_UPDATED in header["digest"]["logs"]


def classify_materios_block(chain: str, number: int, extrinsics: list[dict],
                            events: dict[int, list[str]] | None, sudo_key: bytes | None) -> list[Finding]:
    findings = []
    for index, ext in enumerate(extrinsics):
        if "undecodable" in ext:
            raw = bytes.fromhex(ext["undecodable"][2:])
            findings.append(Finding(
                severity=ALERT,
                key=f"{chain}:{number}:{index}",
                headline=f"{chain} #{number} extrinsic {index} could not be decoded against the runtime metadata",
                details=(f"{len(raw)} bytes blake2_256 0x{hashlib.blake2b(raw, digest_size=32).hexdigest()}",
                         ext["error"]),
            ))
            continue
        signer = _signer(ext)
        lines: list[str] = []
        severity = _walk(ext["call"], signer, sudo_key, 0, lines)
        notes = [f"signer {render_account(signer)}" if signer else "unsigned"]
        if signer is not None and signer == sudo_key:
            severity = CRITICAL
            notes.append("signed by Sudo.Key")
        if severity is None:
            continue
        if events is None:
            notes.append("events unavailable (block state pruned): dispatch result not verified")
        else:
            shown = [e for e in events.get(index, []) if not e.startswith(EVENT_NOISE)]
            notes.append("result: " + (", ".join(shown) if shown else "no events"))
        call = ext["call"]
        findings.append(Finding(
            severity=severity,
            key=f"{chain}:{number}:{index}",
            headline=f"{chain} #{number} extrinsic {index}: {call['call_module']}.{call['call_function']}",
            details=tuple(notes + lines),
        ))
    return findings


def committee_of(extrinsics: list[dict]) -> tuple[tuple[str, ...], ...] | None:
    for ext in extrinsics:
        call = ext.get("call")
        if call is None:
            continue
        if (call["call_module"], call["call_function"]) == ("SessionCommitteeManagement", "set"):
            members = []
            for member in _args(call)["validators"]:
                cross_chain, keys = member[0], member[1]
                members.append((cross_chain, *(f"{k}={keys[k]}" for k in sorted(keys))))
            return tuple(sorted(members))
    return None


def committee_change(chain: str, number: int, previous: tuple, current: tuple) -> Finding:
    key = f"{chain}:{number}:committee"
    if previous == current:
        return Finding(INFO, key, f"{chain} #{number}: committee rotation, membership unchanged", kind="committee")
    added = sorted(set(current) - set(previous))
    removed = sorted(set(previous) - set(current))
    return Finding(
        ALERT, key, f"{chain} #{number}: validator committee membership changed",
        details=tuple([f"+ {' '.join(m)}" for m in added] + [f"- {' '.join(m)}" for m in removed]),
        kind="committee",
    )


# --- Cardano ---------------------------------------------------------------------


def _value(utxos: list[dict]) -> dict[str, int]:
    total: dict[str, int] = defaultdict(int)
    for u in utxos:
        for a in u["amount"]:
            total[a["unit"]] += int(a["quantity"])
    return total


def _delta(after: dict[str, int], before: dict[str, int]) -> dict[str, int]:
    units = set(after) | set(before)
    return {u: after.get(u, 0) - before.get(u, 0) for u in units if after.get(u, 0) != before.get(u, 0)}


def _payment_credential(address: str) -> tuple[bool, bytes] | None:
    """(is_script, credential) for a Shelley address; None for anything else."""
    try:
        _hrp, raw = decode_cardano_address(address)
    except ValueError:
        return None
    if len(raw) < 29 or raw[0] >> 4 > 7:
        return None
    return bool((raw[0] >> 4) & 1), raw[1:29]


def _script_hash(address: str) -> str | None:
    credential = _payment_credential(address)
    return credential[1].hex() if credential and credential[0] else None


class _Names:
    def __init__(self, network: CardanoNetwork):
        self._names: dict[str, tuple[str, int]] = {"lovelace": ("ADA", 6)}
        if network.pool:
            self._names[network.pool.cmatra_unit] = ("cMATRA", network.pool.cmatra_decimals)
            for r in network.pool.redemptions:
                if r.unit:
                    self._names[r.unit] = (r.key, 0)
        self._policies = {r.policy_id: r.key for r in network.pool.redemptions if r.policy_id} if network.pool else {}

    def quantity(self, unit: str, quantity: int) -> str:
        if unit in self._names:
            name, decimals = self._names[unit]
        elif unit[:56] in self._policies:
            name, decimals = f"{self._policies[unit[:56]]}#{bytes.fromhex(unit[56:]).hex()[:16]}", 0
        else:
            name, decimals = f"{unit[:16]}\u2026", 0
        sign = "-" if quantity < 0 else ""
        whole, frac = divmod(abs(quantity), 10 ** decimals) if decimals else (abs(quantity), 0)
        text = f"{whole:,}" + (f".{frac:0{decimals}d}" if decimals else "")
        return f"{sign}{text} {name}"

    def value(self, value: dict[str, int]) -> str:
        parts = [self.quantity(u, q) for u, q in sorted(value.items(), key=lambda kv: kv[0] != "lovelace") if q]
        return ", ".join(parts) if parts else "nothing"


def _entitlement(deposited: dict[str, int], redemptions: tuple[Redemption, ...]) -> tuple[int, dict[str, int], dict[str, int]]:
    """(entitlement, count per redemption key, unrecognized units) for a quarantine deposit."""
    counts: dict[str, int] = defaultdict(int)
    unknown: dict[str, int] = {}
    by_unit = {r.unit: r for r in redemptions if r.unit}
    by_policy = {r.policy_id: r for r in redemptions if r.policy_id}
    for unit, quantity in deposited.items():
        if unit == "lovelace" or quantity <= 0:
            continue
        rule = by_unit.get(unit) or by_policy.get(unit[:56])
        if rule is None:
            unknown[unit] = quantity
        else:
            counts[rule.key] += quantity
    rates = {r.key: r for r in redemptions}
    total = sum(counts[k] * rates[k].numerator // rates[k].denominator for k in counts)
    return total, dict(counts), unknown


def _redeemer_name(pool: SurrenderPool, json_value) -> str:
    if isinstance(json_value, dict) and "constructor" in json_value:
        return pool.redeemers.get(json_value["constructor"], f"constructor {json_value['constructor']}")
    return f"unrecognized redeemer {json.dumps(json_value)[:80]}"


def _classify_pool(network: CardanoNetwork, spent: list[dict], produced: list[dict],
                   redeemers: list[dict], names: _Names) -> tuple[Severity, list[str], int]:
    pool = network.pool
    lines: list[tuple[Severity, str]] = []
    pool_in = [u for u in spent if u["address"] == pool.address]
    pool_out = [u for u in produced if u["address"] == pool.address]

    script = _script_hash(pool.address)
    actions = [_redeemer_name(pool, r.get("json_value")) for r in redeemers
               if r.get("purpose") == "spend" and r.get("script_hash") == script]
    for action in sorted(set(actions)) or ["no redeemer"]:
        if action != pool.surrender_redeemer:
            lines.append((CRITICAL, f"{pool.label} spent with {action}"))

    leaving = _delta(_value(pool_in), _value(pool_out))
    paid = leaving.pop(pool.cmatra_unit, 0)
    leaving.pop("lovelace", None)
    if leaving:
        lines.append((CRITICAL, f"non-cMATRA value moved in {pool.label}: {names.value(leaving)}"))
    for out in pool_out:
        if not out.get("inline_datum"):
            lines.append((CRITICAL, f"{pool.label} output left without an inline datum (unspendable)"))

    claimants = set()
    for u in spent:
        credential = _payment_credential(u["address"]) if u["address"] != pool.address else None
        if credential and not credential[0]:
            claimants.add(credential[1])
    custody = {c[1] for c in (_payment_credential(w.address) for w in network.addresses if w.role == "custody") if c}
    if claimants & custody:
        lines.append((CRITICAL, "a custody wallet funded this pool spend as the claimant"))

    to_others: dict[str, int] = defaultdict(int)
    for out in produced:
        if out["address"] == pool.address:
            continue
        quantity = _value([out]).get(pool.cmatra_unit, 0)
        credential = _payment_credential(out["address"])
        if quantity and not (credential and not credential[0] and credential[1] in claimants):
            to_others[out["address"]] += quantity
    if to_others:
        recipients = ", ".join(f"{a[:24]}\u2026 {names.quantity(pool.cmatra_unit, q)}" for a, q in to_others.items())
        lines.append((CRITICAL, f"cMATRA paid to a non-claimant: {recipients}"))

    deposited: dict[str, int] = {}
    if pool.quarantine_address:
        deposited = _delta(_value([u for u in produced if u["address"] == pool.quarantine_address]),
                           _value([u for u in spent if u["address"] == pool.quarantine_address]))
    entitlement, counts, unknown = _entitlement(deposited, pool.redemptions)
    if unknown:
        lines.append((ALERT, f"unrecognized asset surrendered: {names.value(unknown)}"))
    if paid <= 0 and not counts:
        lines.append((CRITICAL, f"{pool.label} spent without a surrender (cMATRA out {names.quantity(pool.cmatra_unit, paid)})"))
    elif paid > entitlement:
        lines.append((CRITICAL, f"overpaid: {names.quantity(pool.cmatra_unit, paid)} left the pool against an "
                                f"entitlement of {names.quantity(pool.cmatra_unit, entitlement)} for {dict(counts)}"))
    elif paid < entitlement:
        lines.append((ALERT, f"underpaid: {names.quantity(pool.cmatra_unit, paid)} against an entitlement of "
                             f"{names.quantity(pool.cmatra_unit, entitlement)} for {dict(counts)}"))
    if not lines:
        lines.append((INFO, f"surrender: {names.quantity(pool.cmatra_unit, paid)} for {dict(counts)}"))
    return max(s for s, _ in lines), [t for _, t in lines], paid


def classify_cardano_tx(network: CardanoNetwork, tx: dict, utxos: dict, redeemers: list[dict]) -> Finding | None:
    valid = tx.get("valid_contract", True)
    spent = [u for u in utxos["inputs"] if not u.get("reference") and bool(u.get("collateral")) != valid]
    referenced = [u for u in utxos["inputs"] if u.get("reference")]
    produced = [u for u in utxos["outputs"] if bool(u.get("collateral")) != valid]
    names = _Names(network)
    lines: list[tuple[Severity, str]] = []
    kind, amount = "event", 0

    if not valid:
        lines.append((ALERT, "phase-2 script failure: only collateral moved"))

    for watched in network.addresses:
        out = _value([u for u in spent if u["address"] == watched.address])
        into = _value([u for u in produced if u["address"] == watched.address])
        custody = watched.role == "custody"
        if out:
            lines.append((CRITICAL if custody else watched.severity,
                          f"{'outflow from' if custody else 'spent from'} {watched.label}: "
                          f"net {names.value(_delta(into, out))}"))
        elif into:
            lines.append((ALERT if custody else watched.severity,
                          f"{'inflow to' if custody else 'paid to'} {watched.label}: {names.value(into)}"))
        elif any(u["address"] == watched.address for u in referenced):
            lines.append((INFO, f"{watched.label} read as a reference input"))

    minted = _delta(_value(produced), _value(spent))
    minted.pop("lovelace", None)
    for policy in network.policies:
        for unit, quantity in sorted(minted.items()):
            if unit.startswith(policy.policy_id):
                verb = "minted" if quantity > 0 else "burned"
                lines.append((policy.severity, f"{verb} {names.quantity(unit, abs(quantity))} under {policy.label}"))

    pool = network.pool
    if pool and any(u["address"] == pool.address for u in spent):
        severity, pool_lines, paid = _classify_pool(network, spent, produced, redeemers, names)
        lines.extend((severity, t) for t in pool_lines)
        if severity == INFO:
            kind, amount = "surrender", paid
    elif pool and any(u["address"] == pool.address for u in produced):
        received = _value([u for u in produced if u["address"] == pool.address])
        lines.append((ALERT, f"{pool.label} received value outside a pool spend: {names.value(received)}"))

    if not lines:
        return None
    when = datetime.fromtimestamp(tx["block_time"], timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    return Finding(
        severity=max(s for s, _ in lines),
        key=f"{network.name}:{tx['hash']}",
        headline=f"{network.name} tx {tx['hash']} (block {tx['block_height']}, {when})",
        details=tuple(t for _, t in lines),
        kind=kind,
        amount=amount,
    )
