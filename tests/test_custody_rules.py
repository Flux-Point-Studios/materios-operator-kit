"""Custody and authority-move classification against real chain history.

Materios fixtures are complete finalized blocks from the live preprod chain,
decoded with the live runtime's metadata. Cardano fixtures are Blockfrost
responses for real transactions; wallet (key-hash) credentials and transaction
ids are pseudonymized, while contract addresses, policies, amounts, datums and
redeemers are as they are on chain. The two custody-wallet fixtures also carry a
synthetic block position, and the outflow synthetic amounts. The failed-script
fixture is a preprod transaction exactly as Blockfrost serves it.
"""

import copy
import gzip
import hashlib
import json
import tracemalloc
from datetime import datetime
from pathlib import Path

import pytest
import scalecodec.types

from daemon import custody_rules as rules

FIX = Path(__file__).parent / "fixtures" / "custody"
SUDO_KEY = "5H2M5Dbt8hSfSCXS6hfEBPR1N21yh679finzcfMEwD62i7iP"
SPEC_238_CODE_HASH = "0xae5e94cef78cb63c58079c46b32b11e6f8c371ea9a701feeb5a4600c0e76edb4"
# The runtime's RuntimeBlockLength: 5 MiB, of which 75% is open to normal extrinsics.
NORMAL_BLOCK_LENGTH = 5 * 1024 * 1024 * 3 // 4


def _read(name: str) -> bytes:
    raw = (FIX / name).read_bytes()
    return gzip.decompress(raw) if name.endswith(".gz") else raw


@pytest.fixture(scope="module")
def decoder():
    return rules.RuntimeDecoder(_read("materios/metadata_spec238.hex.gz").decode())


def _block(name: str) -> dict:
    return json.loads(_read(f"materios/{name}"))


def _findings(decoder, name: str, sudo_key: str | None = SUDO_KEY):
    block = _block(name)
    extrinsics = decoder.extrinsics(block["extrinsics"])
    return rules.classify_materios_block(
        "materios-preprod", block["number"], extrinsics,
        read_events=lambda: None, sudo_key=rules.account_bytes(sudo_key) if sudo_key else None,
    )


# --- Materios -----------------------------------------------------------------


def test_the_sudo_multisig_account_is_derived_from_the_signatories(decoder):
    ext = decoder.extrinsics(_block("block_1829210.json")["extrinsics"])[2]
    args = {a["name"]: a["value"] for a in ext["call"]["call_args"]}
    signatories = [ext["address"], *args["other_signatories"]]
    account = rules.multisig_account([rules.account_bytes(s) for s in signatories], args["threshold"])
    assert account == rules.account_bytes(SUDO_KEY)


@pytest.mark.parametrize("block", ["block_1829210.json", "block_1829226.json"])
def test_each_leg_of_the_runtime_upgrade_authorization_is_critical(decoder, block):
    [finding] = _findings(decoder, block)
    text = finding.render()
    assert finding.severity == rules.CRITICAL
    assert finding.key == f"materios-preprod:{block[6:13]}:2"
    assert "Multisig.as_multi" in text and "Sudo.sudo" in text
    assert "System.authorize_upgrade" in text and SPEC_238_CODE_HASH in text
    assert "multisig account is Sudo.Key" in text


def test_applying_the_upgrade_is_critical_and_names_the_code_hash(decoder):
    [finding] = _findings(decoder, "block_1829227.json.gz")
    text = finding.render()
    assert finding.severity == rules.CRITICAL
    assert "System.apply_authorized_upgrade" in text
    assert "unsigned" in text
    assert "845484 bytes" in text and SPEC_238_CODE_HASH in text


@pytest.mark.parametrize(
    "block, expected",
    [
        ("block_1645150.json", ["Recovery.create_recovery", "multisig account is Sudo.Key"]),
        ("block_1587358.json", ["Sudo.set_key", SUDO_KEY]),
        ("block_1570580.json", ["Utility.batch_all", "OrinqReceipts.set_break_glass_aura_keys",
                                "OrinqReceipts.set_break_glass_floor_enabled"]),
        ("block_1292725.json", ["OrinqReceipts.reset_candidate_liveness"]),
        ("block_735803.json", ["System.set_storage"]),
        ("block_534612.json", ["Grandpa.note_stalled"]),
        ("block_95132.json", ["TeeAttestation.set_disabled"]),
    ],
)
def test_historical_root_and_custody_calls_are_critical(decoder, block, expected):
    [finding] = _findings(decoder, block)
    assert finding.severity == rules.CRITICAL
    for fragment in expected:
        assert fragment in finding.render()


def test_routine_traffic_raises_nothing(decoder):
    for name in ("block_2029707.json", "block_2034370_with_events.json"):
        assert _findings(decoder, name) == []


def test_the_committee_rotation_inherent_yields_the_seated_committee(decoder):
    extrinsics = decoder.extrinsics(_block("block_2029707.json")["extrinsics"])
    committee = rules.committee_of(extrinsics)
    assert committee is not None and len(committee) == 5


def test_an_unchanged_committee_rotation_goes_to_the_digest(decoder):
    committee = rules.committee_of(decoder.extrinsics(_block("block_2029707.json")["extrinsics"]))
    finding = rules.committee_change("materios-preprod", 2029707, committee, committee)
    assert finding.severity == rules.INFO and finding.kind == "committee"


def test_a_changed_committee_is_an_alert(decoder):
    committee = rules.committee_of(decoder.extrinsics(_block("block_2029707.json")["extrinsics"]))
    previous = committee[1:]
    finding = rules.committee_change("materios-preprod", 2029707, previous, committee)
    assert finding.severity == rules.ALERT
    assert committee[0][0] in finding.render()


def test_an_extrinsic_the_runtime_metadata_cannot_decode_pages_critical_grouped_per_source(decoder):
    block = _block("block_2029707.json")
    garbage = "0x" + (bytes([12, 4, 0xFE, 0x01]) + bytes(2)).hex()
    extrinsics = decoder.extrinsics(block["extrinsics"] + [garbage])
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              read_events=lambda: None, sudo_key=None)
    assert finding.severity == rules.CRITICAL
    assert finding.group == "materios-preprod unclassifiable"
    assert finding.key == f"materios-preprod:{block['number']}:undecoded"
    assert finding.headline == f"materios-preprod #{block['number']}: 1 extrinsic could not be decoded"
    assert finding.details == (f"extrinsic {len(block['extrinsics'])}: unsigned: 6 bytes blake2_256 "
                               f"0x{hashlib.blake2b(bytes.fromhex(garbage[2:]), digest_size=32).hexdigest()}: "
                               f"{extrinsics[-1]['error']}",)


UTILITY_BATCH = bytes([8, 0])
AUTHORIZE_UPGRADE = bytes([0, 9])


def nested_sudo_leg(levels: int) -> str:
    """The spec-238 propose leg as block 1829210 carries it, signature and all, with the
    System.authorize_upgrade its Sudo.sudo wraps nested ``levels`` Utility.batch calls deep."""
    raw = bytes.fromhex(_block("block_1829210.json")["extrinsics"][2][2:])
    body = raw[2:]
    assert raw[:2] == rules._compact(len(body))
    at = body.index(AUTHORIZE_UPGRADE + bytes.fromhex(SPEC_238_CODE_HASH[2:]))
    call = body[at:at + 34]
    for _ in range(levels):
        call = UTILITY_BATCH + rules._compact(1) + call
    body = body[:at] + call + body[at + 34:]
    return "0x" + (rules._compact(len(body)) + body).hex()


@pytest.mark.parametrize("levels", [0, 1, 250])
def test_a_sudo_leg_nested_as_deep_as_a_runtime_decodes_is_read_and_named(decoder, levels):
    # A runtime decodes an extrinsic nested up to MAX_EXTRINSIC_DEPTH (256) deep.
    leg = nested_sudo_leg(levels)
    if levels == 0:
        assert leg == _block("block_1829210.json")["extrinsics"][2]
    [ext] = decoder.extrinsics([leg])
    assert "undecodable" not in ext
    [finding] = rules.classify_materios_block("materios-preprod", 9, [ext], lambda: None,
                                              rules.account_bytes(SUDO_KEY))
    text = finding.render()
    assert finding.severity == rules.CRITICAL and finding.group is None
    assert finding.headline.endswith("System.authorize_upgrade")
    assert "Sudo.sudo" in text and "multisig account is Sudo.Key" in text
    # The call's own arguments lead the details however deep it sits in the tree, and
    # the finding stays small.
    assert SPEC_238_CODE_HASH in "\n".join(finding.details[:5])
    assert len(text) < 20_000


LEG_BLOCK = "block_1829210.json"
# Where the propose leg's call begins: after its compact length, version byte,
# MultiAddress::Id tag, signer, signature and signed extensions.
LEG_CALL_AT = 103
SUDO_SUDO, SUDO_UNCHECKED_WEIGHT = bytes([6, 0]), bytes([6, 1])
SYSTEM_REMARK, SYSTEM_SET_CODE, SYSTEM_KILL_STORAGE = bytes([0, 0]), bytes([0, 2]), bytes([0, 5])
STRANGER = bytes(range(32))


def signed_extrinsic(call: bytes, signer: bytes) -> str:
    """``call`` behind the propose leg's version byte, signature and signed extensions, with
    ``signer`` as its signer. The watcher reads a signature but never checks it."""
    raw = bytes.fromhex(_block(LEG_BLOCK)["extrinsics"][2][2:])
    body = raw[2:4] + signer + raw[36:LEG_CALL_AT] + call
    return "0x" + (rules._compact(len(body)) + body).hex()


def filler_call(shape: str, size: int) -> bytes:
    """A call of about ``size`` bytes that any funded account can have finalized: an inert
    Sudo.sudo_unchecked_weight(System.kill_storage) of empty keys, which costs no weight;
    a Utility.batch of empty remarks; or a Sudo.sudo(System.set_code) of code-sized bytes."""
    if shape == "keys":
        return SUDO_UNCHECKED_WEIGHT + SYSTEM_KILL_STORAGE + rules._compact(size) + bytes(size) + bytes(2)
    if shape == "remarks":
        return UTILITY_BATCH + rules._compact(size // 3) + (SYSTEM_REMARK + rules._compact(0)) * (size // 3)
    return SUDO_SUDO + SYSTEM_SET_CODE + rules._compact(size) + b"\xff" * size


def test_a_signed_extrinsic_is_rebuilt_byte_for_byte_from_its_call_and_signer():
    leg = _block(LEG_BLOCK)["extrinsics"][2]
    raw = bytes.fromhex(leg[2:])
    assert signed_extrinsic(raw[LEG_CALL_AT:], raw[4:36]) == leg


@pytest.mark.parametrize("shape", ["keys", "remarks", "code"])
def test_a_block_filled_to_its_length_limit_is_read_within_the_decode_budget(decoder, shape):
    block = _block(LEG_BLOCK)
    size = NORMAL_BLOCK_LENGTH - 1024
    hexes = [*block["extrinsics"][:2], signed_extrinsic(filler_call(shape, size), STRANGER),
             *block["extrinsics"][2:]]
    before = decoder.values

    def classify():
        return rules.classify_materios_block("materios-preprod", block["number"], decoder.extrinsics(hexes),
                                             lambda: None, rules.account_bytes(SUDO_KEY))

    findings, peak = _peak_memory(classify)
    found = {f.key.rsplit(":", 1)[1]: f for f in findings}
    assert found["3"].severity == rules.CRITICAL and found["3"].headline.endswith("System.authorize_upgrade")
    if shape == "code":
        assert found["2"].headline.endswith("Sudo.sudo > System.set_code")
        assert f"<{size} bytes blake2_256" in found["2"].render()
    else:
        [line] = [d for d in found["ordinary"].details if d.startswith("extrinsic ")]
        assert found["ordinary"].severity == rules.ALERT
        assert line.startswith("extrinsic 2: ") and "DecodeBudgetExceeded" in line
    assert peak < 64 * 1024 * 1024
    assert decoder.values - before <= rules.DECODE_BUDGET


def test_decoding_a_batch_hashes_each_call_once_not_every_byte_after_it(decoder, monkeypatch):
    # scalecodec hashes every call it decodes; left alone it hashes from the call's start
    # to the end of the extrinsic, so a batch's decode grows with its length squared.
    hashed = []

    def counting(data, digest_size):
        hashed.append(len(data))
        return hashlib.blake2b(b"", digest_size=digest_size)

    monkeypatch.setattr(scalecodec.types, "blake2b", counting)
    extrinsic = signed_extrinsic(filler_call("remarks", 9_000), STRANGER)
    [ext] = decoder.extrinsics([extrinsic])
    assert "undecodable" not in ext and len(rules._args(ext["call"])["calls"]) == 3_000
    # Each byte once in its own call, once in the extrinsic's call and once in the extrinsic.
    assert sum(hashed) <= 3 * (len(extrinsic) - 2) // 2


def test_decoding_stops_where_the_blocks_budget_runs_out(decoder, monkeypatch):
    monkeypatch.setattr(rules, "DECODE_BUDGET", 200)
    leg = _block(LEG_BLOCK)["extrinsics"][2]
    filler = signed_extrinsic(filler_call("keys", 1000), STRANGER)
    before = decoder.values
    decoded_filler, decoded_leg = decoder.extrinsics([filler, leg])
    assert decoded_filler["undecodable"] == filler
    assert decoded_filler["error"] == "DecodeBudgetExceeded: stopped at the budget of 200 decoded values per block"
    assert decoded_leg["call"]["call_module"] == "Multisig"
    assert decoder.values - before == 200


def test_a_blocks_inherents_are_decoded_before_any_signed_extrinsic_however_small(decoder, monkeypatch):
    # The timestamp, committee rotation and block beneficiary inherents decode into 74 values.
    monkeypatch.setattr(rules, "DECODE_BUDGET", 80)
    remark = signed_extrinsic(SYSTEM_REMARK + rules._compact(0), STRANGER)
    extrinsics = decoder.extrinsics(_block("block_2029707.json")["extrinsics"] + [remark] * 10)
    assert rules.committee_of(extrinsics) is not None
    assert sum("undecodable" in e for e in extrinsics) == 10


def test_what_the_budget_did_not_reach_from_accounts_with_no_authority_pages_one_alert_per_block(decoder,
                                                                                                monkeypatch):
    # Any funded account can send an extrinsic the budget does not reach, and none of
    # these signers can move custody or authority whatever it holds.
    monkeypatch.setattr(rules, "DECODE_BUDGET", 100)
    fillers = [signed_extrinsic(filler_call("keys", 1000 + i), STRANGER) for i in range(30)]
    extrinsics = decoder.extrinsics(_block("block_2029707.json")["extrinsics"] + fillers)
    [finding] = rules.classify_materios_block("materios-preprod", 2029707, extrinsics, lambda: None, None)
    assert finding.key == "materios-preprod:2029707:undecoded:ordinary"
    assert finding.severity == rules.ALERT and finding.group == "materios-preprod unclassifiable"
    assert finding.headline == ("materios-preprod #2029707: 30 extrinsics could not be decoded, signed by accounts "
                                "with no authority")
    assert finding.details[1].startswith("extrinsic 3: ") and "DecodeBudgetExceeded" in finding.details[1]
    assert len(finding.details) == rules.MAX_UNDECODED_LINES + 2 and finding.details[-1] == "... 10 more"


LEG_SIGNER = bytes.fromhex(_block(LEG_BLOCK)["extrinsics"][2][2:])[4:36]


def remark_filler() -> str:
    """A signed Utility.batch of 55 empty remarks, which any funded account can have
    finalized: 271 bytes, smaller than the 281-byte propose leg."""
    return signed_extrinsic(filler_call("remarks", 165), STRANGER)


def fillers_past_the_budget(decoder) -> list[str]:
    """Enough remark fillers to spend a whole DECODE_BUDGET before a larger extrinsic."""
    filler = remark_filler()
    before = decoder.values
    decoder.extrinsics([filler])
    return [filler] * (rules.DECODE_BUDGET // (decoder.values - before) + 1)


@pytest.mark.parametrize("signer", ["authority", "Sudo.Key"])
def test_what_sudo_key_or_an_authority_signs_is_decoded_whatever_smaller_filler_the_block_holds(decoder, signer):
    block = _block(LEG_BLOCK)
    sudo_key = rules.account_bytes(SUDO_KEY)
    leg = block["extrinsics"][2]
    authorities = frozenset({LEG_SIGNER})
    if signer == "Sudo.Key":
        leg, authorities = signed_extrinsic(bytes.fromhex(leg[2:])[LEG_CALL_AT:], sudo_key), frozenset()
    fillers = fillers_past_the_budget(decoder)
    assert len(fillers[0]) < len(leg)
    extrinsics = decoder.extrinsics([*block["extrinsics"][:2], leg, *fillers],
                                    rules.accountable(sudo_key, authorities))
    found = {f.key.rsplit(":", 1)[1]: f for f in rules.classify_materios_block(
        "materios-preprod", block["number"], extrinsics, lambda: None, sudo_key, authorities)}
    text = found["2"].render()
    assert found["2"].severity == rules.CRITICAL and found["2"].group is None
    assert "System.authorize_upgrade" in text
    assert ("multisig account is Sudo.Key" if signer == "authority" else "signed by Sudo.Key") in text
    assert found["ordinary"].group == "materios-preprod unclassifiable"


def test_what_sudo_key_or_an_authority_signed_and_cannot_be_decoded_pages_alone_per_signer(decoder, monkeypatch):
    # The propose leg decodes into 36 values, more than this budget holds.
    monkeypatch.setattr(rules, "DECODE_BUDGET", 30)
    block = _block(LEG_BLOCK)
    sudo_key = rules.account_bytes(SUDO_KEY)
    authorities = frozenset({LEG_SIGNER})
    hexes = [*block["extrinsics"], remark_filler(), signed_extrinsic(filler_call("keys", 1000), LEG_SIGNER)]
    extrinsics = decoder.extrinsics(hexes, rules.accountable(sudo_key, authorities))
    found = {f.key: f for f in rules.classify_materios_block(
        "materios-preprod", block["number"], extrinsics, lambda: None, sudo_key, authorities)}
    signer, filler = rules.render_account(LEG_SIGNER), len(block["extrinsics"])
    legs = found[f"materios-preprod:1829210:undecoded:{signer}"]
    assert legs.severity == rules.CRITICAL and legs.group is None
    assert legs.headline == "materios-preprod #1829210: 2 extrinsics could not be decoded, signed by an authority account"
    assert legs.details[0].startswith(f"extrinsic 2: signer {signer}: 281 bytes blake2_256 0x")
    assert legs.details[1].startswith(f"extrinsic {filler + 1}: signer {signer}: ")
    assert all("DecodeBudgetExceeded" in line for line in legs.details)
    grouped = found["materios-preprod:1829210:undecoded:ordinary"]
    assert grouped.group == "materios-preprod unclassifiable"
    assert any(line.startswith(f"extrinsic {filler}: signer {rules.render_account(STRANGER)}: 271 bytes")
               for line in grouped.details)
    assert signer not in "\n".join(grouped.details)


def test_events_that_would_pass_the_budget_are_not_decoded(decoder, monkeypatch):
    events = _block("block_2034370_with_events.json")["events"]
    monkeypatch.setattr(rules, "DECODE_BUDGET", 50)
    with pytest.raises(rules.DecodeBudgetExceeded):
        decoder.events(events)


def test_a_budget_reached_inside_a_wrapper_that_catches_every_error_still_stops_the_decode(decoder):
    # scalecodec's OpaqueCall decodes its bytes as a call inside a bare ``except`` and keeps
    # the bytes when that fails. No spec-238 type uses it; a runtime could.
    call = filler_call("keys", 300)
    with pytest.raises(rules.DecodeBudgetExceeded):
        decoder._decode("OpaqueCall", "0x" + (rules._compact(len(call)) + call).hex(), 20)


def test_a_runtime_upgrade_is_read_from_the_block_digest():
    headers = json.loads(_read("materios/headers_spec238_upgrade.json"))
    assert rules.runtime_upgraded(headers["1829227"])
    assert not rules.runtime_upgraded(headers["1829226"])


def test_events_are_grouped_by_extrinsic(decoder):
    block = _block("block_2034370_with_events.json")
    events = decoder.events(block["events"])
    assert all(isinstance(i, int) for i in events)
    assert "System.ExtrinsicSuccess" in events[0]


def test_dispatch_results_are_attached_to_the_alert(decoder):
    block = _block("block_1829226.json")
    extrinsics = decoder.extrinsics(block["extrinsics"])
    events = {2: ["Multisig.MultisigExecuted(result=Ok)", "Sudo.Sudid(sudo_result=Ok)", "System.UpgradeAuthorized"]}
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              read_events=lambda: events, sudo_key=rules.account_bytes(SUDO_KEY))
    assert "Sudo.Sudid(sudo_result=Ok)" in finding.render()


def _count_walks(monkeypatch) -> list[str]:
    """The calls ``_walk`` is entered with at the top of an extrinsic's tree, as they come."""
    walked = []
    walk = rules._walk

    def counting(call, origin, tree, depth, *rest):
        if depth == 0:
            walked.append(f"{call['call_module']}.{call['call_function']}")
        return walk(call, origin, tree, depth, *rest)

    monkeypatch.setattr(rules, "_walk", counting)
    return walked


def test_events_are_read_once_and_only_for_a_block_with_something_to_report(decoder, monkeypatch):
    walked = _count_walks(monkeypatch)
    reads = []

    def read_events():
        reads.append(True)
        return {2: ["Multisig.MultisigExecuted(result=Ok)", "Sudo.Sudid(sudo_result=Ok)", "System.ExtrinsicSuccess"]}

    block = _block("block_1829226.json")
    extrinsics = decoder.extrinsics(block["extrinsics"])
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics, read_events,
                                              rules.account_bytes(SUDO_KEY))
    assert "Sudo.Sudid(sudo_result=Ok)" in finding.render()
    assert len(reads) == 1 and len(walked) == len(extrinsics)

    reads.clear()
    routine = decoder.extrinsics(_block("block_2029707.json")["extrinsics"])
    assert rules.classify_materios_block("materios-preprod", 2029707, routine, read_events,
                                         rules.account_bytes(SUDO_KEY)) == []
    assert reads == []


def test_a_block_whose_state_is_pruned_says_so(decoder):
    block = _block("block_1829226.json")
    extrinsics = decoder.extrinsics(block["extrinsics"])
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              read_events=lambda: None, sudo_key=None)
    assert "events unavailable" in finding.render()


# No privileged call of these kinds has ever been dispatched on the chain, so these
# use the decoder's output shape rather than a captured block.
def _signed(signer: str, module: str, function: str, **args) -> dict:
    return {"address": signer, "call": _call(module, function, **args)}


def _call(module: str, function: str, **args) -> dict:
    return {"call_module": module, "call_function": function,
            "call_args": [{"name": k, "type": "", "value": v} for k, v in args.items()]}


ALICE = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"


@pytest.mark.parametrize(
    "call, severity",
    [
        (_call("Balances", "force_set_balance", who=ALICE, new_free=1), rules.CRITICAL),
        (_call("Balances", "force_transfer", source=ALICE, dest=ALICE, value=1), rules.CRITICAL),
        (_call("Treasury", "spend_local", amount=1, beneficiary=ALICE), rules.CRITICAL),
        (_call("Treasury", "payout", index=0), rules.CRITICAL),
        (_call("SessionCommitteeManagement", "set_main_chain_scripts", main_chain_scripts={}), rules.CRITICAL),
        (_call("PalletSession", "set_keys", keys={}, proof="0x"), rules.ALERT),
        (_call("Grandpa", "report_equivocation", equivocation_proof={}, key_owner_proof={}), rules.ALERT),
        (_call("NativeTokenManagement", "transfer_tokens", token_amount=1), rules.ALERT),
        (_call("Utility", "dispatch_as", as_origin={"system": "Root"},
               call=_call("Balances", "transfer_keep_alive", dest=ALICE, value=1)), rules.CRITICAL),
    ],
)
def test_privileged_calls_without_history_are_classified(call, severity):
    # Unsigned, so no account's origin keeps a root-gated call from counting.
    [finding] = rules.classify_materios_block("materios-preprod", 1, [{"call": call}],
                                              read_events=lambda: None, sudo_key=None)
    assert finding.severity == severity


def test_anything_signed_by_the_sudo_key_is_critical():
    ext = _signed(SUDO_KEY, "Balances", "transfer_keep_alive", dest=ALICE, value=1)
    [finding] = rules.classify_materios_block("materios-preprod", 1, [ext], read_events=lambda: None,
                                              sudo_key=rules.account_bytes(SUDO_KEY))
    assert finding.severity == rules.CRITICAL
    assert "signed by Sudo.Key" in finding.render()


def test_a_multisig_that_is_not_sudo_and_wraps_nothing_privileged_is_ignored():
    ext = _signed(ALICE, "Multisig", "as_multi_threshold_1", other_signatories=[SUDO_KEY],
                  call=_call("Balances", "transfer_keep_alive", dest=ALICE, value=1))
    assert rules.classify_materios_block("materios-preprod", 1, [ext], read_events=lambda: None,
                                         sudo_key=rules.account_bytes(SUDO_KEY)) == []


# --- argument shapes any funded account can put in a block ----------------------
#
# Argument values are chosen by whoever signs the extrinsic, so no value may stop a
# block from being classified: its cursor would never move and every later block,
# privileged calls included, would go unwatched.


def _encode(decoder, module: str, function: str, **args) -> str:
    """An extrinsic encoded against the pinned runtime, in the form a block carries it."""
    ext = decoder._config.create_scale_object("Extrinsic", metadata=decoder._metadata)
    return ext.encode({"call_module": module, "call_function": function, "call_args": args}).to_hex()


REMARK = {"call_module": "System", "call_function": "remark", "call_args": {"remark": "0x00"}}
TEXT_THAT_LOOKS_LIKE_HEX = "0x" + "a" * 199 + "z"


def _classify_extrinsics(extrinsics, events=None, sudo_key=SUDO_KEY):
    return rules.classify_materios_block("materios-preprod", 9, extrinsics, read_events=lambda: events,
                                         sudo_key=rules.account_bytes(sudo_key) if sudo_key else None)


def _peak_memory(work):
    """``work()`` and the most memory it held at once."""
    tracemalloc.start()
    try:
        result = work()
        return result, tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def test_a_block_sized_byte_argument_is_shown_as_its_hash_in_memory_near_its_own_size():
    blob = "0x" + "ab" * NORMAL_BLOCK_LENGTH
    shown, peak = _peak_memory(lambda: rules._render_value(blob))
    assert shown.startswith(f"<{NORMAL_BLOCK_LENGTH} bytes blake2_256 0x")
    assert peak < 2 * NORMAL_BLOCK_LENGTH
    assert rules._render_value(blob[:-1] + "z").startswith('"0xabab')


def test_a_block_sized_text_argument_is_rendered_from_its_start_alone():
    text = "\x00" * NORMAL_BLOCK_LENGTH
    shown, peak = _peak_memory(lambda: rules._render_value(text))
    assert shown == json.dumps(text)[:400] + "\u2026"
    assert peak < 64 * 1024


def test_a_remark_of_text_that_looks_like_hex_is_classified(decoder):
    remark = "0x" + TEXT_THAT_LOOKS_LIKE_HEX.encode().hex()
    [ext] = decoder.extrinsics([_encode(decoder, "System", "remark", remark=remark)])
    assert rules._args(ext["call"])["remark"] == TEXT_THAT_LOOKS_LIKE_HEX
    assert _classify_extrinsics([ext]) == []
    [finding] = _classify_extrinsics([_signed(ALICE, "Sudo", "sudo", call=ext["call"])])
    assert finding.severity == rules.CRITICAL
    assert "could not be classified" not in finding.render()


@pytest.mark.parametrize(
    "who, shown",
    [
        ({"Index": 7}, "who=7"),
        ({"Raw": "0x0102"}, '"Raw"'),
        ({"Address20": "0x" + "22" * 20}, '"Address20"'),
        ({"Address32": "0x" + "11" * 32}, '"Address32"'),
    ],
)
def test_sudo_as_a_multiaddress_that_names_no_account_is_classified(decoder, who, shown):
    [ext] = decoder.extrinsics([_encode(decoder, "Sudo", "sudo_as", who=who, call=REMARK)])
    [finding] = _classify_extrinsics([ext])
    assert finding.severity == rules.CRITICAL
    assert "Sudo.sudo_as" in finding.render() and shown in finding.render()


def test_acting_as_a_recovered_multiaddress_that_names_no_account_is_classified(decoder):
    [ext] = decoder.extrinsics([_encode(decoder, "Recovery", "as_recovered", account={"Raw": "0x0102"},
                                        call=REMARK)])
    [finding] = _classify_extrinsics([ext])
    assert finding.severity >= rules.ALERT
    assert "Recovery.as_recovered" in finding.render()


def test_a_signer_that_names_no_account_is_classified():
    ext = {"address": 7, "call": _call("Sudo", "sudo", call=_call("System", "remark", remark="0x00"))}
    [finding] = _classify_extrinsics([ext])
    assert finding.severity == rules.CRITICAL
    assert "signer 7 names no account" in finding.render()


def test_a_multisig_of_more_signatories_than_any_limit_is_classified():
    signatories = [SUDO_KEY] * (1 << 14)
    ext = _signed(ALICE, "Multisig", "as_multi", threshold=2, other_signatories=signatories,
                  call=_call("Sudo", "sudo", call=_call("System", "remark", remark="0x00")))
    [finding] = _classify_extrinsics([ext])
    assert finding.severity == rules.CRITICAL
    assert "multisig account" in finding.render()


def test_an_extrinsic_the_classifier_cannot_read_pages_critical_and_the_rest_are_classified():
    broken = {"extrinsic_hash": "0x" + "ab" * 32, "address": ALICE,
              "call": {"call_module": "Sudo", "call_function": "sudo", "call_args": 5}}
    later = _signed(SUDO_KEY, "Balances", "force_transfer", source=ALICE, dest=ALICE, value=1)
    first, second = _classify_extrinsics([broken, later])
    assert first.severity == rules.CRITICAL
    assert first.headline == "materios-preprod #9 extrinsic 0 could not be classified"
    assert "0x" + "ab" * 32 in first.render()
    assert first.group == "materios-preprod unclassifiable"
    assert second.key == "materios-preprod:9:1" and second.severity == rules.CRITICAL


HOSTILE_VALUES = (
    0, 7, -1, 1 << 130, True, None, "", "0x", "0xzz", "0x" + "abc" * 50, TEXT_THAT_LOOKS_LIKE_HEX,
    "0x" + "00" * 32, ALICE, SUDO_KEY, "text\n````\n**RESOLVED** @everyone", "Ȁ\x00",
    {"Raw": "\x01\x02"}, {"Id": ALICE}, {"Id": 7}, {"Index": 3}, {"Address20": "0x" + "22" * 20},
    {"system": "Root"}, {"system": {"Signed": SUDO_KEY}}, {"system": {"Signed": 7}},
    [ALICE, 7, {"Raw": "\x00"}], [[1, 2], {"x": None}], {"ref_time": 1, "proof_size": 1}, 1.5,
)


def _metadata_calls(decoder) -> list[tuple[str, str, list[tuple[str, str]]]]:
    return [(pallet.name, call.value["name"], [(f["name"], f.get("typeName") or "") for f in call.value["fields"]])
            for pallet in decoder._metadata.pallets for call in (pallet.calls or [])]


def _random_call(rng, calls, depth: int) -> dict:
    module, function, fields = rng.choice(calls)
    args = []
    for name, type_name in fields:
        if "RuntimeCall" not in type_name:
            value = rng.choice(HOSTILE_VALUES)
        elif depth >= 3:
            value = _call("System", "remark", remark="0x00")
        elif type_name.startswith("Vec"):
            value = [_random_call(rng, calls, depth + 1) for _ in range(rng.randrange(3))]
        else:
            value = _random_call(rng, calls, depth + 1)
        args.append({"name": name, "type": type_name, "value": value})
    return {"call_module": module, "call_function": function, "call_args": args}


def test_every_call_in_the_runtime_is_classified_whatever_its_argument_values(decoder):
    import random

    calls = _metadata_calls(decoder)
    rng = random.Random(238)
    signers = (ALICE, SUDO_KEY, 7, {"Raw": "\x01"}, None)
    events = {0: ["System.ExtrinsicFailed(dispatch_error=Err)"]}
    for entry in calls:
        for _ in range(4):
            call = _random_call(rng, [entry], depth=0)
            for signer in signers:
                ext = {"call": call} if signer is None else {"address": signer, "call": call}
                for block_events in (None, events, {}):
                    for finding in _classify_extrinsics([ext], events=block_events):
                        assert "could not be classified" not in finding.headline, (entry[:2], finding.render())


# --- attempts that cannot take effect -----------------------------------------
#
# Root comes only from Sudo, and Sudo dispatches only for Sudo.Key, so a root-gated
# call reached from any other account fails at dispatch. Once the block's events are
# read such an attempt goes to the digest; while they are not, it pages as before.

BOB = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
FAILED = {0: ["System.ExtrinsicFailed(dispatch_error={\"Module\": {\"index\": 7, \"error\": \"0x01000000\"}})"]}
SUCCEEDED = {0: ["System.ExtrinsicSuccess"]}
SET_CODE = _call("System", "set_code", code="0x" + "00" * 100)


def test_an_ordinary_account_calling_sudo_goes_to_the_digest_once_the_call_failed():
    ext = _signed(ALICE, "Sudo", "sudo", call=SET_CODE)
    [failed] = _classify_extrinsics([ext], events=FAILED)
    assert failed.severity == rules.INFO
    assert "dispatch failed: nothing took effect" in failed.render()
    [unverified] = _classify_extrinsics([ext], events=None)
    assert unverified.severity == rules.CRITICAL


ROOT_GATED_FROM_AN_ACCOUNT = [
    (_call("Utility", "force_batch", calls=[_call("Balances", "force_transfer", source=ALICE, dest=BOB, value=1)]),
     {0: ["Utility.ItemFailed", "Utility.BatchCompletedWithErrors", "System.ExtrinsicSuccess"]}),
    (_call("Utility", "batch", calls=[_call("Utility", "dispatch_as", as_origin={"system": "Root"}, call=SET_CODE)]),
     {0: ["Utility.BatchInterrupted", "System.ExtrinsicSuccess"]}),
]
SUDO_FROM_AN_ACCOUNT = [
    (_call("Utility", "batch", calls=[_call("Sudo", "sudo", call=SET_CODE)]),
     {0: ["Utility.BatchInterrupted", "System.ExtrinsicSuccess"]}),
    (_call("Multisig", "as_multi", threshold=2, other_signatories=[BOB], maybe_timepoint=None,
           call=_call("Sudo", "sudo", call=SET_CODE)),
     {0: ["Multisig.NewMultisig", "System.ExtrinsicSuccess"]}),
    (_call("Utility", "batch", calls=[_call("Utility", "as_derivative", index=0, call=_call("Sudo", "sudo", call=SET_CODE))]),
     {0: ["Utility.BatchInterrupted", "System.ExtrinsicSuccess"]}),
]


@pytest.mark.parametrize("call, events", ROOT_GATED_FROM_AN_ACCOUNT + SUDO_FROM_AN_ACCOUNT)
def test_a_root_gated_call_an_ordinary_origin_cannot_dispatch_goes_to_the_digest(call, events):
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=events)
    assert finding.severity == rules.INFO
    assert "cannot take effect from this origin" in finding.render()


@pytest.mark.parametrize("call, events", ROOT_GATED_FROM_AN_ACCOUNT)
def test_a_root_gated_call_from_an_account_needs_neither_events_nor_state_to_be_inert(call, events):
    # Root comes only from Sudo, never from an account's own origin.
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=None)
    assert finding.severity == rules.INFO
    assert "cannot take effect from this origin" in finding.render()


@pytest.mark.parametrize("call, events", SUDO_FROM_AN_ACCOUNT)
def test_a_sudo_call_from_an_account_without_events_or_state_pages_grouped_per_source(call, events):
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=None)
    assert finding.severity == rules.CRITICAL
    assert finding.group == "materios-preprod unverified"


def test_a_live_call_beside_an_inert_one_still_pages():
    call = _call("Utility", "force_batch", calls=[
        _call("Balances", "force_transfer", source=ALICE, dest=BOB, value=1),
        _call("Recovery", "create_recovery", friends=[BOB], threshold=1, delay_period=0)])
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=SUCCEEDED)
    assert finding.severity == rules.ALERT
    assert "Recovery.create_recovery" in finding.render()


def test_an_extrinsic_with_no_events_of_its_own_is_unverified_not_failed():
    ext = _signed(ALICE, "Utility", "batch", calls=[_call("Sudo", "sudo", call=SET_CODE)])
    [finding] = _classify_extrinsics([ext], events={1: ["System.ExtrinsicSuccess"]})
    assert finding.severity == rules.CRITICAL
    assert "events unavailable" in finding.render()


def test_a_key_change_by_an_earlier_sudo_key_still_pages_critical(decoder):
    # Sudo.Key is read at the finalized head; the multisig that set it is the key it replaced.
    block = _block("block_1587358.json")
    extrinsics = decoder.extrinsics(block["extrinsics"])
    [index] = [i for i, e in enumerate(extrinsics) if e.get("address")]
    events = {index: ["Multisig.MultisigExecuted(result=Ok)", "Sudo.KeyChanged", "System.ExtrinsicSuccess"]}
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              read_events=lambda: events, sudo_key=rules.account_bytes(SUDO_KEY))
    assert finding.severity == rules.CRITICAL
    assert "Sudo.set_key" in finding.render()


def test_the_sudo_keys_failed_call_still_pages_critical():
    [finding] = _classify_extrinsics([_signed(SUDO_KEY, "Sudo", "sudo", call=SET_CODE)], events=FAILED)
    assert finding.severity == rules.CRITICAL


def test_the_sudo_multisigs_failed_call_still_pages_critical(decoder):
    block = _block("block_1829226.json")
    extrinsics = decoder.extrinsics(block["extrinsics"])
    events = {2: ["Multisig.MultisigExecuted(result=Err:{\"Module\": {}})", "System.ExtrinsicSuccess"]}
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              read_events=lambda: events, sudo_key=rules.account_bytes(SUDO_KEY))
    assert finding.severity == rules.CRITICAL


def test_a_sudo_event_proves_the_signer_held_the_key_at_that_block():
    events = {0: ["Sudo.Sudid(sudo_result=Ok)", "System.ExtrinsicSuccess"]}
    [finding] = _classify_extrinsics([_signed(ALICE, "Sudo", "sudo", call=SET_CODE)], events=events)
    assert finding.severity == rules.CRITICAL


def test_acting_as_a_recovered_sudo_key_is_critical():
    ext = _signed(ALICE, "Recovery", "as_recovered", account=SUDO_KEY, call=_call("Sudo", "sudo", call=SET_CODE))
    [finding] = _classify_extrinsics([ext], events=SUCCEEDED)
    assert finding.severity == rules.CRITICAL
    assert "System.set_code" in finding.render()


@pytest.mark.parametrize(
    "call",
    [
        _call("Sudo", "sudo_as", who=SUDO_KEY, call=SET_CODE),
        _call("Recovery", "as_recovered", account=SUDO_KEY, call=_call("System", "remark", remark="0x00")),
        _call("Utility", "dispatch_as", as_origin={"system": {"Signed": SUDO_KEY}}, call=SET_CODE),
    ],
)
def test_naming_the_sudo_key_as_a_target_does_not_make_an_attempt_the_keys(call):
    # Anyone may name any account; only the signer, and accounts derived from it,
    # are proven. A failed attempt from an ordinary signer is not an authority's.
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=FAILED)
    assert finding.severity == rules.INFO
    assert finding.group == f"materios-preprod signer {ALICE}"


def test_an_ordinary_account_creating_its_own_recovery_is_an_alert():
    ext = _signed(ALICE, "Recovery", "create_recovery", friends=[BOB], threshold=1, delay_period=0)
    for events in (None, SUCCEEDED):
        [finding] = _classify_extrinsics([ext], events=events)
        assert finding.severity == rules.ALERT


@pytest.mark.parametrize("call", [
    _call("Recovery", "vouch_recovery", lost=ALICE, rescuer=BOB),
    _call("Recovery", "create_recovery", friends=[ALICE], threshold=1, delay_period=0),
    _call("Recovery", "initiate_recovery", account=ALICE),
], ids=["vouch", "create", "initiate"])
def test_what_an_authority_account_does_in_any_recovery_is_critical(call):
    [finding] = rules.classify_materios_block("materios-preprod", 9, [{"address": BOB, "call": call}],
                                              read_events=lambda: SUCCEEDED, sudo_key=rules.account_bytes(SUDO_KEY),
                                              authorities=frozenset({rules.account_bytes(BOB)}))
    assert finding.severity == rules.CRITICAL


@pytest.mark.parametrize("call", [
    _call("Recovery", "initiate_recovery", account=SUDO_KEY),
    _call("Recovery", "create_recovery", friends=[SUDO_KEY], threshold=1, delay_period=0),
    _call("Recovery", "close_recovery", rescuer=SUDO_KEY),
], ids=["initiate", "create naming Sudo.Key a friend", "close naming Sudo.Key the rescuer"])
def test_a_recovery_an_outsider_starts_or_configures_gives_it_no_power_over_sudo_key(call):
    # A recovery of Sudo.Key does nothing until Sudo.Key's friends vouch for it, and a
    # recovery config or its closing concerns the caller's own account.
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=SUCCEEDED)
    assert finding.severity == rules.ALERT


def _state(value):
    """A read of the block's state in which Sudo.Key held throughout and every other fact
    asked for is ``value``."""
    return lambda wanted: {fact: rules.account_bytes(SUDO_KEY) if fact == rules.SUDO else value for fact in wanted}


BATCH_APPLIED = {0: ["Utility.ItemFailed", "Utility.BatchCompleted", "System.ExtrinsicSuccess"]}
ASKED_FOR = {
    "vouch": _call("Recovery", "vouch_recovery", lost=SUDO_KEY, rescuer=BOB),
    "claim": _call("Recovery", "claim_recovery", account=SUDO_KEY),
    "cancel": _call("Recovery", "cancel_recovered", account=SUDO_KEY),
    "as_recovered": _call("Recovery", "as_recovered", account=SUDO_KEY, call=_call("Sudo", "sudo", call=SET_CODE)),
    "apply_authorized_upgrade": _call("System", "apply_authorized_upgrade", code="0x00"),
    "payout": _call("Treasury", "payout", index=0),
}


@pytest.mark.parametrize("events", [BATCH_APPLIED, None], ids=["events read", "events unread"])
@pytest.mark.parametrize("name", sorted(ASKED_FOR))
def test_a_call_the_blocks_state_does_not_allow_from_its_origin_goes_to_the_digest(name, events):
    ext = {"address": ALICE, "call": _call("Utility", "force_batch", calls=[ASKED_FOR[name]])}
    for allowed, severity in ((False, rules.INFO), (True, rules.CRITICAL)):
        [finding] = rules.classify_materios_block("materios-preprod", 9, [ext], lambda: events,
                                                  rules.account_bytes(SUDO_KEY), read_state=_state(allowed))
        assert finding.severity == severity, allowed
        assert ("cannot take effect from this origin" in finding.render()) != allowed


def test_an_ordinary_signers_findings_share_a_group_and_an_authoritys_page_alone(decoder):
    [ordinary] = _classify_extrinsics([_signed(ALICE, "Recovery", "create_recovery", friends=[BOB],
                                               threshold=1, delay_period=0)], events=SUCCEEDED)
    assert ordinary.group == f"materios-preprod signer {ALICE}"
    [authority] = _findings(decoder, "block_1829210.json")
    assert authority.group is None


def test_authority_accounts_are_read_from_the_config():
    doc = json.loads((FIX / "config.json").read_text())
    doc["materios"]["authority_accounts"] = [BOB]
    assert rules.parse_config(doc).materios.authority_accounts == (BOB,)


# --- what a page shows ----------------------------------------------------------


def test_the_headline_names_the_most_severe_call_and_its_line_leads_the_details():
    padding = [_call("System", "remark", remark="x" * 150) for _ in range(12)]
    call = _call("Sudo", "sudo", call=_call("Utility", "batch_all", calls=[*padding, SET_CODE]))
    [finding] = _classify_extrinsics([{"address": SUDO_KEY, "call": call}])
    assert finding.headline.endswith("extrinsic 0: Sudo.sudo > Utility.batch_all > System.set_code")
    sites = [d for d in finding.details if d.startswith(("CRITICAL: ", "ALERT: "))]
    assert sites[0].startswith("CRITICAL: Sudo.sudo > Utility.batch_all > System.set_code(code=")
    assert finding.details.index(sites[0]) < finding.details.index("call tree:")


def test_a_batch_of_thousands_of_calls_keeps_a_bounded_finding():
    calls = [_call("System", "set_storage", items=[]) for _ in range(5000)]
    [finding] = _classify_extrinsics([_signed(ALICE, "Utility", "batch", calls=calls)])
    assert len(finding.details) < 250
    assert "4,980 more privileged calls" in finding.render()
    assert "more lines of the call tree" in finding.render()


def test_the_call_tree_stops_growing_at_its_line_limit_while_it_is_walked():
    calls = [_call("System", "remark", remark="0x00") for _ in range(5000)]
    batch = _call("Utility", "batch", calls=calls)
    tree = rules._Tree(None, frozenset())
    rules._walk(batch, None, tree, 0, (), frozenset(), True)
    assert len(tree.lines) == rules.MAX_TREE_LINES
    [finding] = _classify_extrinsics([_signed(ALICE, "Sudo", "sudo", call=batch)])
    assert finding.details[-1] == "... 4,802 more lines of the call tree"


def test_a_batch_as_large_as_a_block_allows_is_classified_in_linear_time():
    import time

    inert = _call("Balances", "force_transfer", source=ALICE, dest=BOB, value=1)
    live = _call("Recovery", "create_recovery", friends=[BOB], threshold=1, delay_period=0)
    calls = [inert, live] * 10_000
    started = time.monotonic()
    [finding] = _classify_extrinsics([_signed(ALICE, "Utility", "force_batch", calls=calls)], events=SUCCEEDED)
    assert time.monotonic() - started < 10
    assert finding.severity == rules.ALERT


def test_a_call_in_neither_table_is_an_alert():
    [finding] = _classify_extrinsics([_signed(ALICE, "NewPallet", "do_thing", value=1)])
    assert finding.severity == rules.ALERT
    assert "neither the severity table nor the routine list" in finding.render()


@pytest.mark.parametrize("function", ["schedule", "cancel", "fast_track", "enact", "set_delay", "set_guardian"])
def test_every_root_timelock_call_is_critical(function):
    [finding] = _classify_extrinsics([_signed(ALICE, "RootTimelock", function, id=1)])
    assert finding.severity == rules.CRITICAL


def test_every_call_in_the_pinned_runtime_is_in_the_severity_table_or_the_routine_list(decoder):
    unlisted = []
    for module, function, _ in _metadata_calls(decoder):
        for finding in _classify_extrinsics([_signed(ALICE, module, function)]):
            if "neither the severity table nor the routine list" in finding.render():
                unlisted.append(f"{module}.{function}")
    assert unlisted == []


# --- Cardano ------------------------------------------------------------------


@pytest.fixture(scope="module")
def networks():
    config = rules.parse_config(json.loads((FIX / "config.json").read_text()))
    return {n.name: n for n in config.cardano}


def _tx(name: str) -> dict:
    return json.loads((FIX / "cardano" / f"{name}.json").read_text())


def _classify(networks, network: str, name: str):
    tx = _tx(name)
    return rules.classify_cardano_tx(networks[network], tx["tx"], tx["utxos"], tx["redeemers"])


@pytest.mark.parametrize(
    "name, payout",
    [
        ("surrender_agent", 1056778496),
        ("surrender_agent_shards_t1", 10238574959574),
        ("surrender_t2_pass", 188339428791),
        ("surrender_flux_pass_shards", 837837474230),
        ("surrender_flux_pass_brawlers", 175153213171),
    ],
)
def test_a_surrender_paid_exactly_its_entitlement_goes_to_the_digest(networks, name, payout):
    finding = _classify(networks, "cardano-mainnet", name)
    assert finding.severity == rules.INFO
    assert finding.kind == "surrender"
    assert finding.amount == payout
    assert "surrender" in finding.render()


def test_an_outflow_from_a_custody_wallet_is_critical(networks):
    finding = _classify(networks, "cardano-mainnet", "custody_outflow")
    assert finding.severity == rules.CRITICAL
    assert "outflow from custody-a" in finding.render()


def test_minting_cmatra_is_critical(networks):
    finding = _classify(networks, "cardano-mainnet", "mint_v2")
    text = finding.render()
    assert finding.severity == rules.CRITICAL
    assert "minted" in text and "cMATRA" in text and "1,000,000,000" in text


def test_a_pool_spend_that_is_not_a_surrender_is_critical(networks):
    finding = _classify(networks, "cardano-mainnet", "pool_rotate")
    assert finding.severity == rules.CRITICAL
    assert "pool spent without a surrender" in finding.render()


def test_a_custody_wallet_surrendering_through_the_pool_is_critical(networks):
    finding = _classify(networks, "cardano-mainnet", "custody_wallet_surrender")
    text = finding.render()
    assert finding.severity == rules.CRITICAL
    assert "outflow from custody-c" in text
    assert "a custody wallet funded this pool spend as the claimant" in text


def test_admin_withdraw_is_critical(networks):
    finding = _classify(networks, "merger-rehearsal-preprod", "rehearsal_admin_withdraw")
    text = finding.render()
    assert finding.severity == rules.CRITICAL
    assert "AdminWithdraw" in text and "non-claimant" in text


def test_pool_value_paid_to_an_address_that_surrendered_nothing_is_critical(networks):
    finding = _classify(networks, "merger-rehearsal-preprod", "rehearsal_surrender")
    assert finding.severity == rules.CRITICAL
    assert "non-claimant" in finding.render()


@pytest.mark.parametrize(
    "name, label, severity",
    [
        ("d_parameter_upsert", "DParameterValidator", rules.ALERT),
        ("permissioned_candidates_upsert", "PermissionedCandidatesValidator", rules.ALERT),
        ("spo_registration", "CommitteeCandidateValidator", rules.ALERT),
    ],
)
def test_every_partner_chain_contract_transaction_is_an_alert(networks, name, label, severity):
    finding = _classify(networks, "cardano-preprod-partner-chain", name)
    assert finding.severity >= severity
    assert label in finding.render()


def test_the_governance_wallet_paying_for_a_contract_update_is_critical(networks):
    finding = _classify(networks, "cardano-preprod-partner-chain", "d_parameter_upsert")
    assert finding.severity == rules.CRITICAL
    assert "spent from governance" in finding.render()


# Tampered copies of a real surrender: each is what a drain dressed as a surrender
# would look like once both admin signatures are available.
def _tampered_surrender(mutate):
    tx = copy.deepcopy(_tx("surrender_agent"))
    mutate(tx["utxos"])
    return tx


def _move_cmatra(utxos, source, sink, amount):
    for out in (source, sink):
        for a in out["amount"]:
            if a["unit"].startswith("7ff33a55"):
                a["quantity"] = str(int(a["quantity"]) + (amount if out is sink else -amount))


def _outputs(utxos):
    pool = next(o for o in utxos["outputs"] if o["address"].startswith("addr1w8s6"))
    claimant = next(o for o in utxos["outputs"]
                    if any(a["unit"].startswith("7ff33a55") for a in o["amount"]) and o is not pool)
    return pool, claimant


def test_a_surrender_paying_more_than_the_entitlement_is_critical(networks):
    def overpay(utxos):
        pool, claimant = _outputs(utxos)
        _move_cmatra(utxos, pool, claimant, 5_000_000_000_000)

    tx = _tampered_surrender(overpay)
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL
    assert "overpaid" in finding.render()


def test_a_surrender_that_pays_someone_else_is_critical(networks):
    stranger = _outputs(_tx("surrender_t2_pass")["utxos"])[1]["address"]

    def redirect(utxos):
        pool, claimant = _outputs(utxos)
        claimant["address"] = stranger

    tx = _tampered_surrender(redirect)
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL
    assert "non-claimant" in finding.render()


def _joined_by(utxos, address: str, units: dict[str, int], at: int = 0):
    """``address`` adds one input carrying 2 ADA and ``units``, and takes the 2 ADA back."""
    amount = [{"unit": "lovelace", "quantity": "2000000"}, *({"unit": u, "quantity": str(q)} for u, q in units.items())]
    utxos["inputs"].append({"address": address, "tx_hash": "ab" * 32, "output_index": at, "amount": amount,
                            "collateral": False, "reference": False})
    utxos["outputs"].append({"address": address, "output_index": 90 + at, "collateral": False,
                             "amount": [{"unit": "lovelace", "quantity": "2000000"}]})


AGENT_UNIT = "97bbb7db0baef89caefce61b8107ac74c7a7340166b39d906f174bec54616c6f73"


def _redirected_to_a_second_wallet(carrying: dict[str, int]):
    """The real AGENT surrender with its payout sent to a second wallet, which adds one
    input carrying ``carrying``; what that input carries goes to the quarantine address."""
    stranger = _outputs(_tx("surrender_t2_pass")["utxos"])[1]["address"]

    def mutate(utxos):
        _, claimant = _outputs(utxos)
        claimant["address"] = stranger
        _joined_by(utxos, stranger, carrying)
        quarantine = next(o for o in utxos["outputs"] if o["address"].startswith("addr1wy5g"))
        for unit, quantity in carrying.items():
            row = next((a for a in quarantine["amount"] if a["unit"] == unit), None)
            if row is None:
                quarantine["amount"].append({"unit": unit, "quantity": str(quantity)})
            else:
                row["quantity"] = str(int(row["quantity"]) + quantity)

    return _tampered_surrender(mutate), stranger


def test_a_payout_sent_to_a_wallet_that_only_added_ada_is_critical(networks):
    tx, stranger = _redirected_to_a_second_wallet({})
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL and finding.kind != "surrender"
    assert f"cMATRA paid to a non-claimant: {stranger[:24]}… 1,056.778496 cMATRA" in finding.render()


def test_a_payout_to_a_wallet_beyond_what_its_own_deposit_is_entitled_to_is_critical(networks):
    # The second wallet surrenders one AGENT of its own and takes the whole payout: the
    # total still matches the entitlement, but the first wallet's share went to it.
    tx, stranger = _redirected_to_a_second_wallet({AGENT_UNIT: 1})
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    text = finding.render()
    assert finding.severity == rules.CRITICAL and finding.kind != "surrender"
    assert "overpaid" not in text
    assert (f"cMATRA paid to {stranger[:24]}…: 1,056.778496 cMATRA against the 0.462890 cMATRA "
            f"its own deposit is entitled to") in text


def test_legacy_units_that_leave_a_surrender_for_a_third_wallet_are_critical(networks):
    # Units given up by the depositor that reach neither the quarantine address nor the
    # depositor's own change are not a surrender's.
    third = _outputs(_tx("surrender_t2_pass")["utxos"])[1]["address"]

    def diverted(utxos):
        depositor = utxos["inputs"][0]
        next(a for a in depositor["amount"] if a["unit"] == AGENT_UNIT)["quantity"] = "673"
        utxos["outputs"].append({"address": third, "output_index": 9, "collateral": False,
                                 "amount": [{"unit": "lovelace", "quantity": "1200000"},
                                            {"unit": AGENT_UNIT, "quantity": "5"}]})

    tx = _tampered_surrender(diverted)
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL and finding.kind != "surrender"
    assert "legacy units left this surrender for a wallet that did not give them up" in finding.render()


def test_a_payout_sent_into_the_quarantine_lockbox_is_critical(networks):
    # Nothing can spend from the quarantine address, so cMATRA paid into it is lost to
    # every holder still to surrender, though the total paid matches the entitlement.
    def lockbox(utxos):
        _, claimant = _outputs(utxos)
        paid = next(a for a in claimant["amount"] if a["unit"] == CMATRA_UNIT)
        half = int(paid["quantity"]) // 2
        paid["quantity"] = str(int(paid["quantity"]) - half)
        quarantine = next(o for o in utxos["outputs"] if o["address"].startswith("addr1wy5g"))
        quarantine["amount"].append({"unit": CMATRA_UNIT, "quantity": str(half)})

    tx = _tampered_surrender(lockbox)
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL and finding.kind != "surrender"
    assert "cMATRA paid into the quarantine address, where nothing can spend it: 528.389248 cMATRA" in finding.render()
    assert "unrecognized asset" not in finding.render()


def test_a_depositors_own_cmatra_returned_as_change_is_not_a_payout(networks):
    def holds_cmatra(utxos):
        utxos["inputs"][0]["amount"].append({"unit": CMATRA_UNIT, "quantity": "7000000"})
        _, claimant = _outputs(utxos)
        paid = next(a for a in claimant["amount"] if a["unit"] == CMATRA_UNIT)
        paid["quantity"] = str(int(paid["quantity"]) + 7_000_000)

    tx = _tampered_surrender(holds_cmatra)
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.INFO and finding.kind == "surrender" and finding.amount == 1056778496


def test_a_surrender_that_leaves_the_pool_without_its_datum_is_critical(networks):
    def strip_datum(utxos):
        pool, _ = _outputs(utxos)
        pool["inline_datum"] = None

    tx = _tampered_surrender(strip_datum)
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL
    assert "without an inline datum" in finding.render()


def test_value_arriving_at_a_custody_address_alone_is_an_alert(networks):
    tx = _tx("surrender_agent")
    network = networks["cardano-mainnet"]
    quarantine = network.pool.quarantine_address
    watched = rules.WatchedAddress(label="quarantine", address=quarantine, role="custody", severity=rules.CRITICAL)
    network = rules.CardanoNetwork(**{**network.__dict__, "addresses": network.addresses + (watched,)})
    finding = rules.classify_cardano_tx(network, tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.ALERT
    assert "inflow to quarantine" in finding.render()


def test_an_unrelated_transaction_is_not_a_finding(networks):
    tx = _tx("spo_registration")
    assert rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"]) is None


# A transaction whose script fails phase 2 consumes its collateral instead of its inputs,
# and produces its collateral return instead of its outputs. A custody key can sign one
# on purpose, with the reserve as collateral and the collateral return paying it away.


def _with_custody(network, address: str, label: str = "collateral-wallet"):
    watched = rules.WatchedAddress(label=label, address=address, role="custody")
    return rules.CardanoNetwork(**{**network.__dict__, "addresses": network.addresses + (watched,)})


def _failed_script(mutate=lambda utxos: None):
    """The real preprod transaction b9ebe459, whose script failed: db-sync stores the
    collateral it consumed and the collateral return it produced as its inputs and outputs,
    so Blockfrost lists them unflagged, and lists no redeemer."""
    tx = copy.deepcopy(_tx("phase2_invalid_collateral"))
    assert tx["tx"]["valid_contract"] is False and tx["redeemers"] == []
    mutate(tx["utxos"])
    return tx


def _classify_failed(networks, tx):
    network = _with_custody(networks["cardano-mainnet"], tx["utxos"]["inputs"][0]["address"])
    return rules.classify_cardano_tx(network, tx["tx"], tx["utxos"], tx["redeemers"])


def test_a_custody_utxo_a_failed_script_consumed_as_collateral_is_a_critical_outflow(networks):
    tx = _failed_script()
    assert not any(u["collateral"] for u in tx["utxos"]["inputs"] + tx["utxos"]["outputs"])
    finding = _classify_failed(networks, tx)
    assert finding.severity == rules.CRITICAL and finding.group is None
    assert finding.details[0] == "outflow from collateral-wallet: net -10.000000 ADA"
    assert finding.details[-1].startswith("phase-2 script failure")


CMATRA_UNIT = "7ff33a5565393dc47b48ac47becc12d92c9952e724e8446dfb6adc66634d41545241"


def _reserve_as_collateral(utxos):
    [collateral], [collateral_return] = utxos["inputs"], utxos["outputs"]
    for row in (collateral, collateral_return):
        row["amount"].append({"unit": CMATRA_UNIT, "quantity": "277500000000000"})


def test_the_reserve_paid_away_through_a_collateral_return_is_named_in_the_outflow(networks):
    finding = _classify_failed(networks, _failed_script(_reserve_as_collateral))
    assert finding.severity == rules.CRITICAL and finding.group is None
    assert finding.details[0] == "outflow from collateral-wallet: net -10.000000 ADA, -277,500,000.000000 cMATRA"


def test_a_failed_scripts_collateral_listed_twice_counts_once_with_its_tokens(networks):
    # Blockfrost documents ``collateral`` as marking the collateral a failed script
    # consumed; its collateral listing carries lovelace alone. However a source lists the
    # consumed collateral, flagged, unflagged or both, it is one outflow with every unit.
    def listed_twice(utxos):
        _reserve_as_collateral(utxos)
        flagged = copy.deepcopy(utxos["inputs"][0])
        flagged.update(collateral=True, amount=[a for a in flagged["amount"] if a["unit"] == "lovelace"])
        utxos["inputs"].append(flagged)

    finding = _classify_failed(networks, _failed_script(listed_twice))
    assert finding.details[0] == "outflow from collateral-wallet: net -10.000000 ADA, -277,500,000.000000 cMATRA"


def test_a_failed_script_never_counts_as_a_surrender(networks):
    tx = copy.deepcopy(_tx("surrender_agent"))
    tx["tx"]["valid_contract"] = False
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.kind != "surrender" and finding.amount == 0


# A drain dressed as surrenders of passes that did not exist before: minted in the
# surrender itself, or under a fresh name, while the collection policy can still mint.


def _t2_surrender():
    tx = copy.deepcopy(_tx("surrender_t2_pass"))
    t2 = "06a64965c0ac1144a72a6ddfcb23aa9d4d7742a5b20ddd5cfb1164b9"
    [unit] = {a["unit"] for o in tx["utxos"]["outputs"] for a in o["amount"] if a["unit"].startswith(t2)}
    return tx, t2, unit


def test_a_surrender_whose_pass_is_minted_in_the_same_transaction_is_critical(networks):
    tx, t2, unit = _t2_surrender()
    for spent in tx["utxos"]["inputs"]:
        spent["amount"] = [a for a in spent["amount"] if a["unit"] != unit]
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL and finding.kind != "surrender"
    assert f"mints or burns under policy {t2}" in finding.render()


def test_a_surrender_of_a_pass_outside_the_pinned_names_is_critical(networks):
    tx, t2, unit = _t2_surrender()
    fresh = t2 + b"AdamPass9999".hex()
    tx = json.loads(json.dumps(tx).replace(unit, fresh))
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL and finding.kind != "surrender"
    assert "outside the pinned redeemable names" in finding.render()


def test_a_redemption_by_policy_must_pin_its_redeemable_names():
    doc = json.loads((FIX / "config.json").read_text())
    del doc["cardano"][0]["surrender_pool"]["redemptions"][2]["asset_names"]
    with pytest.raises(ValueError, match="FLUX_PASS"):
        rules.parse_config(doc)


def test_a_surrender_paying_more_than_the_ceiling_is_an_alert(networks):
    network = networks["cardano-mainnet"]
    pool = rules.SurrenderPool(**{**network.pool.__dict__, "max_payout": 100_000_000_000})
    network = rules.CardanoNetwork(**{**network.__dict__, "pool": pool})
    tx = _tx("surrender_t2_pass")
    finding = rules.classify_cardano_tx(network, tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.ALERT
    assert "above the per-surrender ceiling of 100,000.000000 cMATRA" in finding.render()


def test_what_a_claimant_can_make_a_surrender_do_is_grouped_by_the_pool(networks):
    # Anyone holding a legacy unit can surrender it, so an underpayment or a payout above
    # the ceiling is the claimant's doing, not a move of the pool's keys.
    def underpay(utxos):
        pool, claimant = _outputs(utxos)
        _move_cmatra(utxos, claimant, pool, 1_000_000)

    tx = _tampered_surrender(underpay)
    underpaid = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    network = networks["cardano-mainnet"]
    pool = rules.SurrenderPool(**{**network.pool.__dict__, "max_payout": 100_000_000_000})
    tx = _tx("surrender_t2_pass")
    above = rules.classify_cardano_tx(rules.CardanoNetwork(**{**network.__dict__, "pool": pool}), tx["tx"],
                                      tx["utxos"], tx["redeemers"])
    assert "underpaid" in underpaid.render() and "ceiling" in above.render()
    for finding in (underpaid, above):
        assert finding.severity == rules.ALERT
        assert finding.group == "cardano-mainnet surrender-pool"


def test_the_ceiling_is_read_from_the_config():
    doc = json.loads((FIX / "config.json").read_text())
    doc["cardano"][0]["surrender_pool"]["max_payout"] = 25_000_000_000_000
    assert rules.parse_config(doc).cardano[0].pool.max_payout == 25_000_000_000_000


def test_surrenders_beyond_the_rate_table_supply_alert(networks):
    network = networks["cardano-mainnet"]
    t2 = "06a64965c0ac1144a72a6ddfcb23aa9d4d7742a5b20ddd5cfb1164b9"
    names = sorted(next(r for r in network.pool.redemptions if r.key == "T2_ADAM_PASS").asset_names)
    at_supply = {t2 + name: 1 for name in names[:95]}
    assert rules.redemption_overruns(network, {"lovelace": 5_000_000, **at_supply}) == []
    [finding] = rules.redemption_overruns(network, {**at_supply, t2 + names[95]: 1})
    assert finding.severity == rules.ALERT
    assert finding.key == "cardano-mainnet:redeemed:T2_ADAM_PASS:96"
    assert "96 T2_ADAM_PASS surrendered against a rate-table supply of 95" in finding.headline


def _payment(network, address: str, lovelace: int = 1_000_000) -> dict:
    tx = {"hash": "bb" * 32, "block_height": 1, "block_time": 1_790_000_000, "valid_contract": True}
    payer = "addr_test1vz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzerspjrlsz"
    utxos = {"inputs": [{"address": payer, "amount": [{"unit": "lovelace", "quantity": str(3 * lovelace)}]}],
             "outputs": [{"address": address, "amount": [{"unit": "lovelace", "quantity": str(lovelace)}]}]}
    return rules.classify_cardano_tx(network, tx, utxos, [])


def test_paying_into_a_contract_anyone_may_pay_is_an_alert_grouped_by_address(networks):
    network = networks["cardano-preprod-partner-chain"]
    ics = next(a for a in network.addresses if a.label == "IlliquidCirculationSupplyValidator")
    assert ics.severity == rules.CRITICAL
    finding = _payment(network, ics.address)
    assert finding.severity == rules.ALERT
    assert finding.group == "cardano-preprod-partner-chain IlliquidCirculationSupplyValidator"


def test_custody_and_pool_findings_are_never_grouped(networks):
    assert _classify(networks, "cardano-mainnet", "custody_outflow").group is None
    assert _classify(networks, "cardano-mainnet", "surrender_agent").group is None
    assert _classify(networks, "cardano-mainnet", "mint_v2").group is None


def test_a_pool_redeemer_whose_constructor_is_not_a_number_is_critical(networks):
    tx = _tx("surrender_agent")
    for redeemer in tx["redeemers"]:
        redeemer["json_value"] = {"constructor": [0], "fields": []}
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL
    assert "unrecognized redeemer" in finding.render()


def test_the_finding_key_is_the_network_and_transaction(networks):
    tx = _tx("custody_outflow")
    finding = rules.classify_cardano_tx(networks["cardano-mainnet"], tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.key == f"cardano-mainnet:{tx['tx']['hash']}"


# --- surrender-pool coverage --------------------------------------------------
#
# Byte copies of matra-token-merger's audit_pack/2026-09-27/redemption_pin.json and
# audit_pack/2026-04-19/rate_table_cmatra.json, as on its main at 5d1ae8d.

COVERAGE = FIX / "coverage"
PIN_SHA256 = "c21c230055fe6ace0d7aae252cb3184f8ec0ff6f1bf2cc9fd37fb5dd87d909c4"
RATE_TABLE_SHA256 = "316cb65c2b309b09242754b322a2763c5ae622304769ed0baffc2f13482b0970"
# The merger's own figures at the pin: load_redemption_pin(pin).remaining summed per merge
# asset and priced by compute_redemption(rate_table, asset, count), floor(count *
# rate_numerator / rate_denominator), run at 5d1ae8d.
MERGER_LIABILITIES = {
    "AGENT": 205_895_069_264_433, "BRAWL_PASS_ETD": 2_358_294_490_036, "FLUX_PASS": 17_871_815_681_109,
    "SE_BRAWLERS": 5_400_013_097_593, "SHARDS": 47_582_303_806_424, "T1_ADAM_PASS": 51_069_991_816_771,
    "T2_ADAM_PASS": 14_125_457_159_335,
}
# The pool after the last surrender before the pin, and the cMATRA the pool paid out in the
# seven days before 2026-09-28 13:00Z (six surrenders), both read from mainnet.
TODAYS_POOL = 333_944_276_732_371
TODAYS_WEEK = 20_266_203_828_110
TODAY = datetime.fromisoformat("2026-09-28T13:00:00+00:00").timestamp()


def coverage_doc(**overrides) -> dict:
    doc = json.loads((FIX / "config.json").read_text())
    doc["cardano"][0]["surrender_pool"]["coverage"] = {
        "redemption_pin_file": str(COVERAGE / "redemption_pin.json"),
        "rate_table_file": str(COVERAGE / "rate_table_cmatra.json"),
        "deadline_utc": "2026-11-29T00:00:00Z", **overrides}
    return doc


def _covered(**overrides) -> rules.CardanoNetwork:
    return rules.parse_config(coverage_doc(**overrides)).cardano[0]


def _at_the_pin(network) -> dict[str, int]:
    return {u.unit: u.quarantined for u in network.pool.coverage.units}


def test_the_vendored_pin_and_rate_table_are_the_mergers_audit_packs():
    assert hashlib.sha256((COVERAGE / "redemption_pin.json").read_bytes()).hexdigest() == PIN_SHA256
    assert hashlib.sha256((COVERAGE / "rate_table_cmatra.json").read_bytes()).hexdigest() == RATE_TABLE_SHA256


def test_what_is_outstanding_at_the_pin_matches_the_mergers_own_redemption_code():
    network = _covered()
    assert rules.outstanding(network.pool.coverage, _at_the_pin(network)) == MERGER_LIABILITIES
    assert sum(MERGER_LIABILITIES.values()) == 344_302_945_315_701


def test_units_surrendered_since_the_pin_are_no_longer_outstanding():
    network = _covered()
    held = _at_the_pin(network)
    held[AGENT_UNIT] += 1_000
    agent = next(r for r in network.pool.redemptions if r.key == "AGENT")
    assert rules.outstanding(network.pool.coverage, held)["AGENT"] == \
        (444_803_187 - 1_000) * agent.numerator // agent.denominator


def test_todays_pool_is_covered_above_the_floor_and_pages_nothing():
    network = _covered()
    lines, page = rules.pool_coverage(network, TODAYS_POOL, _at_the_pin(network), TODAYS_WEEK, 0, TODAY)
    assert page is None
    assert lines == [
        "cardano-mainnet surrender pool: 333,944,276.732371 cMATRA against 344,302,945.315701 cMATRA outstanding "
        "at the pinned rates, 96.99% covered (10,358,668.583330 cMATRA short)",
        "  redeemed in the last 7 days: 20,266,203.828110 cMATRA; 61 days to the 2026-11-29 deadline; "
        "at that pace the pool lasts 115 days",
    ]


def test_coverage_below_the_floor_pages_an_alert_once_a_day():
    network = _covered()
    liabilities = sum(MERGER_LIABILITIES.values())
    _, page = rules.pool_coverage(network, liabilities * 89 // 100, _at_the_pin(network), TODAYS_WEEK, 0, TODAY)
    assert page.severity == rules.ALERT and page.group is None
    assert page.key == "cardano-mainnet:coverage:2026-09-28"
    assert page.headline == "cardano-mainnet surrender pool covers 88.99% of what is outstanding, below the 90% floor"


def test_a_pool_that_runs_out_within_two_weeks_and_before_the_deadline_pages():
    network = _covered()
    _, page = rules.pool_coverage(network, TODAYS_POOL, _at_the_pin(network), TODAYS_POOL * 7 // 10, 0, TODAY)
    assert page.severity == rules.ALERT
    assert page.headline == ("cardano-mainnet surrender pool runs out in about 10 days at the last 7 days' pace, "
                             "before the 2026-11-29 deadline")


@pytest.mark.parametrize("days, now", [(20, TODAY), (10, datetime.fromisoformat(
    "2026-11-25T00:00:00+00:00").timestamp()), (10, datetime.fromisoformat("2026-11-30T00:00:00+00:00").timestamp())])
def test_a_run_out_further_than_two_weeks_or_past_the_deadline_pages_nothing(days, now):
    network = _covered()
    _, page = rules.pool_coverage(network, TODAYS_POOL, _at_the_pin(network), TODAYS_POOL * 7 // days, 0, now)
    assert page is None


def test_the_floor_and_the_run_out_window_are_read_from_the_config():
    network = _covered(floor_percent=98, runout_page_days=200)
    lines, page = rules.pool_coverage(network, TODAYS_POOL, _at_the_pin(network), TODAYS_WEEK, 0, TODAY)
    assert "below the 98% floor" in page.headline
    assert "runs out" not in page.headline


def test_a_weeks_payouts_read_only_in_part_say_so():
    network = _covered()
    lines, _ = rules.pool_coverage(network, TODAYS_POOL, _at_the_pin(network), TODAYS_WEEK, 12, TODAY)
    assert "at least 20,266,203.828110 cMATRA (12 transactions left unread)" in lines[1]


def test_a_coverage_rate_that_differs_from_the_redemption_it_prices_is_refused():
    doc = coverage_doc()
    doc["cardano"][0]["surrender_pool"]["redemptions"][0]["numerator"] += 1
    with pytest.raises(ValueError, match="AGENT"):
        rules.parse_config(doc)


def test_a_pinned_asset_the_rate_table_does_not_price_is_refused(tmp_path):
    table = json.loads((COVERAGE / "rate_table_cmatra.json").read_text())
    del table["tokens"]["SE_BRAWLERS"]
    (tmp_path / "rates.json").write_text(json.dumps(table))
    with pytest.raises(ValueError, match="SE_BRAWLERS"):
        rules.parse_config(coverage_doc(rate_table_file=str(tmp_path / "rates.json")))


def test_coverage_without_the_quarantine_address_it_counts_is_refused():
    doc = coverage_doc()
    del doc["cardano"][0]["surrender_pool"]["quarantine_address"]
    with pytest.raises(ValueError, match="quarantine_address"):
        rules.parse_config(doc)


def test_a_coverage_deadline_without_a_utc_offset_is_refused():
    # Read without an offset, the deadline would move with the host's time zone.
    with pytest.raises(ValueError, match="deadline_utc"):
        rules.parse_config(coverage_doc(deadline_utc="2026-11-29T00:00:00"))
    assert _covered(deadline_utc="2026-11-29T00:00:00+00:00").pool.coverage.deadline == _covered().pool.coverage.deadline


def test_the_pool_outflow_of_a_surrender_is_its_payout():
    tx = _tx("surrender_agent")
    network = _covered()
    assert rules.pool_outflow(network.pool, tx["utxos"]) == 1_056_778_496
