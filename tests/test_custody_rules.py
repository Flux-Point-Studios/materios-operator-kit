"""Custody and authority-move classification against real chain history.

Materios fixtures are complete finalized blocks from the live preprod chain,
decoded with the live runtime's metadata. Cardano fixtures are Blockfrost
responses for real transactions; wallet (key-hash) credentials and transaction
ids are pseudonymized, while contract addresses, policies, amounts, datums and
redeemers are as they are on chain. The two custody-wallet fixtures also carry a
synthetic block position, and the outflow synthetic amounts.
"""

import copy
import gzip
import json
from pathlib import Path

import pytest

from daemon import custody_rules as rules

FIX = Path(__file__).parent / "fixtures" / "custody"
SUDO_KEY = "5H2M5Dbt8hSfSCXS6hfEBPR1N21yh679finzcfMEwD62i7iP"
SPEC_238_CODE_HASH = "0xae5e94cef78cb63c58079c46b32b11e6f8c371ea9a701feeb5a4600c0e76edb4"


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
        events=None, sudo_key=rules.account_bytes(sudo_key) if sudo_key else None,
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


def test_an_extrinsic_the_runtime_metadata_cannot_decode_is_an_alert(decoder):
    block = _block("block_2029707.json")
    garbage = "0x" + (bytes([12, 4, 0xFE, 0x01]) + bytes(2)).hex()
    extrinsics = decoder.extrinsics(block["extrinsics"] + [garbage])
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              events=None, sudo_key=None)
    assert finding.severity == rules.ALERT
    assert finding.key == f"materios-preprod:{block['number']}:{len(block['extrinsics'])}"
    assert "could not be decoded" in finding.render()


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
                                              events=events, sudo_key=rules.account_bytes(SUDO_KEY))
    assert "Sudo.Sudid(sudo_result=Ok)" in finding.render()


def test_a_block_whose_state_is_pruned_says_so(decoder):
    block = _block("block_1829226.json")
    extrinsics = decoder.extrinsics(block["extrinsics"])
    [finding] = rules.classify_materios_block("materios-preprod", block["number"], extrinsics,
                                              events=None, sudo_key=None)
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
    [finding] = rules.classify_materios_block("materios-preprod", 1, [{"address": ALICE, "call": call}],
                                              events=None, sudo_key=None)
    assert finding.severity == severity


def test_anything_signed_by_the_sudo_key_is_critical():
    ext = _signed(SUDO_KEY, "Balances", "transfer_keep_alive", dest=ALICE, value=1)
    [finding] = rules.classify_materios_block("materios-preprod", 1, [ext], events=None,
                                              sudo_key=rules.account_bytes(SUDO_KEY))
    assert finding.severity == rules.CRITICAL
    assert "signed by Sudo.Key" in finding.render()


def test_a_multisig_that_is_not_sudo_and_wraps_nothing_privileged_is_ignored():
    ext = _signed(ALICE, "Multisig", "as_multi_threshold_1", other_signatories=[SUDO_KEY],
                  call=_call("Balances", "transfer_keep_alive", dest=ALICE, value=1))
    assert rules.classify_materios_block("materios-preprod", 1, [ext], events=None,
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
    return rules.classify_materios_block("materios-preprod", 9, extrinsics, events=events,
                                         sudo_key=rules.account_bytes(sudo_key) if sudo_key else None)


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
    later = _signed(ALICE, "Balances", "force_transfer", source=ALICE, dest=ALICE, value=1)
    first, second = _classify_extrinsics([broken, later])
    assert first.severity == rules.CRITICAL
    assert first.headline == "materios-preprod #9 extrinsic 0 could not be classified"
    assert "0x" + "ab" * 32 in first.render()
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


@pytest.mark.parametrize(
    "call, events",
    [
        (_call("Utility", "batch", calls=[_call("Sudo", "sudo", call=SET_CODE)]),
         {0: ["Utility.BatchInterrupted", "System.ExtrinsicSuccess"]}),
        (_call("Utility", "force_batch", calls=[_call("Balances", "force_transfer", source=ALICE, dest=BOB, value=1)]),
         {0: ["Utility.ItemFailed", "Utility.BatchCompletedWithErrors", "System.ExtrinsicSuccess"]}),
        (_call("Multisig", "as_multi", threshold=2, other_signatories=[BOB], maybe_timepoint=None,
               call=_call("Sudo", "sudo", call=SET_CODE)),
         {0: ["Multisig.NewMultisig", "System.ExtrinsicSuccess"]}),
        (_call("Utility", "batch", calls=[_call("Utility", "dispatch_as", as_origin={"system": "Root"}, call=SET_CODE)]),
         {0: ["Utility.BatchInterrupted", "System.ExtrinsicSuccess"]}),
        (_call("Utility", "batch", calls=[_call("Utility", "as_derivative", index=0, call=_call("Sudo", "sudo", call=SET_CODE))]),
         {0: ["Utility.BatchInterrupted", "System.ExtrinsicSuccess"]}),
    ],
)
def test_a_root_gated_call_an_ordinary_origin_cannot_dispatch_goes_to_the_digest(call, events):
    [finding] = _classify_extrinsics([{"address": ALICE, "call": call}], events=events)
    assert finding.severity == rules.INFO
    assert "cannot take effect from this origin" in finding.render()
    [unverified] = _classify_extrinsics([{"address": ALICE, "call": call}], events=None)
    assert unverified.severity == rules.CRITICAL


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
                                              events=events, sudo_key=rules.account_bytes(SUDO_KEY))
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
                                              events=events, sudo_key=rules.account_bytes(SUDO_KEY))
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


def test_an_ordinary_account_creating_its_own_recovery_is_an_alert():
    ext = _signed(ALICE, "Recovery", "create_recovery", friends=[BOB], threshold=1, delay_period=0)
    for events in (None, SUCCEEDED):
        [finding] = _classify_extrinsics([ext], events=events)
        assert finding.severity == rules.ALERT


@pytest.mark.parametrize(
    "ext, authorities",
    [
        (_signed(ALICE, "Recovery", "initiate_recovery", account=SUDO_KEY), frozenset()),
        (_signed(ALICE, "Recovery", "create_recovery", friends=[BOB], threshold=1, delay_period=0),
         frozenset({BOB})),
        (_signed(BOB, "Recovery", "vouch_recovery", lost=ALICE, rescuer=BOB), frozenset({BOB})),
    ],
)
def test_recovery_that_touches_an_authority_account_is_critical(ext, authorities):
    [finding] = rules.classify_materios_block("materios-preprod", 9, [ext], events=SUCCEEDED,
                                              sudo_key=rules.account_bytes(SUDO_KEY),
                                              authorities=frozenset(map(rules.account_bytes, authorities)))
    assert finding.severity == rules.CRITICAL


def test_an_ordinary_signers_findings_share_a_group_and_an_authoritys_page_alone(decoder):
    [ordinary] = _classify_extrinsics([_signed(ALICE, "Recovery", "create_recovery", friends=[BOB],
                                               threshold=1, delay_period=0)])
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
    assert sites[0] == "CRITICAL: Sudo.sudo > Utility.batch_all > System.set_code"
    assert finding.details.index(sites[0]) < finding.details.index("call tree:")


def test_a_batch_of_thousands_of_calls_keeps_a_bounded_finding():
    calls = [_call("System", "set_storage", items=[]) for _ in range(5000)]
    [finding] = _classify_extrinsics([_signed(ALICE, "Utility", "batch", calls=calls)])
    assert len(finding.details) < 250
    assert "4,980 more privileged calls" in finding.render()
    assert "more calls in the tree" in finding.render()


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
