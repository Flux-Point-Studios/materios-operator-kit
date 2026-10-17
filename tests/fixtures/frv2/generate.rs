use pallet_intent_settlement::{
    account_to_bytes, settlement_request_id, slash_bad_settlement_evidence_payload,
};
use pallet_intent_settlement::types::{ClaimId, FraudProof, SettlementEvidence, SettlementRequestRecord};
use parity_scale_codec::Encode;
use serde_json::json;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn vector<B: Copy + Into<u64> + Encode>(submitted: B, direct: bool, proof: FraudProof) -> serde_json::Value {
    let chain_id = [0x11; 32];
    let claim_id = ClaimId::from([0x22; 32]);
    let request = SettlementRequestRecord {
        requester: core::array::from_fn::<u8, 32, _>(|i| i as u8),
        evidence: SettlementEvidence {
            cardano_tx_hash: [0x44; 32], observed_at_depth: 0x12345678,
            observed_slot: 0x123456789abcdef0, beneficiary_addr_hash: [0x55; 28],
            amount_lovelace: u64::MAX, mainchain_genesis_hash: [0x77; 32],
        },
        submitted_block: submitted, settled_direct: direct, bond_amount: 10_000_000,
    };
    let preimage = (account_to_bytes(&request.requester), submitted.into(), request.evidence, direct).encode();
    let request_id = settlement_request_id(&request);
    assert_eq!(sp_io::hashing::blake2_256(&preimage), request_id, "request preimage drifted from the pallet");
    json!({
        "chain_id": hex(&chain_id), "claim_id": hex(claim_id.as_bytes()),
        "requester": hex(&request.requester), "submitted_block": submitted.into(),
        "block_scale_width": submitted.encode().len(), "settled_direct": direct,
        "cardano_tx_hash": hex(&request.evidence.cardano_tx_hash),
        "observed_at_depth": request.evidence.observed_at_depth,
        "observed_slot": request.evidence.observed_slot,
        "beneficiary_addr_hash": hex(&request.evidence.beneficiary_addr_hash),
        "amount_lovelace": request.evidence.amount_lovelace,
        "mainchain_genesis_hash": hex(&request.evidence.mainchain_genesis_hash),
        "request_preimage": hex(&preimage), "request_id": hex(&request_id),
        "proof_scale": hex(&proof.encode()),
        "digest": hex(&slash_bad_settlement_evidence_payload(&chain_id, &claim_id, &request, &proof)),
    })
}

fn main() {
    let vectors = vec![
        vector(100u32, false, FraudProof::TxNotFound),
        vector(100u64, false, FraudProof::TxNotFound),
        vector(u32::MAX, true, FraudProof::WrongAmount { actual_lovelace: u128::MAX }),
        vector(u64::MAX, false, FraudProof::WrongBeneficiary { actual_payment_hash: [0x99; 28] }),
        vector(0u32, true, FraudProof::WrongAmount { actual_lovelace: 0 }),
        vector(100u64, true, FraudProof::TxNotFound),
    ];
    println!("{}", serde_json::to_string_pretty(&vectors).expect("serialize vectors"));
}
