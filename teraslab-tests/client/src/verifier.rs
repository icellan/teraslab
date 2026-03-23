//! In-memory expected state model for consistency checking.
//!
//! [`StateVerifier`] tracks the expected state of every record that has been
//! created via the test workload. Test scenarios use it to compare the
//! cluster's actual state against what *should* be true, catching consistency
//! bugs such as lost writes, phantom spends, or stale mined flags.
//!
//! The verifier does **not** perform any network I/O itself — it is a pure
//! in-memory model. The test scenarios are responsible for reading from the
//! cluster and comparing with [`get_record`](StateVerifier::get_record).

use parking_lot::RwLock;
use std::collections::HashMap;

/// Wire format metadata section byte offsets (when `FIELD_ALL_METADATA` is used).
///
/// When all per-field metadata bits (0-18) are set — i.e. `FIELD_ALL_METADATA`
/// (`0x0007_FFFF`) — the server serializes the fields in canonical bit order,
/// producing the same 148-byte layout as the old monolithic `FIELD_METADATA`:
///
/// ```text
/// tx_version:          u32     4   offset 0
/// locktime:            u32     4   offset 4
/// fee:                 u64     8   offset 8
/// size_in_bytes:       u64     8   offset 16
/// extended_size:       u64     8   offset 24
/// flags:               u8      1   offset 32
/// spending_height:     u32     4   offset 33
/// created_at:          u64     8   offset 37
/// spent_utxos:         u32     4   offset 45
/// pruned_utxos:        u32     4   offset 49
/// utxo_count:          u32     4   offset 53
/// generation:          u32     4   offset 57
/// updated_at:          u64     8   offset 61
/// unmined_since:       u32     4   offset 69
/// delete_at_height:    u32     4   offset 73
/// preserve_until:      u32     4   offset 77
/// external_ref:                    offset 81
///   store_type:        u8      1   offset 81
///   content_hash:      [u8;32] 32  offset 82
///   total_size:        u64     8   offset 114
///   input_count:       u32     4   offset 122
///   output_count:      u32     4   offset 126
///   inputs_offset:     u64     8   offset 130
///   outputs_offset:    u64     8   offset 138
/// reassignment_count:  u8      1   offset 146
/// block_entry_count:   u8      1   offset 147
/// ```
///
/// Total wire metadata section = 148 bytes (only valid with `FIELD_ALL_METADATA`).
const WIRE_META_FLAGS_OFFSET: usize = 32; // 4+4+8+8+8
const WIRE_META_SPENT_UTXOS_OFFSET: usize = 45; // 32+1+4+8
const WIRE_META_BLOCK_ENTRY_COUNT_OFFSET: usize = 147;
const WIRE_META_SIZE: usize = 148;

/// Extract key verifiable fields from raw GET response data (with `FIELD_ALL_METADATA`).
///
/// Returns `(spent_utxos, is_mined, is_conflicting, is_locked)`.
/// The `is_mined` flag is derived from `block_entry_count > 0`, which is
/// included in the `FIELD_ALL_METADATA` wire section.
///
/// **Important:** The byte offsets used here are only valid when the response
/// was requested with `FIELD_ALL_METADATA` (all per-field metadata bits 0-18).
pub fn parse_metadata_fields(data: &[u8]) -> Option<(u32, bool, bool, bool)> {
    if data.len() < WIRE_META_SIZE {
        return None;
    }
    let flags = data[WIRE_META_FLAGS_OFFSET];
    let is_conflicting = flags & 0b0000_0010 != 0;
    let is_locked = flags & 0b0000_0100 != 0;

    let spent_utxos = u32::from_le_bytes(
        data[WIRE_META_SPENT_UTXOS_OFFSET..WIRE_META_SPENT_UTXOS_OFFSET + 4]
            .try_into()
            .ok()?,
    );

    let block_entry_count = data[WIRE_META_BLOCK_ENTRY_COUNT_OFFSET];
    let is_mined = block_entry_count > 0;

    Some((spent_utxos, is_mined, is_conflicting, is_locked))
}

// ---------------------------------------------------------------------------
// Expected record
// ---------------------------------------------------------------------------

/// The expected state of a single transaction record.
///
/// Each field mirrors the semantics of the server-side UTXO record. The
/// verifier updates these fields as operations are recorded, and the test
/// scenario compares them against the cluster's responses.
#[derive(Debug, Clone)]
pub struct ExpectedRecord {
    /// Number of UTXOs (outputs) in this transaction.
    pub utxo_count: u32,
    /// The UTXO hashes for each output slot.
    pub utxo_hashes: Vec<[u8; 32]>,
    /// Per-slot spend flags (`true` = spent).
    pub spent_slots: Vec<bool>,
    /// Number of outputs currently marked as spent.
    pub spent_utxos: u32,
    /// Whether the transaction has been marked as mined.
    pub is_mined: bool,
    /// Whether the record has been deleted.
    pub is_deleted: bool,
    /// Whether the transaction has been marked as conflicting.
    pub is_conflicting: bool,
    /// Whether the transaction has been marked as locked.
    pub is_locked: bool,
    /// Per-slot freeze flags (`true` = frozen).
    pub frozen_slots: Vec<bool>,
    /// Block height until which the record should be preserved.
    pub preserve_until: u32,
}

// ---------------------------------------------------------------------------
// Mismatch
// ---------------------------------------------------------------------------

/// Describes a single field-level discrepancy between expected and actual state.
#[derive(Debug, Clone)]
pub struct Mismatch {
    /// The txid of the record with the mismatch.
    pub txid: [u8; 32],
    /// Human-readable name of the field that differs.
    pub field: String,
    /// String representation of the expected value.
    pub expected: String,
    /// String representation of the actual value.
    pub actual: String,
}

// ---------------------------------------------------------------------------
// StateVerifier
// ---------------------------------------------------------------------------

/// Thread-safe in-memory model of expected record states.
///
/// All mutation methods acquire a write lock; read methods acquire a read lock.
/// The lock is never held across any async `.await` point because this module
/// is entirely synchronous.
pub struct StateVerifier {
    records: RwLock<HashMap<[u8; 32], ExpectedRecord>>,
}

impl StateVerifier {
    /// Create a new, empty verifier.
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
        }
    }

    /// Record a CREATE operation.
    ///
    /// Inserts a new expected record with the given UTXO count and hashes.
    /// All spend and freeze flags are initialised to `false`, mined/deleted/
    /// conflicting/locked are `false`, and `preserve_until` is 0.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `utxo_count`: Number of outputs.
    /// - `utxo_hashes`: Hash for each output slot (length should equal `utxo_count`).
    pub fn record_create(&self, txid: [u8; 32], utxo_count: u32, utxo_hashes: Vec<[u8; 32]>) {
        let record = ExpectedRecord {
            utxo_count,
            utxo_hashes,
            spent_slots: vec![false; utxo_count as usize],
            spent_utxos: 0,
            is_mined: false,
            is_deleted: false,
            is_conflicting: false,
            is_locked: false,
            frozen_slots: vec![false; utxo_count as usize],
            preserve_until: 0,
        };
        self.records.write().insert(txid, record);
    }

    /// Record a SPEND operation on output `vout`.
    ///
    /// Marks the slot as spent and increments the spent counter.
    /// Does nothing if the txid is unknown or the slot is already spent.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `vout`: Output index to mark as spent.
    pub fn record_spend(&self, txid: [u8; 32], vout: u32) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            let idx = vout as usize;
            if idx < rec.spent_slots.len() && !rec.spent_slots[idx] {
                rec.spent_slots[idx] = true;
                rec.spent_utxos += 1;
            }
        }
    }

    /// Record an UNSPEND operation on output `vout`.
    ///
    /// Clears the spent flag and decrements the spent counter.
    /// Does nothing if the txid is unknown or the slot is not spent.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `vout`: Output index to unmark.
    pub fn record_unspend(&self, txid: [u8; 32], vout: u32) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            let idx = vout as usize;
            if idx < rec.spent_slots.len() && rec.spent_slots[idx] {
                rec.spent_slots[idx] = false;
                rec.spent_utxos -= 1;
            }
        }
    }

    /// Record a SET_MINED operation.
    ///
    /// Marks the transaction as mined. Does nothing if the txid is unknown.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    pub fn record_set_mined(&self, txid: [u8; 32]) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            rec.is_mined = true;
        }
    }

    /// Record an UNSET_MINED operation.
    ///
    /// Clears the mined flag. Does nothing if the txid is unknown.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    pub fn record_unset_mined(&self, txid: [u8; 32]) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            rec.is_mined = false;
        }
    }

    /// Record a FREEZE operation on output `vout`.
    ///
    /// Marks the slot as frozen. Does nothing if the txid is unknown or
    /// the vout is out of range or already frozen.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `vout`: Output index to freeze.
    pub fn record_freeze(&self, txid: [u8; 32], vout: u32) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            let idx = vout as usize;
            if idx < rec.frozen_slots.len() {
                rec.frozen_slots[idx] = true;
            }
        }
    }

    /// Record an UNFREEZE operation on output `vout`.
    ///
    /// Clears the freeze flag. Does nothing if the txid is unknown or
    /// the vout is out of range or not frozen.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `vout`: Output index to unfreeze.
    pub fn record_unfreeze(&self, txid: [u8; 32], vout: u32) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            let idx = vout as usize;
            if idx < rec.frozen_slots.len() {
                rec.frozen_slots[idx] = false;
            }
        }
    }

    /// Record a SET_CONFLICTING operation.
    ///
    /// Sets or clears the conflicting flag. Does nothing if the txid is unknown.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `value`: `true` to mark as conflicting, `false` to clear.
    pub fn record_set_conflicting(&self, txid: [u8; 32], value: bool) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            rec.is_conflicting = value;
        }
    }

    /// Record a SET_LOCKED operation.
    ///
    /// Sets or clears the locked flag. Does nothing if the txid is unknown.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `value`: `true` to lock, `false` to unlock.
    pub fn record_set_locked(&self, txid: [u8; 32], value: bool) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            rec.is_locked = value;
        }
    }

    /// Record a DELETE operation.
    ///
    /// Marks the record as deleted. Does nothing if the txid is unknown.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    pub fn record_delete(&self, txid: [u8; 32]) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            rec.is_deleted = true;
        }
    }

    /// Record a REASSIGN operation on output `vout`.
    ///
    /// Sets a new hash for a frozen output slot. This is used when a frozen
    /// UTXO is reassigned to a new owner.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `vout`: Output index to reassign.
    /// - `new_hash`: The new 32-byte UTXO hash for this slot.
    pub fn record_reassign(&self, txid: [u8; 32], vout: u32, new_hash: [u8; 32]) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            let idx = vout as usize;
            if idx < rec.utxo_hashes.len() {
                rec.utxo_hashes[idx] = new_hash;
            }
        }
    }

    /// Record a PRESERVE_UNTIL operation.
    ///
    /// Sets the block height until which the record should be preserved.
    /// Does nothing if the txid is unknown.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    /// - `height`: Block height to preserve until.
    pub fn record_preserve_until(&self, txid: [u8; 32], height: u32) {
        let mut records = self.records.write();
        if let Some(rec) = records.get_mut(&txid) {
            rec.preserve_until = height;
        }
    }

    /// Returns the number of records tracked by the verifier.
    pub fn record_count(&self) -> usize {
        self.records.read().len()
    }

    /// Returns all tracked txids.
    ///
    /// The order is arbitrary (HashMap iteration order).
    pub fn all_txids(&self) -> Vec<[u8; 32]> {
        self.records.read().keys().copied().collect()
    }

    /// Returns a clone of the expected record for the given txid, or `None`
    /// if the txid is not tracked.
    ///
    /// # Parameters
    /// - `txid`: 32-byte transaction ID.
    pub fn get_record(&self, txid: &[u8; 32]) -> Option<ExpectedRecord> {
        self.records.read().get(txid).cloned()
    }

    /// Returns all tracked txids for records that have NOT been deleted.
    pub fn non_deleted_txids(&self) -> Vec<[u8; 32]> {
        self.records
            .read()
            .iter()
            .filter(|(_, rec)| !rec.is_deleted)
            .map(|(txid, _)| *txid)
            .collect()
    }

    /// Compare actual record data from the cluster against expected state.
    ///
    /// The `actual` parameter contains the raw fields returned by a GET
    /// operation. This method checks every tracked field and returns a
    /// `Vec<Mismatch>` describing all discrepancies. An empty vector means
    /// the record matches perfectly.
    ///
    /// # Parameters
    /// - `txid`: The 32-byte transaction ID.
    /// - `actual_spent_count`: The `spent_utxos` counter from the cluster response.
    /// - `actual_is_mined`: Whether the cluster reports the record as mined.
    /// - `actual_is_conflicting`: Whether the cluster reports the record as conflicting.
    /// - `actual_is_locked`: Whether the cluster reports the record as locked.
    /// - `actual_is_deleted`: Whether the cluster reports the record as deleted (i.e. NotFound).
    pub fn verify_record(
        &self,
        txid: &[u8; 32],
        actual_spent_count: u32,
        actual_is_mined: bool,
        actual_is_conflicting: bool,
        actual_is_locked: bool,
        actual_is_deleted: bool,
    ) -> Vec<Mismatch> {
        let records = self.records.read();
        let Some(expected) = records.get(txid) else {
            if !actual_is_deleted {
                return vec![Mismatch {
                    txid: *txid,
                    field: "existence".to_string(),
                    expected: "not tracked (should not exist)".to_string(),
                    actual: "record exists".to_string(),
                }];
            }
            return vec![];
        };

        let mut mismatches = Vec::new();

        if expected.is_deleted && !actual_is_deleted {
            mismatches.push(Mismatch {
                txid: *txid,
                field: "deleted".to_string(),
                expected: "deleted (NotFound)".to_string(),
                actual: "record exists".to_string(),
            });
            return mismatches;
        }

        if !expected.is_deleted && actual_is_deleted {
            mismatches.push(Mismatch {
                txid: *txid,
                field: "deleted".to_string(),
                expected: "exists".to_string(),
                actual: "deleted (NotFound)".to_string(),
            });
            return mismatches;
        }

        if expected.is_deleted && actual_is_deleted {
            return mismatches;
        }

        if expected.spent_utxos != actual_spent_count {
            mismatches.push(Mismatch {
                txid: *txid,
                field: "spent_utxos".to_string(),
                expected: format!("{}", expected.spent_utxos),
                actual: format!("{actual_spent_count}"),
            });
        }

        if expected.is_mined != actual_is_mined {
            mismatches.push(Mismatch {
                txid: *txid,
                field: "is_mined".to_string(),
                expected: format!("{}", expected.is_mined),
                actual: format!("{actual_is_mined}"),
            });
        }

        if expected.is_conflicting != actual_is_conflicting {
            mismatches.push(Mismatch {
                txid: *txid,
                field: "is_conflicting".to_string(),
                expected: format!("{}", expected.is_conflicting),
                actual: format!("{actual_is_conflicting}"),
            });
        }

        if expected.is_locked != actual_is_locked {
            mismatches.push(Mismatch {
                txid: *txid,
                field: "is_locked".to_string(),
                expected: format!("{}", expected.is_locked),
                actual: format!("{actual_is_locked}"),
            });
        }

        mismatches
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_txid(byte: u8) -> [u8; 32] {
        let mut txid = [0u8; 32];
        txid[0] = byte;
        txid
    }

    fn make_hash(byte: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = byte;
        h
    }

    #[test]
    fn new_verifier_is_empty() {
        let v = StateVerifier::new();
        assert_eq!(v.record_count(), 0);
        assert!(v.all_txids().is_empty());
    }

    #[test]
    fn create_stores_record() {
        let v = StateVerifier::new();
        let txid = make_txid(1);
        let hashes = vec![make_hash(0xAA), make_hash(0xBB)];
        v.record_create(txid, 2, hashes.clone());

        assert_eq!(v.record_count(), 1);
        let rec = v.get_record(&txid).expect("record should exist");
        assert_eq!(rec.utxo_count, 2);
        assert_eq!(rec.utxo_hashes.len(), 2);
        assert_eq!(rec.utxo_hashes[0], make_hash(0xAA));
        assert_eq!(rec.utxo_hashes[1], make_hash(0xBB));
        assert_eq!(rec.spent_slots, vec![false, false]);
        assert_eq!(rec.spent_utxos, 0);
        assert!(!rec.is_mined);
        assert!(!rec.is_deleted);
        assert!(!rec.is_conflicting);
        assert!(!rec.is_locked);
        assert_eq!(rec.frozen_slots, vec![false, false]);
        assert_eq!(rec.preserve_until, 0);
    }

    #[test]
    fn spend_and_unspend() {
        let v = StateVerifier::new();
        let txid = make_txid(2);
        v.record_create(txid, 3, vec![make_hash(1), make_hash(2), make_hash(3)]);

        v.record_spend(txid, 0);
        let rec = v.get_record(&txid).unwrap();
        assert!(rec.spent_slots[0]);
        assert!(!rec.spent_slots[1]);
        assert!(!rec.spent_slots[2]);
        assert_eq!(rec.spent_utxos, 1);

        v.record_spend(txid, 2);
        let rec = v.get_record(&txid).unwrap();
        assert!(rec.spent_slots[0]);
        assert!(!rec.spent_slots[1]);
        assert!(rec.spent_slots[2]);
        assert_eq!(rec.spent_utxos, 2);

        // Spending an already-spent slot is a no-op.
        v.record_spend(txid, 0);
        let rec = v.get_record(&txid).unwrap();
        assert_eq!(rec.spent_utxos, 2);

        v.record_unspend(txid, 0);
        let rec = v.get_record(&txid).unwrap();
        assert!(!rec.spent_slots[0]);
        assert_eq!(rec.spent_utxos, 1);

        // Unspending an already-unspent slot is a no-op.
        v.record_unspend(txid, 0);
        let rec = v.get_record(&txid).unwrap();
        assert_eq!(rec.spent_utxos, 1);
    }

    #[test]
    fn spend_out_of_range_is_noop() {
        let v = StateVerifier::new();
        let txid = make_txid(3);
        v.record_create(txid, 1, vec![make_hash(1)]);

        // vout 5 is out of range for a record with 1 output — should be ignored.
        v.record_spend(txid, 5);
        let rec = v.get_record(&txid).unwrap();
        assert_eq!(rec.spent_utxos, 0);
    }

    #[test]
    fn spend_unknown_txid_is_noop() {
        let v = StateVerifier::new();
        // No record for this txid exists — should not panic.
        v.record_spend(make_txid(99), 0);
    }

    #[test]
    fn set_mined_and_unset_mined() {
        let v = StateVerifier::new();
        let txid = make_txid(4);
        v.record_create(txid, 1, vec![make_hash(1)]);

        v.record_set_mined(txid);
        assert!(v.get_record(&txid).unwrap().is_mined);

        v.record_unset_mined(txid);
        assert!(!v.get_record(&txid).unwrap().is_mined);
    }

    #[test]
    fn freeze_and_unfreeze() {
        let v = StateVerifier::new();
        let txid = make_txid(5);
        v.record_create(txid, 2, vec![make_hash(1), make_hash(2)]);

        v.record_freeze(txid, 1);
        let rec = v.get_record(&txid).unwrap();
        assert!(!rec.frozen_slots[0]);
        assert!(rec.frozen_slots[1]);

        v.record_unfreeze(txid, 1);
        let rec = v.get_record(&txid).unwrap();
        assert!(!rec.frozen_slots[1]);
    }

    #[test]
    fn freeze_out_of_range_is_noop() {
        let v = StateVerifier::new();
        let txid = make_txid(6);
        v.record_create(txid, 1, vec![make_hash(1)]);

        v.record_freeze(txid, 10);
        let rec = v.get_record(&txid).unwrap();
        assert_eq!(rec.frozen_slots, vec![false]);
    }

    #[test]
    fn set_conflicting() {
        let v = StateVerifier::new();
        let txid = make_txid(7);
        v.record_create(txid, 1, vec![make_hash(1)]);

        v.record_set_conflicting(txid, true);
        assert!(v.get_record(&txid).unwrap().is_conflicting);

        v.record_set_conflicting(txid, false);
        assert!(!v.get_record(&txid).unwrap().is_conflicting);
    }

    #[test]
    fn set_locked() {
        let v = StateVerifier::new();
        let txid = make_txid(8);
        v.record_create(txid, 1, vec![make_hash(1)]);

        v.record_set_locked(txid, true);
        assert!(v.get_record(&txid).unwrap().is_locked);

        v.record_set_locked(txid, false);
        assert!(!v.get_record(&txid).unwrap().is_locked);
    }

    #[test]
    fn delete_record() {
        let v = StateVerifier::new();
        let txid = make_txid(9);
        v.record_create(txid, 1, vec![make_hash(1)]);

        v.record_delete(txid);
        assert!(v.get_record(&txid).unwrap().is_deleted);
    }

    #[test]
    fn preserve_until() {
        let v = StateVerifier::new();
        let txid = make_txid(10);
        v.record_create(txid, 1, vec![make_hash(1)]);

        v.record_preserve_until(txid, 800_000);
        assert_eq!(v.get_record(&txid).unwrap().preserve_until, 800_000);

        v.record_preserve_until(txid, 900_000);
        assert_eq!(v.get_record(&txid).unwrap().preserve_until, 900_000);
    }

    #[test]
    fn all_txids_returns_all_created() {
        let v = StateVerifier::new();
        let t1 = make_txid(1);
        let t2 = make_txid(2);
        let t3 = make_txid(3);
        v.record_create(t1, 1, vec![make_hash(1)]);
        v.record_create(t2, 1, vec![make_hash(2)]);
        v.record_create(t3, 1, vec![make_hash(3)]);

        let mut txids = v.all_txids();
        txids.sort();
        let mut expected = vec![t1, t2, t3];
        expected.sort();
        assert_eq!(txids, expected);
    }

    #[test]
    fn get_record_unknown_txid_returns_none() {
        let v = StateVerifier::new();
        assert!(v.get_record(&make_txid(42)).is_none());
    }

    #[test]
    fn get_record_returns_clone() {
        let v = StateVerifier::new();
        let txid = make_txid(11);
        v.record_create(txid, 1, vec![make_hash(1)]);

        let rec = v.get_record(&txid).unwrap();
        // Mutating the clone should not affect the verifier's internal state.
        assert!(!rec.is_mined);
        v.record_set_mined(txid);
        // The previously-obtained clone is unchanged.
        assert!(!rec.is_mined);
        // But a fresh get reflects the update.
        assert!(v.get_record(&txid).unwrap().is_mined);
    }

    #[test]
    fn multiple_operations_on_same_record() {
        let v = StateVerifier::new();
        let txid = make_txid(12);
        v.record_create(txid, 4, vec![make_hash(1), make_hash(2), make_hash(3), make_hash(4)]);

        v.record_spend(txid, 0);
        v.record_spend(txid, 2);
        v.record_set_mined(txid);
        v.record_freeze(txid, 3);
        v.record_set_conflicting(txid, true);
        v.record_set_locked(txid, true);
        v.record_preserve_until(txid, 500_000);

        let rec = v.get_record(&txid).unwrap();
        assert_eq!(rec.utxo_count, 4);
        assert_eq!(rec.spent_slots, vec![true, false, true, false]);
        assert_eq!(rec.spent_utxos, 2);
        assert!(rec.is_mined);
        assert!(!rec.is_deleted);
        assert!(rec.is_conflicting);
        assert!(rec.is_locked);
        assert_eq!(rec.frozen_slots, vec![false, false, false, true]);
        assert_eq!(rec.preserve_until, 500_000);
    }

    #[test]
    fn operations_on_unknown_txid_are_all_noops() {
        let v = StateVerifier::new();
        let unknown = make_txid(0xFF);

        // None of these should panic or modify state.
        v.record_spend(unknown, 0);
        v.record_unspend(unknown, 0);
        v.record_set_mined(unknown);
        v.record_unset_mined(unknown);
        v.record_freeze(unknown, 0);
        v.record_unfreeze(unknown, 0);
        v.record_set_conflicting(unknown, true);
        v.record_set_locked(unknown, true);
        v.record_delete(unknown);
        v.record_preserve_until(unknown, 100);

        assert_eq!(v.record_count(), 0);
    }
}
