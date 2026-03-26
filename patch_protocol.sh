sed -i '' 's/const OP_PRUNE_SLOT: u8 = 13;/const OP_PRUNE_SLOT: u8 = 13;\nconst OP_MARK_LONGEST_CHAIN: u8 = 14;/g' src/replication/protocol.rs
