package teraslab

import "testing"

func TestShardForTxIDDeterministic(t *testing.T) {
	var txid TxID
	txid[0] = 0xAB
	txid[1] = 0xCD
	s1 := ShardForTxID(txid)
	s2 := ShardForTxID(txid)
	if s1 != s2 {
		t.Errorf("shard not deterministic: %d != %d", s1, s2)
	}
	if s1 >= NumShards {
		t.Errorf("shard %d >= %d", s1, NumShards)
	}
}

func TestShardForTxIDMatchesRust(t *testing.T) {
	// Rust: let h = u16::from_le_bytes([key.txid[0], key.txid[1]]); h & 0x0FFF
	// For txid[0]=0xAB, txid[1]=0xCD: LE u16 = 0xCDAB, & 0x0FFF = 0x0DAB = 3499
	var txid TxID
	txid[0] = 0xAB
	txid[1] = 0xCD
	shard := ShardForTxID(txid)
	expected := uint16(0xCDAB & 0x0FFF)
	if shard != expected {
		t.Errorf("shard = %d, want %d", shard, expected)
	}
}

func TestShardForTxIDDistribution(t *testing.T) {
	counts := make([]int, NumShards)
	for i := range 100_000 {
		var txid TxID
		txid[0] = byte(i)
		txid[1] = byte(i >> 8)
		txid[2] = byte(i >> 16)
		txid[3] = byte(i >> 24)
		shard := ShardForTxID(txid)
		counts[shard]++
	}
	expected := 100_000.0 / float64(NumShards)
	maxDev := 0.0
	for _, c := range counts {
		d := float64(c) - expected
		if d < 0 {
			d = -d
		}
		if d > maxDev {
			maxDev = d
		}
	}
	// Within 50% of expected per shard.
	if maxDev > expected*0.5 {
		t.Errorf("distribution too skewed: max deviation %.1f (expected ~%.1f)", maxDev, expected)
	}
}

func TestShardForTxIDRange(t *testing.T) {
	for i := range 10_000 {
		var txid TxID
		txid[0] = byte(i)
		txid[1] = byte(i >> 8)
		shard := ShardForTxID(txid)
		if shard >= NumShards {
			t.Fatalf("shard %d >= %d for i=%d", shard, NumShards, i)
		}
	}
}
