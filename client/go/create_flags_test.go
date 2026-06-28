package teraslab

import "testing"

// TestCreateFlagValues pins the CREATE-wire flag bit numbering. The wire
// namespace (locked=0x01, conflicting=0x02, frozen=0x04, external=0x08) is
// distinct from the server's persisted TxFlags; the persisted-LOCKED bit
// (0x04) on the wire would silently create a FROZEN UTXO, so these values are
// load-bearing.
func TestCreateFlagValues(t *testing.T) {
	cases := []struct {
		name string
		got  uint8
		want uint8
	}{
		{"FlagLocked", FlagLocked, 0x01},
		{"FlagConflicting", FlagConflicting, 0x02},
		{"FlagFrozen", FlagFrozen, 0x04},
		{"FlagExternalBlob", FlagExternalBlob, 0x08},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %#02x, want %#02x", c.name, c.got, c.want)
		}
	}
}

// TestCreateFlagsAreDistinctSingleBits verifies each flag is exactly one bit
// and no two flags share a bit.
func TestCreateFlagsAreDistinctSingleBits(t *testing.T) {
	bits := []uint8{FlagLocked, FlagConflicting, FlagFrozen, FlagExternalBlob}
	var or uint8
	var sum uint8
	for _, b := range bits {
		if popcount(b) != 1 {
			t.Errorf("flag %#02x is not a single bit", b)
		}
		or |= b
		sum += b
	}
	if or != sum {
		t.Errorf("flag bits overlap: or=%#02x sum=%#02x", or, sum)
	}
	if or != 0x0F {
		t.Errorf("combined flags = %#02x, want 0x0F", or)
	}
}

func popcount(b uint8) int {
	n := 0
	for b != 0 {
		n += int(b & 1)
		b >>= 1
	}
	return n
}
