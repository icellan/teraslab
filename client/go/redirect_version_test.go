package teraslab

import "testing"

func TestDecodeRedirectWithVersion(t *testing.T) {
	addr := "127.0.0.1:9100"

	t.Run("legacy no version", func(t *testing.T) {
		payload := encodeRedirectPayload(addr)
		gotAddr, ver, has, err := decodeRedirectWithVersion(payload)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if gotAddr != addr || has || ver != 0 {
			t.Fatalf("got (%q, ver=%d, has=%v), want (%q, 0, false)", gotAddr, ver, has, addr)
		}
	})

	t.Run("with version", func(t *testing.T) {
		payload := encodeRedirectPayloadVersion(addr, 42)
		gotAddr, ver, has, err := decodeRedirectWithVersion(payload)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if gotAddr != addr || !has || ver != 42 {
			t.Fatalf("got (%q, ver=%d, has=%v), want (%q, 42, true)", gotAddr, ver, has, addr)
		}
	})

	t.Run("malformed version tail", func(t *testing.T) {
		payload := append(encodeRedirectPayload(addr), 0x01, 0x02, 0x03) // 3 trailing bytes
		if _, _, _, err := decodeRedirectWithVersion(payload); err == nil {
			t.Fatal("expected error for malformed version tail")
		}
	})

	t.Run("decodeRedirect ignores version", func(t *testing.T) {
		got, err := decodeRedirect(encodeRedirectPayloadVersion(addr, 7))
		if err != nil || got != addr {
			t.Fatalf("decodeRedirect = (%q, %v), want (%q, nil)", got, err, addr)
		}
	})
}

func TestClassifyRedirect(t *testing.T) {
	cases := []struct {
		name          string
		serverVersion uint64
		hasVersion    bool
		clientVersion uint64
		want          redirectDecision
	}{
		{"newer follows", 10, true, 5, redirectFollow},
		{"equal is stale", 5, true, 5, redirectStale},
		{"older is stale", 3, true, 5, redirectStale},
		{"no version unknown", 0, false, 5, redirectUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRedirect(tc.serverVersion, tc.hasVersion, tc.clientVersion); got != tc.want {
				t.Fatalf("classifyRedirect = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestClassifyRetry(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want retryAction
	}{
		{"migration backoff", &ServerError{Code: ErrCodeMigrationInProgress}, retryBackoff},
		{"stale epoch backoff", &ServerError{Code: ErrCodeStaleEpoch}, retryBackoff},
		{"replication backoff", &ServerError{Code: ErrCodeReplicationFailed}, retryBackoff},
		{"no quorum refresh", &ServerError{Code: ErrCodeNoQuorum}, retryRefresh},
		{"conflicting none", &ServerError{Code: ErrCodeConflicting}, retryNone},
		{"stale redirect refresh", &StaleRedirectError{Addr: "x"}, retryRefresh},
		{
			"partial all retryable backoff",
			&PartialError{Errors: []BatchItemError{{Code: ErrCodeMigrationInProgress}, {Code: ErrCodeStaleEpoch}}},
			retryBackoff,
		},
		{
			"partial mixed none",
			&PartialError{Errors: []BatchItemError{{Code: ErrCodeMigrationInProgress}, {Code: ErrCodeConflicting}}},
			retryNone,
		},
		{
			"partial with successes none",
			&PartialError{
				Successes: []BatchItemSuccess{{ItemIndex: 0}},
				Errors:    []BatchItemError{{Code: ErrCodeMigrationInProgress}},
			},
			retryNone,
		},
		{"not found none", &NotFoundError{}, retryNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRetry(tc.err); got != tc.want {
				t.Fatalf("classifyRetry = %d, want %d", got, tc.want)
			}
		})
	}
}
