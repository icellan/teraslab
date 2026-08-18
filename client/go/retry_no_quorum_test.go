package teraslab

import "testing"

// W12 TAIL 1 (RED→GREEN) — a PER-ITEM ErrCodeNoQuorum partial is the exact
// shape the scale-up workload produces (a 1-item create batch whose single
// item comes back with code 15) and it must take the SAME-TARGET BACKOFF
// ladder, not terminate immediately.
//
// The server emits code 15 from three sites and documents a back-off-and-retry
// recovery at every one: the NodeId(0) unassigned sentinel returned for every
// shard while a node's activated shard table lags the quorum-committed term
// (narrowed only by the operator opt-in stale_table_partial_serving, default
// OFF), and the read/write paths that turn a redirect with an unknown master
// address into "retryable ERR_NO_QUORUM instead of empty redirect".
//
// Both windows close by WAITING — CI @ fc5e5f7 measured 100ms (node2/node3) to
// 195ms (node1) between "topology committed term=2" and "activating topology
// after exchange phase" on a 3->4 scale-up. Before this change the Go client
// classified such a partial as retryNone: terminal with ZERO retries, strictly
// worse than the Rust client's single zero-delay retry, which was itself
// already too short to outlast the window.
func TestPerItemNoQuorumPartialTakesTheBackoffLadder(t *testing.T) {
	err := &PartialError{Errors: []BatchItemError{{ItemIndex: 0, Code: ErrCodeNoQuorum}}}
	if got := classifyRetry(err, true); got != retryBackoff {
		t.Fatalf("per-item no-quorum partial: classifyRetry = %d, want retryBackoff (%d)", got, retryBackoff)
	}
	// The budget state must not change a per-item verdict: the wait, not the
	// routing refresh, is what clears this window.
	if got := classifyRetry(err, false); got != retryBackoff {
		t.Fatalf("per-item no-quorum partial (no refreshes left): classifyRetry = %d, want retryBackoff (%d)", got, retryBackoff)
	}
}

// W12 TAIL 1 — a GLOBAL no-quorum still spends its immediate routing-refresh
// budget first (the master may simply have moved, and a refresh is free), but
// once that is exhausted it must fall back to the backed-off same-target
// ladder instead of surfacing. maxRefreshRetries refreshes with NO backoff
// re-issue inside the very window this is meant to ride out.
func TestGlobalNoQuorumFallsBackToBackoffAfterRefreshes(t *testing.T) {
	err := &ServerError{Code: ErrCodeNoQuorum}
	if got := classifyRetry(err, true); got != retryRefresh {
		t.Fatalf("global no-quorum with refreshes left: classifyRetry = %d, want retryRefresh (%d)", got, retryRefresh)
	}
	if got := classifyRetry(err, false); got != retryBackoff {
		t.Fatalf("global no-quorum with refreshes spent: classifyRetry = %d, want retryBackoff (%d)", got, retryBackoff)
	}
}

// A StaleRedirectError is a PURE routing signal: there is nothing to wait for,
// so once the refresh budget is spent it must surface rather than sleep out
// the whole ladder for no reason.
func TestStaleRedirectDoesNotFallBackToBackoff(t *testing.T) {
	err := &StaleRedirectError{Addr: "x"}
	if got := classifyRetry(err, true); got != retryRefresh {
		t.Fatalf("stale redirect with refreshes left: classifyRetry = %d, want retryRefresh (%d)", got, retryRefresh)
	}
	if got := classifyRetry(err, false); got != retryNone {
		t.Fatalf("stale redirect with refreshes spent: classifyRetry = %d, want retryNone (%d)", got, retryNone)
	}
}
