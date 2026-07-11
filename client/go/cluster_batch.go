package teraslab

import (
	"bytes"
	"context"
	"fmt"
	"sync"
)

// This file implements cluster-mode fan-out for item- and txid-keyed batch
// operations. Each batch is split by the shard that owns every item's routing
// txid, the per-shard sub-batches are dispatched in parallel (following
// StatusRedirect replies), and per-item results/errors are remapped back into
// the caller's original index space. Mirrors the Rust reference client's
// send_item_batch_cluster (client/rust/src/lib.rs).

// itemSubBatch is one shard's slice of an item batch plus the mapping from
// sub-batch position back to the caller's original index.
type itemSubBatch[T any] struct {
	pool        *connPool
	items       []T
	originalIdx []int
}

// groupItemsByShard partitions items by the node that owns each item's routing
// txid. Like groupTxIDs it never silently drops an item: on a routing miss it
// refreshes the partition map once and retries, then errors if still unroutable.
func groupItemsByShard[T any](c *Client, items []T, txidOf func(T) TxID) (map[*connPool]*itemSubBatch[T], error) {
	try := func() (map[*connPool]*itemSubBatch[T], error) {
		groups := make(map[*connPool]*itemSubBatch[T])
		for i := range items {
			pool, err := c.cluster.poolForTxID(txidOf(items[i]))
			if err != nil {
				return nil, fmt.Errorf("item %d: %w", i, err)
			}
			g, ok := groups[pool]
			if !ok {
				g = &itemSubBatch[T]{pool: pool}
				groups[pool] = g
			}
			g.items = append(g.items, items[i])
			g.originalIdx = append(g.originalIdx, i)
		}
		return groups, nil
	}
	groups, err := try()
	if err == nil {
		return groups, nil
	}
	c.cluster.tryRefresh()
	groups, err = try()
	if err != nil {
		return nil, fmt.Errorf("group items: %w", err)
	}
	return groups, nil
}

// sendItemMutationCluster fans an item-keyed mutation batch out across shards
// and merges per-item errors into the original index space, retrying transient
// failures. Used by Create, Freeze, Unfreeze, Reassign and RemoveConflictingChild.
func sendItemMutationCluster[T any](
	ctx context.Context,
	c *Client,
	opCode uint16,
	items []T,
	txidOf func(T) TxID,
	sizeHint func(n int) int,
	encode func(buf []byte, sub []T) []byte,
) (*BatchResult, error) {
	res, err := withTransientRetry(ctx, c, func() (*BatchResult, error) {
		return sendItemMutationClusterOnce(ctx, c, opCode, items, txidOf, sizeHint, encode)
	})
	return resolveItemRedirects(ctx, c, opCode, items, txidOf, sizeHint, encode, res, err)
}

// resolveItemRedirects re-sends only the items that came back with a per-item
// ERR_REDIRECT after refreshing the partition map, leaving any genuine per-item
// failures intact. Bounded by maxRefreshRetries passes. No-op in single-node
// mode or when there are no redirect-coded errors. Mirrors the Rust client's
// collect_redirect_groups, but routes via the refreshed map rather than the
// per-item redirect address (equivalent, and avoids trusting stale targets).
func resolveItemRedirects[T any](
	ctx context.Context,
	c *Client,
	opCode uint16,
	items []T,
	txidOf func(T) TxID,
	sizeHint func(n int) int,
	encode func(buf []byte, sub []T) []byte,
	res *BatchResult,
	err error,
) (*BatchResult, error) {
	if c.cluster == nil {
		return res, err
	}
	for pass := 0; pass < maxRefreshRetries; pass++ {
		pe, ok := err.(*PartialError)
		if !ok {
			return res, err
		}
		var redirectIdx []int
		var otherErrs []BatchItemError
		for _, be := range pe.Errors {
			if be.Code == ErrCodeRedirect {
				redirectIdx = append(redirectIdx, int(be.ItemIndex))
			} else {
				otherErrs = append(otherErrs, be)
			}
		}
		if len(redirectIdx) == 0 {
			return res, err
		}

		// Refresh the map and re-send only the redirected items.
		c.cluster.tryRefresh()
		sub := make([]T, len(redirectIdx))
		for i, idx := range redirectIdx {
			sub[i] = items[idx]
		}
		_, subErr := sendItemMutationClusterOnce(ctx, c, opCode, sub, txidOf, sizeHint, encode)

		combined := append([]BatchItemError(nil), otherErrs...)
		degraded := pe.Degraded
		if subPe, ok := subErr.(*PartialError); ok {
			degraded = degraded || subPe.Degraded
			combined = append(combined, remapBatchErrors(subPe.Errors, redirectIdx)...)
		} else if subErr != nil {
			return nil, subErr
		}

		if len(combined) == 0 {
			return &BatchResult{}, nil
		}
		res, err = nil, &PartialError{Errors: combined, Degraded: degraded}
	}
	return res, err
}

func sendItemMutationClusterOnce[T any](
	ctx context.Context,
	c *Client,
	opCode uint16,
	items []T,
	txidOf func(T) TxID,
	sizeHint func(n int) int,
	encode func(buf []byte, sub []T) []byte,
) (*BatchResult, error) {
	groups, err := groupItemsByShard(c, items, txidOf)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return &BatchResult{}, nil
	}

	send := func(g *itemSubBatch[T]) error {
		buf := getBuf(sizeHint(len(g.items)))
		payload := encode(buf, g.items)
		resp, err := c.followRedirects(ctx, g.pool, opCode, payload)
		putBuf(payload)
		if err != nil {
			return err
		}
		_, err = handleMutationResponse(resp)
		return err
	}

	if len(groups) == 1 {
		for _, g := range groups {
			err := send(g)
			if pe, ok := err.(*PartialError); ok {
				return nil, &PartialError{Errors: remapBatchErrors(pe.Errors, g.originalIdx), Degraded: pe.Degraded}
			}
			if err != nil {
				return nil, err
			}
			return &BatchResult{}, nil
		}
	}

	type subResult struct {
		err    error
		idxMap []int
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]subResult, 0, len(groups))
	for _, g := range groups {
		wg.Add(1)
		go func(g *itemSubBatch[T]) {
			defer wg.Done()
			err := send(g)
			mu.Lock()
			results = append(results, subResult{err: err, idxMap: g.originalIdx})
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	var allErrors []BatchItemError
	degraded := false
	for _, r := range results {
		if r.err != nil {
			if pe, ok := r.err.(*PartialError); ok {
				degraded = degraded || pe.Degraded
				allErrors = append(allErrors, remapBatchErrors(pe.Errors, r.idxMap)...)
				continue
			}
			return nil, r.err
		}
	}
	if len(allErrors) > 0 {
		return nil, &PartialError{Errors: allErrors, Degraded: degraded}
	}
	return &BatchResult{}, nil
}

// setMinedBatchCluster fans a SetMinedBatch out across shards, merging the
// per-item signals and errors back into the original index space. Unlike the
// generic mutation helper it preserves the signal payload (SpendBatchResponse).
func (c *Client) setMinedBatchCluster(ctx context.Context, params SetMinedBatchParams, txids []TxID) (*SpendBatchResponse, error) {
	res, err := withTransientRetry(ctx, c, func() (*SpendBatchResponse, error) {
		return c.setMinedBatchClusterOnce(ctx, params, txids)
	})
	return c.resolveSignalRedirects(ctx, res, err, func(redirectIdx []int) (*SpendBatchResponse, error) {
		sub := make([]TxID, len(redirectIdx))
		for i, idx := range redirectIdx {
			sub[i] = txids[idx]
		}
		return c.setMinedBatchClusterOnce(ctx, params, sub)
	})
}

func (c *Client) setMinedBatchClusterOnce(ctx context.Context, params SetMinedBatchParams, txids []TxID) (*SpendBatchResponse, error) {
	groups, err := c.groupTxIDs(txids)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return &SpendBatchResponse{}, nil
	}

	send := func(g *txidGroup) (*SpendBatchResponse, error) {
		subTxids := make([]TxID, len(g.originalIdx))
		for i, origIdx := range g.originalIdx {
			subTxids[i] = txids[origIdx]
		}
		buf := getBuf(26 + len(subTxids)*32)
		payload := encodeSetMinedBatch(buf, params, subTxids)
		resp, err := c.followRedirects(ctx, g.pool, OpSetMinedBatch, payload)
		putBuf(payload)
		if err != nil {
			return nil, err
		}
		return handleSignalResponse(resp)
	}

	if len(groups) == 1 {
		for _, g := range groups {
			result, err := send(g)
			remapResult(result, g.originalIdx)
			return result, remapPartialError(err, g.originalIdx)
		}
	}

	type subResult struct {
		result *SpendBatchResponse
		err    error
		idxMap []int
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]subResult, 0, len(groups))
	for _, g := range groups {
		wg.Add(1)
		go func(g *txidGroup) {
			defer wg.Done()
			r, e := send(g)
			mu.Lock()
			results = append(results, subResult{result: r, err: e, idxMap: g.originalIdx})
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	merged := &SpendBatchResponse{}
	var allErrors []BatchItemError
	degraded := false
	for _, r := range results {
		if r.err != nil {
			pe, ok := r.err.(*PartialError)
			if !ok {
				return nil, r.err
			}
			degraded = degraded || pe.Degraded
			for i := range pe.Successes {
				if int(pe.Successes[i].ItemIndex) < len(r.idxMap) {
					pe.Successes[i].ItemIndex = uint32(r.idxMap[pe.Successes[i].ItemIndex])
				}
				merged.Successes = append(merged.Successes, pe.Successes[i])
			}
			allErrors = append(allErrors, remapBatchErrors(pe.Errors, r.idxMap)...)
			continue
		}
		if r.result != nil {
			for i := range r.result.Successes {
				if int(r.result.Successes[i].ItemIndex) < len(r.idxMap) {
					r.result.Successes[i].ItemIndex = uint32(r.idxMap[r.result.Successes[i].ItemIndex])
				}
				merged.Successes = append(merged.Successes, r.result.Successes[i])
			}
		}
	}
	merged.Errors = allErrors
	if len(allErrors) > 0 {
		return merged, &PartialError{Successes: merged.Successes, Errors: allErrors, Degraded: degraded}
	}
	return merged, nil
}

// getBatchCluster fans a GetBatch out across shards and reassembles the
// per-txid results in the caller's original order.
func (c *Client) getBatchCluster(ctx context.Context, fieldMask uint32, txids []TxID) (*GetBatchResult, error) {
	return withTransientRetry(ctx, c, func() (*GetBatchResult, error) {
		return c.getBatchClusterOnce(ctx, fieldMask, txids)
	})
}

func (c *Client) getBatchClusterOnce(ctx context.Context, fieldMask uint32, txids []TxID) (*GetBatchResult, error) {
	groups, err := c.groupTxIDs(txids)
	if err != nil {
		return nil, err
	}
	merged := make([]GetResult, len(txids))
	if len(groups) == 0 {
		return &GetBatchResult{FieldMask: fieldMask, Items: merged}, nil
	}

	send := func(g *txidGroup) ([]GetResult, error) {
		subTxids := make([]TxID, len(g.originalIdx))
		for i, origIdx := range g.originalIdx {
			subTxids[i] = txids[origIdx]
		}
		buf := getBuf(8 + len(subTxids)*32)
		payload := encodeGetBatch(buf, fieldMask, subTxids)
		resp, err := c.followRedirects(ctx, g.pool, OpGetBatch, payload)
		putBuf(payload)
		if err != nil {
			return nil, err
		}
		return decodeGetFrame(resp)
	}

	type subResult struct {
		items  []GetResult
		err    error
		idxMap []int
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]subResult, 0, len(groups))
	for _, g := range groups {
		wg.Add(1)
		go func(g *txidGroup) {
			defer wg.Done()
			items, e := send(g)
			mu.Lock()
			results = append(results, subResult{items: items, err: e, idxMap: g.originalIdx})
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.items) != len(r.idxMap) {
			return nil, fmt.Errorf("get batch: shard returned %d results for %d items", len(r.items), len(r.idxMap))
		}
		for i, origIdx := range r.idxMap {
			merged[origIdx] = r.items[i]
		}
	}
	return &GetBatchResult{FieldMask: fieldMask, Items: merged}, nil
}

// getSpendBatchCluster fans a GetSpendBatch out across shards and reassembles
// the per-item results in the caller's original order.
func (c *Client) getSpendBatchCluster(ctx context.Context, items []GetSpendItem) ([]GetSpendResult, error) {
	return withTransientRetry(ctx, c, func() ([]GetSpendResult, error) {
		return c.getSpendBatchClusterOnce(ctx, items)
	})
}

func (c *Client) getSpendBatchClusterOnce(ctx context.Context, items []GetSpendItem) ([]GetSpendResult, error) {
	groups, err := groupItemsByShard(c, items, func(it GetSpendItem) TxID { return it.TxID })
	if err != nil {
		return nil, err
	}
	merged := make([]GetSpendResult, len(items))
	if len(groups) == 0 {
		return merged, nil
	}

	send := func(g *itemSubBatch[GetSpendItem]) ([]GetSpendResult, error) {
		buf := getBuf(getSpendBatchSize(len(g.items)))
		payload := encodeGetSpendBatch(buf, g.items)
		resp, err := c.followRedirects(ctx, g.pool, OpGetSpendBatch, payload)
		putBuf(payload)
		if err != nil {
			return nil, err
		}
		return decodeGetSpendFrame(resp)
	}

	type subResult struct {
		results []GetSpendResult
		err     error
		idxMap  []int
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]subResult, 0, len(groups))
	for _, g := range groups {
		wg.Add(1)
		go func(g *itemSubBatch[GetSpendItem]) {
			defer wg.Done()
			r, e := send(g)
			mu.Lock()
			results = append(results, subResult{results: r, err: e, idxMap: g.originalIdx})
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.results) != len(r.idxMap) {
			return nil, fmt.Errorf("get spend batch: shard returned %d results for %d items", len(r.results), len(r.idxMap))
		}
		for i, origIdx := range r.idxMap {
			merged[origIdx] = r.results[i]
		}
	}
	return merged, nil
}

// queryNodesUnion runs a diagnostic txid-list query against every distinct node
// and returns the deduplicated union of results. In cluster mode the server
// filters each node's response to the shards it masters, so the union is the
// cluster-wide answer. Used by QueryOldUnmined / QueryConflicting.
//
// Each node caps its response independently, so every node is paged to
// completion (FU#5 resume cursor) before moving to the next. The cursor is
// per-node — it resumes within that node's own sorted candidate set.
//
// Pagination is gated PER NODE (never one client-global verdict applied to every
// node): each node's protocol version is negotiated on its own pool, and a node
// below version 3 is queried once and surfaced as incomplete rather than paged.
// During a rolling upgrade nodes disagree, so a per-client gate could send a
// resume cursor to a still-v2 node that ignores it and loop forever. Two layers
// prevent that: the per-node gate (skips paging a known pre-v3 node) and, load-
// bearing, a non-advancing-cursor guard that stops paging any node whose next
// page does not advance past the cursor just sent. If any node's result is left
// partial (pre-v3 node, or the guard tripped), the deduplicated union is returned
// with ErrQueryTruncated — the other nodes' full contributions are preserved.
func (c *Client) queryNodesUnion(ctx context.Context, opCode uint16, encode func(buf []byte, cursor *TxID) []byte, decode func([]byte) ([]TxID, bool, error)) ([]TxID, error) {
	pools := c.cluster.allPools()
	if len(pools) == 0 {
		return nil, fmt.Errorf("no pools available")
	}
	seen := make(map[TxID]struct{})
	var union []TxID
	incomplete := false
	for _, pool := range pools {
		nodeSupportsPaging := c.nodeSupportsPaging(ctx, pool)
		var cursor *TxID
		for {
			conn, err := pool.get(ctx)
			if err != nil {
				return nil, err
			}
			buf := getBuf(40)
			payload := encode(buf, cursor)
			resp, err := conn.roundTrip(ctx, opCode, 0, payload)
			putBuf(payload)
			if err != nil {
				return nil, err
			}
			if resp.Status != StatusOK {
				if resp.Status == StatusError {
					code, msg, _ := decodeErrorPayload(resp.Payload)
					recyclePayload(resp.Payload)
					return nil, &ServerError{Code: code, Message: msg}
				}
				status := resp.Status
				recyclePayload(resp.Payload)
				return nil, fmt.Errorf("unexpected status: %d", status)
			}
			txids, truncated, err := decode(resp.Payload)
			recyclePayload(resp.Payload)
			if err != nil {
				return nil, err
			}
			for _, t := range txids {
				if _, ok := seen[t]; !ok {
					seen[t] = struct{}{}
					union = append(union, t)
				}
			}
			if !truncated {
				break
			}
			if !nodeSupportsPaging {
				// This node cannot resume from a cursor: keep its partial page and
				// move on rather than looping. The union is flagged incomplete.
				incomplete = true
				break
			}
			if len(txids) == 0 {
				return union, fmt.Errorf("query paging: truncated response with empty page")
			}
			last := txids[len(txids)-1]
			// Non-advancing-cursor guard (load-bearing). The server sorts ascending
			// by txid and a conforming v3 server returns only txids strictly greater
			// than the sent cursor, so the new page's last txid MUST exceed it. If it
			// does not, the server ignored the cursor (a still-v2 node the per-node
			// gate misjudged, or any non-paging node) and paging would loop forever.
			// Stop, keep the partial page, and flag the union incomplete. The very
			// first page has a nil cursor, so the guard only applies from the second
			// round-trip on.
			if cursor != nil && bytes.Compare(last[:], cursor[:]) <= 0 {
				incomplete = true
				break
			}
			cursor = &last
		}
	}
	if incomplete {
		return union, ErrQueryTruncated
	}
	return union, nil
}

// nodeSupportsPaging reports whether the node behind pool speaks protocol version
// >= 3 (the FU#5 resume cursor). The version is negotiated once per pool via
// OP_HELLO and cached on the pool, so a cluster fan-out gates EACH node on its OWN
// version rather than one client-global verdict. When the per-node handshake
// cannot be completed (transient transport failure) it falls back to the client-
// global negotiated version; the non-advancing-cursor guard in queryNodesUnion
// keeps that fallback safe. A cached verdict never over-pages: if a node was
// upgraded v2->v3 after the cache filled, it is merely queried once and surfaced
// as incomplete (safe) until the pool reconnects.
func (c *Client) nodeSupportsPaging(ctx context.Context, pool *connPool) bool {
	v := pool.negotiatedVersion.Load()
	if v == 0 {
		if nv, ok := c.helloPool(ctx, pool); ok {
			pool.negotiatedVersion.Store(uint32(nv))
			v = uint32(nv)
		} else {
			// Inconclusive handshake: fall back to the client-global verdict rather
			// than caching a wrong one. The guard protects correctness regardless.
			v = uint32(c.NegotiatedVersion())
		}
	}
	return v >= 3
}

// helloPool performs the OP_HELLO handshake against a specific pool and returns
// the node's protocol version. ok is false only when the handshake could not be
// completed (dial or transport failure), so the caller can fall back instead of
// caching a wrong verdict; a genuine pre-handshake node replies with an error
// status, which is a definitive version 1 (ok == true).
func (c *Client) helloPool(ctx context.Context, pool *connPool) (version uint16, ok bool) {
	conn, err := pool.get(ctx)
	if err != nil {
		return 0, false
	}
	resp, err := conn.roundTrip(ctx, OpHello, 0, nil)
	if err != nil {
		return 0, false
	}
	defer recyclePayload(resp.Payload)
	switch {
	case resp.Status == StatusError:
		return 1, true
	case resp.Status != StatusOK || len(resp.Payload) < 2:
		return 0, false
	default:
		return getU16(resp.Payload[0:2]), true
	}
}

// decodeGetFrame decodes a GetBatch response frame, recycling its payload.
func decodeGetFrame(resp responseFrame) ([]GetResult, error) {
	switch resp.Status {
	case StatusOK:
		items, err := decodeGetResponse(resp.Payload)
		recyclePayload(resp.Payload)
		return items, err
	case StatusError:
		code, msg, err := decodeErrorPayload(resp.Payload)
		recyclePayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, &ServerError{Code: code, Message: msg}
	case StatusRedirect:
		addr, err := decodeRedirect(resp.Payload)
		recyclePayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode redirect: %w", err)
		}
		return nil, &RedirectError{Addr: addr}
	default:
		recyclePayload(resp.Payload)
		return nil, fmt.Errorf("unexpected status: %d", resp.Status)
	}
}

// decodeGetSpendFrame decodes a GetSpendBatch response frame, recycling its payload.
func decodeGetSpendFrame(resp responseFrame) ([]GetSpendResult, error) {
	switch resp.Status {
	case StatusOK:
		results, err := decodeGetSpendResponse(resp.Payload)
		recyclePayload(resp.Payload)
		return results, err
	case StatusError:
		code, msg, err := decodeErrorPayload(resp.Payload)
		recyclePayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, &ServerError{Code: code, Message: msg}
	case StatusRedirect:
		addr, err := decodeRedirect(resp.Payload)
		recyclePayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode redirect: %w", err)
		}
		return nil, &RedirectError{Addr: addr}
	default:
		recyclePayload(resp.Payload)
		return nil, fmt.Errorf("unexpected status: %d", resp.Status)
	}
}
