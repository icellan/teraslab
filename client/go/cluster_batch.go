package teraslab

import (
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
		if subPe, ok := subErr.(*PartialError); ok {
			combined = append(combined, remapBatchErrors(subPe.Errors, redirectIdx)...)
		} else if subErr != nil {
			return nil, subErr
		}

		if len(combined) == 0 {
			return &BatchResult{}, nil
		}
		res, err = nil, &PartialError{Errors: combined}
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
				return nil, &PartialError{Errors: remapBatchErrors(pe.Errors, g.originalIdx)}
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
	for _, r := range results {
		if r.err != nil {
			if pe, ok := r.err.(*PartialError); ok {
				allErrors = append(allErrors, remapBatchErrors(pe.Errors, r.idxMap)...)
				continue
			}
			return nil, r.err
		}
	}
	if len(allErrors) > 0 {
		return nil, &PartialError{Errors: allErrors}
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
	for _, r := range results {
		if r.err != nil {
			pe, ok := r.err.(*PartialError)
			if !ok {
				return nil, r.err
			}
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
		return merged, &PartialError{Successes: merged.Successes, Errors: allErrors}
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

// queryNodesUnion runs a parameterless txid-list query against every distinct
// node and returns the deduplicated union of results. In cluster mode the
// server filters each node's response to the shards it masters, so the union is
// the cluster-wide answer. Used by QueryOldUnmined / QueryConflicting.
func (c *Client) queryNodesUnion(ctx context.Context, opCode uint16, encode func(buf []byte) []byte, decode func([]byte) ([]TxID, error)) ([]TxID, error) {
	pools := c.cluster.allPools()
	if len(pools) == 0 {
		return nil, fmt.Errorf("no pools available")
	}
	seen := make(map[TxID]struct{})
	var union []TxID
	for _, pool := range pools {
		conn, err := pool.get(ctx)
		if err != nil {
			return nil, err
		}
		buf := getBuf(16)
		payload := encode(buf)
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
		txids, err := decode(resp.Payload)
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
	}
	return union, nil
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
