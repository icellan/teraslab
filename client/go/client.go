package teraslab

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// bufPool provides reusable byte buffers for encoding request payloads.
// Buffers are returned to the pool after the request is written to the wire.
var bufPool = sync.Pool{
	New: func() any { return make([]byte, 0, 4096) },
}

func getBuf(sizeHint int) []byte {
	buf := bufPool.Get().([]byte)
	buf = buf[:0]
	if cap(buf) < sizeHint {
		return make([]byte, 0, sizeHint)
	}
	return buf
}

func putBuf(buf []byte) {
	if buf != nil {
		bufPool.Put(buf[:0])
	}
}

// ClientConfig configures a TeraSlab client.
type ClientConfig struct {
	// Addr is the server address for single-node mode.
	Addr string
	// Seeds are seed node addresses for cluster mode. If non-empty, overrides Addr.
	Seeds []string
	// Pool configures the per-node connection pool.
	Pool PoolConfig
	// ClusterRefreshInterval is how often to refresh the partition map (default: 30s).
	ClusterRefreshInterval time.Duration
	// MaxRedirects is the maximum redirect retries per request (default: 3).
	MaxRedirects int
}

// Client is a goroutine-safe TeraSlab client. Use New to create one.
type Client struct {
	cfg     ClientConfig
	cluster *cluster  // non-nil in cluster mode
	pool    *connPool // non-nil in single-node mode
}

// New creates a new Client and connects to the server(s).
// In cluster mode (Seeds non-empty), it fetches the initial partition map.
func New(ctx context.Context, cfg ClientConfig) (*Client, error) {
	c := &Client{cfg: cfg}

	if len(cfg.Seeds) > 0 {
		cl, err := newCluster(ctx, ClusterConfig{
			Seeds:           cfg.Seeds,
			PoolConfig:      cfg.Pool,
			RefreshInterval: cfg.ClusterRefreshInterval,
			MaxRedirects:    cfg.MaxRedirects,
		})
		if err != nil {
			return nil, fmt.Errorf("cluster init: %w", err)
		}
		c.cluster = cl
	} else if cfg.Addr != "" {
		c.pool = newPool(cfg.Addr, cfg.Pool)
	} else {
		return nil, fmt.Errorf("either Addr or Seeds must be set")
	}

	return c, nil
}

// Close closes all connections.
func (c *Client) Close() error {
	if c.cluster != nil {
		return c.cluster.close()
	}
	if c.pool != nil {
		return c.pool.close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (c *Client) getConn(ctx context.Context) (*pipeConn, error) {
	if c.pool != nil {
		return c.pool.get(ctx)
	}
	return nil, fmt.Errorf("no default pool in cluster mode; use txid-routed methods")
}

func (c *Client) getConnForTxID(ctx context.Context, txid TxID) (*pipeConn, error) {
	if c.cluster != nil {
		pool, err := c.cluster.poolForTxID(txid)
		if err != nil {
			return nil, err
		}
		return pool.get(ctx)
	}
	return c.pool.get(ctx)
}

func (c *Client) getConnForAnyTxID(ctx context.Context, txids []TxID) (*pipeConn, error) {
	if c.cluster != nil && len(txids) > 0 {
		return c.getConnForTxID(ctx, txids[0])
	}
	return c.getConn(ctx)
}

// sendAndRecycle sends a request using a pooled payload buffer, then returns
// the buffer to the pool after the write is complete.
func (c *Client) sendAndRecycle(ctx context.Context, conn *pipeConn, opCode uint16, payload []byte) (responseFrame, error) {
	resp, err := conn.roundTrip(ctx, opCode, 0, payload)
	putBuf(payload)
	return resp, err
}

func handleMutationResponse(resp responseFrame) (*BatchResult, error) {
	defer recyclePayload(resp.Payload)
	switch resp.Status {
	case StatusOK:
		return &BatchResult{}, nil
	case StatusError:
		code, msg, err := decodeErrorPayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, &ServerError{Code: code, Message: msg}
	case StatusNotFound:
		return nil, &NotFoundError{}
	case StatusRedirect:
		addr, err := decodeRedirect(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode redirect: %w", err)
		}
		return nil, &RedirectError{Addr: addr}
	case StatusPartialError:
		errs, err := decodeSparseErrors(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode partial errors: %w", err)
		}
		return nil, &PartialError{Errors: errs}
	default:
		return nil, fmt.Errorf("unknown status: %d", resp.Status)
	}
}

func handleSignalResponse(resp responseFrame) (*SpendBatchResponse, error) {
	defer recyclePayload(resp.Payload)
	switch resp.Status {
	case StatusOK:
		if len(resp.Payload) > 0 {
			successes, errs, err := decodePartialWithSignals(resp.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode signals: %w", err)
			}
			result := &SpendBatchResponse{Successes: successes, Errors: errs}
			if len(errs) > 0 {
				return result, &PartialError{Successes: successes, Errors: errs}
			}
			return result, nil
		}
		return &SpendBatchResponse{}, nil
	case StatusError:
		code, msg, err := decodeErrorPayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, &ServerError{Code: code, Message: msg}
	case StatusNotFound:
		return nil, &NotFoundError{}
	case StatusRedirect:
		addr, err := decodeRedirect(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode redirect: %w", err)
		}
		return nil, &RedirectError{Addr: addr}
	case StatusPartialError:
		successes, errs, err := decodePartialWithSignals(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode partial: %w", err)
		}
		result := &SpendBatchResponse{Successes: successes, Errors: errs}
		return result, &PartialError{Successes: successes, Errors: errs}
	default:
		return nil, fmt.Errorf("unknown status: %d", resp.Status)
	}
}

// ---------------------------------------------------------------------------
// Cluster-aware batch routing
// ---------------------------------------------------------------------------

type txidGroup struct {
	pool        *connPool
	originalIdx []int
}

func (c *Client) groupTxIDs(txids []TxID) map[*connPool]*txidGroup {
	if c.cluster == nil {
		return nil
	}
	groups := make(map[*connPool]*txidGroup)
	for i, txid := range txids {
		pool, err := c.cluster.poolForTxID(txid)
		if err != nil {
			continue
		}
		g, ok := groups[pool]
		if !ok {
			g = &txidGroup{pool: pool}
			groups[pool] = g
		}
		g.originalIdx = append(g.originalIdx, i)
	}
	return groups
}

// ---------------------------------------------------------------------------
// Mutation operations
// ---------------------------------------------------------------------------

// SpendBatch sends a batch spend request.
func (c *Client) SpendBatch(ctx context.Context, params SpendBatchParams, items []SpendItem) (*SpendBatchResponse, error) {
	if c.cluster != nil {
		return c.spendBatchCluster(ctx, params, items)
	}
	buf := getBuf(spendBatchSize(len(items)))
	payload := encodeSpendBatch(buf, params, items)
	conn, err := c.pool.get(ctx)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpSpendBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleSignalResponse(resp)
}

func (c *Client) spendBatchCluster(ctx context.Context, params SpendBatchParams, items []SpendItem) (*SpendBatchResponse, error) {
	type subBatch struct {
		pool        *connPool
		items       []SpendItem
		originalIdx []int
	}
	groups := make(map[*connPool]*subBatch)
	for i := range items {
		pool, err := c.cluster.poolForTxID(items[i].TxID)
		if err != nil {
			return nil, err
		}
		g, ok := groups[pool]
		if !ok {
			g = &subBatch{pool: pool}
			groups[pool] = g
		}
		g.items = append(g.items, items[i])
		g.originalIdx = append(g.originalIdx, i)
	}

	if len(groups) == 1 {
		for _, g := range groups {
			buf := getBuf(spendBatchSize(len(g.items)))
			payload := encodeSpendBatch(buf, params, g.items)
			conn, err := g.pool.get(ctx)
			if err != nil {
				putBuf(payload)
				return nil, err
			}
			resp, err := c.sendAndRecycle(ctx, conn, OpSpendBatch, payload)
			if err != nil {
				return nil, err
			}
			result, err := handleSignalResponse(resp)
			remapResult(result, g.originalIdx)
			return result, remapPartialError(err, g.originalIdx)
		}
	}

	type subResult struct {
		result *SpendBatchResponse
		err    error
		group  *subBatch
	}
	results := make([]subResult, 0, len(groups))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, g := range groups {
		wg.Add(1)
		go func(g *subBatch) {
			defer wg.Done()
			buf := getBuf(spendBatchSize(len(g.items)))
			payload := encodeSpendBatch(buf, params, g.items)
			conn, err := g.pool.get(ctx)
			if err != nil {
				putBuf(payload)
				mu.Lock()
				results = append(results, subResult{err: err, group: g})
				mu.Unlock()
				return
			}
			resp, err := c.sendAndRecycle(ctx, conn, OpSpendBatch, payload)
			if err != nil {
				mu.Lock()
				results = append(results, subResult{err: err, group: g})
				mu.Unlock()
				return
			}
			r, e := handleSignalResponse(resp)
			mu.Lock()
			results = append(results, subResult{result: r, err: e, group: g})
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	merged := &SpendBatchResponse{}
	var allErrors []BatchItemError
	for _, r := range results {
		if r.err != nil {
			if pe, ok := r.err.(*PartialError); ok {
				for i := range pe.Successes {
					if int(pe.Successes[i].ItemIndex) < len(r.group.originalIdx) {
						pe.Successes[i].ItemIndex = uint32(r.group.originalIdx[pe.Successes[i].ItemIndex])
					}
					merged.Successes = append(merged.Successes, pe.Successes[i])
				}
				for i := range pe.Errors {
					if int(pe.Errors[i].ItemIndex) < len(r.group.originalIdx) {
						pe.Errors[i].ItemIndex = uint32(r.group.originalIdx[pe.Errors[i].ItemIndex])
					}
					allErrors = append(allErrors, pe.Errors[i])
				}
				continue
			}
			return nil, r.err
		}
		if r.result != nil {
			for i := range r.result.Successes {
				if int(r.result.Successes[i].ItemIndex) < len(r.group.originalIdx) {
					r.result.Successes[i].ItemIndex = uint32(r.group.originalIdx[r.result.Successes[i].ItemIndex])
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

func remapResult(r *SpendBatchResponse, indexMap []int) {
	if r == nil {
		return
	}
	for i := range r.Successes {
		if int(r.Successes[i].ItemIndex) < len(indexMap) {
			r.Successes[i].ItemIndex = uint32(indexMap[r.Successes[i].ItemIndex])
		}
	}
	for i := range r.Errors {
		if int(r.Errors[i].ItemIndex) < len(indexMap) {
			r.Errors[i].ItemIndex = uint32(indexMap[r.Errors[i].ItemIndex])
		}
	}
}

func remapPartialError(err error, indexMap []int) error {
	if err == nil {
		return nil
	}
	pe, ok := err.(*PartialError)
	if !ok {
		return err
	}
	for i := range pe.Successes {
		if int(pe.Successes[i].ItemIndex) < len(indexMap) {
			pe.Successes[i].ItemIndex = uint32(indexMap[pe.Successes[i].ItemIndex])
		}
	}
	for i := range pe.Errors {
		if int(pe.Errors[i].ItemIndex) < len(indexMap) {
			pe.Errors[i].ItemIndex = uint32(indexMap[pe.Errors[i].ItemIndex])
		}
	}
	return pe
}

func remapBatchErrors(errs []BatchItemError, indexMap []int) []BatchItemError {
	for i := range errs {
		if int(errs[i].ItemIndex) < len(indexMap) {
			errs[i].ItemIndex = uint32(indexMap[errs[i].ItemIndex])
		}
	}
	return errs
}

// sendTxIDBatch is a helper for cluster-aware txid-list batch operations.
func (c *Client) sendTxIDBatch(ctx context.Context, opCode uint16, txids []TxID, encodePayload func([]byte, []TxID) []byte) (*BatchResult, error) {
	if c.cluster != nil {
		return c.sendTxIDBatchCluster(ctx, opCode, txids, encodePayload)
	}
	buf := getBuf(4 + 16 + len(txids)*32) // generous estimate
	payload := encodePayload(buf, txids)
	conn, err := c.pool.get(ctx)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, opCode, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

func (c *Client) sendTxIDBatchCluster(ctx context.Context, opCode uint16, txids []TxID, encodePayload func([]byte, []TxID) []byte) (*BatchResult, error) {
	groups := c.groupTxIDs(txids)
	if groups == nil || len(groups) <= 1 {
		buf := getBuf(4 + 16 + len(txids)*32)
		payload := encodePayload(buf, txids)
		conn, err := c.getConn(ctx)
		if err != nil {
			for _, g := range groups {
				conn, err = g.pool.get(ctx)
				if err != nil {
					putBuf(payload)
					return nil, err
				}
				break
			}
		}
		resp, err := c.sendAndRecycle(ctx, conn, opCode, payload)
		if err != nil {
			return nil, err
		}
		return handleMutationResponse(resp)
	}

	type subResult struct {
		result *BatchResult
		err    error
		idxMap []int
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]subResult, 0, len(groups))

	for _, g := range groups {
		wg.Add(1)
		go func(g *txidGroup, idxMap []int) {
			defer wg.Done()
			subTxids := make([]TxID, len(idxMap))
			for i, origIdx := range idxMap {
				subTxids[i] = txids[origIdx]
			}
			buf := getBuf(4 + 16 + len(subTxids)*32)
			payload := encodePayload(buf, subTxids)
			conn, err := g.pool.get(ctx)
			if err != nil {
				putBuf(payload)
				mu.Lock()
				results = append(results, subResult{err: err, idxMap: idxMap})
				mu.Unlock()
				return
			}
			resp, err := conn.roundTrip(ctx, opCode, 0, payload)
			putBuf(payload)
			if err != nil {
				mu.Lock()
				results = append(results, subResult{err: err, idxMap: idxMap})
				mu.Unlock()
				return
			}
			r, e := handleMutationResponse(resp)
			mu.Lock()
			results = append(results, subResult{result: r, err: e, idxMap: idxMap})
			mu.Unlock()
		}(g, g.originalIdx)
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

// UnspendBatch sends a batch unspend request.
func (c *Client) UnspendBatch(ctx context.Context, params UnspendBatchParams, items []UnspendItem) (*BatchResult, error) {
	buf := getBuf(12 + len(items)*68)
	payload := encodeUnspendBatch(buf, params, items)
	var conn *pipeConn
	var err error
	if c.cluster != nil && len(items) > 0 {
		conn, err = c.getConnForTxID(ctx, items[0].TxID)
	} else {
		conn, err = c.getConn(ctx)
	}
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpUnspendBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// SetMinedBatch marks transactions as mined in a specific block.
func (c *Client) SetMinedBatch(ctx context.Context, params SetMinedBatchParams, txids []TxID) (*SpendBatchResponse, error) {
	buf := getBuf(26 + len(txids)*32)
	payload := encodeSetMinedBatch(buf, params, txids)
	conn, err := c.getConnForAnyTxID(ctx, txids)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpSetMinedBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleSignalResponse(resp)
}

// CreateBatch creates new transaction records.
func (c *Client) CreateBatch(ctx context.Context, items []CreateItem) (*BatchResult, error) {
	buf := getBuf(4 + len(items)*128)
	payload := encodeCreateBatch(buf, items)
	var conn *pipeConn
	var err error
	if c.cluster != nil && len(items) > 0 {
		conn, err = c.getConnForTxID(ctx, items[0].TxID)
	} else {
		conn, err = c.getConn(ctx)
	}
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpCreateBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// FreezeBatch freezes specific UTXO slots.
func (c *Client) FreezeBatch(ctx context.Context, items []FreezeItem) (*BatchResult, error) {
	buf := getBuf(4 + len(items)*68)
	payload := encodeSlotItemBatch(buf, items)
	var conn *pipeConn
	var err error
	if c.cluster != nil && len(items) > 0 {
		conn, err = c.getConnForTxID(ctx, items[0].TxID)
	} else {
		conn, err = c.getConn(ctx)
	}
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpFreezeBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// UnfreezeBatch unfreezes specific UTXO slots.
func (c *Client) UnfreezeBatch(ctx context.Context, items []FreezeItem) (*BatchResult, error) {
	buf := getBuf(4 + len(items)*68)
	payload := encodeSlotItemBatch(buf, items)
	var conn *pipeConn
	var err error
	if c.cluster != nil && len(items) > 0 {
		conn, err = c.getConnForTxID(ctx, items[0].TxID)
	} else {
		conn, err = c.getConn(ctx)
	}
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpUnfreezeBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// ReassignBatch reassigns frozen UTXO slots with new hashes.
func (c *Client) ReassignBatch(ctx context.Context, params ReassignBatchParams, items []ReassignItem) (*BatchResult, error) {
	buf := getBuf(12 + len(items)*100)
	payload := encodeReassignBatch(buf, params, items)
	var conn *pipeConn
	var err error
	if c.cluster != nil && len(items) > 0 {
		conn, err = c.getConnForTxID(ctx, items[0].TxID)
	} else {
		conn, err = c.getConn(ctx)
	}
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpReassignBatch, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// SetConflictingBatch sets or clears the conflicting flag on transactions.
func (c *Client) SetConflictingBatch(ctx context.Context, params SetConflictingParams, txids []TxID) (*BatchResult, error) {
	return c.sendTxIDBatch(ctx, OpSetConflictingBatch, txids, func(buf []byte, t []TxID) []byte {
		return encodeSetConflictingBatch(buf, params, t)
	})
}

// SetLockedBatch sets or clears the locked flag on transactions.
func (c *Client) SetLockedBatch(ctx context.Context, value bool, txids []TxID) (*BatchResult, error) {
	return c.sendTxIDBatch(ctx, OpSetLockedBatch, txids, func(buf []byte, t []TxID) []byte {
		return encodeSetLockedBatch(buf, value, t)
	})
}

// PreserveUntilBatch sets preserve_until on transactions.
func (c *Client) PreserveUntilBatch(ctx context.Context, blockHeight uint32, txids []TxID) (*BatchResult, error) {
	return c.sendTxIDBatch(ctx, OpPreserveUntilBatch, txids, func(buf []byte, t []TxID) []byte {
		return encodePreserveUntilBatch(buf, blockHeight, t)
	})
}

// DeleteBatch deletes transactions.
func (c *Client) DeleteBatch(ctx context.Context, txids []TxID) (*BatchResult, error) {
	return c.sendTxIDBatch(ctx, OpDeleteBatch, txids, func(buf []byte, t []TxID) []byte {
		return encodeDeleteBatch(buf, t)
	})
}

// MarkLongestChainBatch updates longest-chain status for transactions.
func (c *Client) MarkLongestChainBatch(ctx context.Context, params MarkLongestChainParams, txids []TxID) (*BatchResult, error) {
	return c.sendTxIDBatch(ctx, OpMarkLongestChainBatch, txids, func(buf []byte, t []TxID) []byte {
		return encodeMarkLongestChainBatch(buf, params, t)
	})
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

// GetBatch retrieves transaction data for multiple txids.
func (c *Client) GetBatch(ctx context.Context, fieldMask uint16, txids []TxID) ([]GetResult, error) {
	buf := getBuf(6 + len(txids)*32)
	payload := encodeGetBatch(buf, fieldMask, txids)
	conn, err := c.getConnForAnyTxID(ctx, txids)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpGetBatch, payload)
	if err != nil {
		return nil, err
	}
	switch resp.Status {
	case StatusOK:
		results, err := decodeGetResponse(resp.Payload)
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

// GetSpendBatch looks up spend status for specific UTXO slots.
func (c *Client) GetSpendBatch(ctx context.Context, items []GetSpendItem) ([]GetSpendResult, error) {
	buf := getBuf(4 + len(items)*36)
	payload := encodeGetSpendBatch(buf, items)
	var conn *pipeConn
	var err error
	if c.cluster != nil && len(items) > 0 {
		conn, err = c.getConnForTxID(ctx, items[0].TxID)
	} else {
		conn, err = c.getConn(ctx)
	}
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpGetSpendBatch, payload)
	if err != nil {
		return nil, err
	}
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
	default:
		recyclePayload(resp.Payload)
		return nil, fmt.Errorf("unexpected status: %d", resp.Status)
	}
}

// ---------------------------------------------------------------------------
// Pruner operations
// ---------------------------------------------------------------------------

// QueryOldUnmined queries transactions unmined since before cutoffHeight.
func (c *Client) QueryOldUnmined(ctx context.Context, cutoffHeight uint32) ([]TxID, error) {
	buf := getBuf(4)
	payload := encodeQueryOldUnmined(buf, cutoffHeight)
	conn, err := c.getConn(ctx)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpQueryOldUnmined, payload)
	if err != nil {
		return nil, err
	}
	defer recyclePayload(resp.Payload)
	if resp.Status != StatusOK {
		if resp.Status == StatusError {
			code, msg, _ := decodeErrorPayload(resp.Payload)
			return nil, &ServerError{Code: code, Message: msg}
		}
		return nil, fmt.Errorf("unexpected status: %d", resp.Status)
	}
	return decodeQueryOldUnminedResponse(resp.Payload)
}

// PreserveTransactions preserves transactions until the given block height.
func (c *Client) PreserveTransactions(ctx context.Context, blockHeight uint32, txids []TxID) (*BatchResult, error) {
	buf := getBuf(8 + len(txids)*32)
	payload := encodePreserveTransactions(buf, blockHeight, txids)
	conn, err := c.getConnForAnyTxID(ctx, txids)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpPreserveTransactions, payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// ProcessExpiredPreservations triggers deletion of expired preserved transactions.
func (c *Client) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) (*ProcessExpiredResult, error) {
	buf := getBuf(4)
	payload := encodeProcessExpired(buf, currentHeight)
	conn, err := c.getConn(ctx)
	if err != nil {
		putBuf(payload)
		return nil, err
	}
	resp, err := c.sendAndRecycle(ctx, conn, OpProcessExpiredPreservations, payload)
	if err != nil {
		return nil, err
	}
	defer recyclePayload(resp.Payload)
	if resp.Status != StatusOK {
		if resp.Status == StatusError {
			code, msg, _ := decodeErrorPayload(resp.Payload)
			return nil, &ServerError{Code: code, Message: msg}
		}
		return nil, fmt.Errorf("unexpected status: %d", resp.Status)
	}
	deleted, failed, err := decodeProcessExpiredResponse(resp.Payload)
	if err != nil {
		return nil, err
	}
	return &ProcessExpiredResult{Deleted: deleted, Failed: failed}, nil
}

// ---------------------------------------------------------------------------
// Admin operations
// ---------------------------------------------------------------------------

// Ping sends a ping and returns the round-trip time.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	conn, err := c.getConn(ctx)
	if err != nil {
		return 0, err
	}
	resp, err := conn.roundTrip(ctx, OpPing, 0, nil)
	if err != nil {
		return 0, err
	}
	recyclePayload(resp.Payload)
	if resp.Status != StatusOK {
		return 0, fmt.Errorf("ping: status %d", resp.Status)
	}
	return time.Since(start), nil
}

// Health checks the server health.
func (c *Client) Health(ctx context.Context) error {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	resp, err := conn.roundTrip(ctx, OpHealth, 0, nil)
	if err != nil {
		return err
	}
	recyclePayload(resp.Payload)
	if resp.Status != StatusOK {
		return fmt.Errorf("health: status %d", resp.Status)
	}
	return nil
}

// GetPartitionMap fetches the current cluster partition map.
func (c *Client) GetPartitionMap(ctx context.Context) (*PartitionMap, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := conn.roundTrip(ctx, OpGetPartitionMap, 0, nil)
	if err != nil {
		return nil, err
	}
	defer recyclePayload(resp.Payload)
	if resp.Status != StatusOK {
		if resp.Status == StatusError {
			code, msg, _ := decodeErrorPayload(resp.Payload)
			return nil, &ServerError{Code: code, Message: msg}
		}
		return nil, fmt.Errorf("partition map: status %d", resp.Status)
	}
	return decodePartitionMap(resp.Payload)
}
