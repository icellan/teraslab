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

// sendAndRecycle sends a request using a pooled payload buffer, then returns
// the buffer to the pool after the write is complete.
func (c *Client) sendAndRecycle(ctx context.Context, conn *pipeConn, opCode uint16, payload []byte) (responseFrame, error) {
	resp, err := conn.roundTrip(ctx, opCode, 0, payload)
	putBuf(payload)
	return resp, err
}

// maxRedirectsFor returns the configured per-request redirect cap.
// Defaults to 3 when ClusterConfig.MaxRedirects is not set.
func (c *Client) maxRedirectsFor() int {
	if c.cluster == nil {
		return 0
	}
	n := c.cluster.config.MaxRedirects
	if n <= 0 {
		return 3
	}
	return n
}

// txidRoutedRoundTrip performs a roundTrip routed by txid in cluster mode,
// transparently following StatusRedirect replies up to MaxRedirects. The
// payload is owned by the caller (typically a pooled getBuf slice) and is
// re-sent verbatim on each redirect attempt.
//
// On success it returns the final responseFrame as-is; the caller is
// responsible for status-handling and payload recycling exactly as before.
// If the redirect chain exceeds MaxRedirects, returns *TooManyRedirectsError.
//
// In single-node mode (c.cluster == nil) it falls back to a single round trip
// against the default pool and does NOT follow redirects — single-node
// callers historically receive *RedirectError directly, and that contract
// is preserved.
func (c *Client) txidRoutedRoundTrip(ctx context.Context, txid TxID, opCode uint16, payload []byte) (responseFrame, error) {
	if c.cluster == nil {
		conn, err := c.getConn(ctx)
		if err != nil {
			return responseFrame{}, err
		}
		return conn.roundTrip(ctx, opCode, 0, payload)
	}

	pool, err := c.cluster.poolForTxID(txid)
	if err != nil {
		return responseFrame{}, err
	}
	return c.followRedirects(ctx, pool, opCode, payload)
}

// followRedirects sends a request to pool and, while the server replies with
// StatusRedirect, refreshes the partition map and retries against the new
// owner up to MaxRedirects times. Each retry recycles the previous redirect
// reply payload before issuing the next request.
//
// Cluster mode only; single-node callers must not invoke this.
func (c *Client) followRedirects(ctx context.Context, pool *connPool, opCode uint16, payload []byte) (responseFrame, error) {
	if c.cluster == nil {
		// Defensive: single-node mode should never reach here, but if it does
		// we issue a single round-trip and bubble any redirect verbatim.
		conn, err := pool.get(ctx)
		if err != nil {
			return responseFrame{}, err
		}
		return conn.roundTrip(ctx, opCode, 0, payload)
	}
	maxHops := c.maxRedirectsFor()
	lastAddr := ""
	for hop := 0; hop <= maxHops; hop++ {
		conn, err := pool.get(ctx)
		if err != nil {
			return responseFrame{}, err
		}
		resp, err := conn.roundTrip(ctx, opCode, 0, payload)
		if err != nil {
			return responseFrame{}, err
		}
		if resp.Status != StatusRedirect {
			return resp, nil
		}
		// Decode the redirect target, recycle the response payload, and route
		// the next attempt to the new owner.
		addr, decErr := decodeRedirect(resp.Payload)
		recyclePayload(resp.Payload)
		if decErr != nil {
			return responseFrame{}, fmt.Errorf("decode redirect: %w", decErr)
		}
		lastAddr = addr
		newPool, hrErr := c.cluster.handleRedirect(addr)
		if hrErr != nil {
			return responseFrame{}, fmt.Errorf("handle redirect to %s: %w", addr, hrErr)
		}
		pool = newPool
	}
	return responseFrame{}, &TooManyRedirectsError{Hops: maxHops, LastAddr: lastAddr}
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
		// Try signal format first (successes + errors), fall back to sparse errors only
		successes, errs, err := decodePartialWithSignals(resp.Payload)
		if err != nil {
			// Server may send sparse errors without success section
			errs, err = decodeSparseErrors(resp.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode partial: %w", err)
			}
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
			resp, err := c.followRedirects(ctx, g.pool, OpSpendBatch, payload)
			putBuf(payload)
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
			resp, err := c.followRedirects(ctx, g.pool, OpSpendBatch, payload)
			putBuf(payload)
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
// In cluster mode each shard's sub-batch follows StatusRedirect replies up
// to MaxRedirects.
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
		// Pick a pool — either the single group's pool or any seed.
		var pool *connPool
		for _, g := range groups {
			pool = g.pool
			break
		}
		if pool == nil {
			// No groups (empty txids); fall back to any pool for the request.
			c.cluster.mu.RLock()
			for _, p := range c.cluster.pools {
				pool = p
				break
			}
			c.cluster.mu.RUnlock()
			if pool == nil {
				putBuf(payload)
				return nil, fmt.Errorf("no pools available")
			}
		}
		resp, err := c.followRedirects(ctx, pool, opCode, payload)
		putBuf(payload)
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
			resp, err := c.followRedirects(ctx, g.pool, opCode, payload)
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

// UnspendBatch sends a batch unspend request. In cluster mode the call
// follows StatusRedirect replies up to MaxRedirects.
func (c *Client) UnspendBatch(ctx context.Context, params UnspendBatchParams, items []UnspendItem) (*BatchResult, error) {
	buf := getBuf(unspendBatchSize(len(items)))
	payload := encodeUnspendBatch(buf, params, items)
	resp, err := c.roundTripWithFirstTxID(ctx, OpUnspendBatch, payload, txidOfUnspendItems(items))
	putBuf(payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// roundTripWithFirstTxID dispatches a single-payload request using the given
// txid for cluster routing (if present) and follows redirects. When txid is
// the zero value, it routes via the default single-node pool path.
//
// In cluster mode with a nil/empty txid, the request is sent to an arbitrary
// node; the caller should provide a real txid for correct routing.
func (c *Client) roundTripWithFirstTxID(ctx context.Context, opCode uint16, payload []byte, txid *TxID) (responseFrame, error) {
	if c.cluster != nil && txid != nil {
		return c.txidRoutedRoundTrip(ctx, *txid, opCode, payload)
	}
	conn, err := c.getConn(ctx)
	if err != nil {
		return responseFrame{}, err
	}
	return conn.roundTrip(ctx, opCode, 0, payload)
}

func txidOfUnspendItems(items []UnspendItem) *TxID {
	if len(items) == 0 {
		return nil
	}
	t := items[0].TxID
	return &t
}

func txidOfFreezeItems(items []FreezeItem) *TxID {
	if len(items) == 0 {
		return nil
	}
	t := items[0].TxID
	return &t
}

func txidOfReassignItems(items []ReassignItem) *TxID {
	if len(items) == 0 {
		return nil
	}
	t := items[0].TxID
	return &t
}

func txidOfCreateItems(items []CreateItem) *TxID {
	if len(items) == 0 {
		return nil
	}
	t := items[0].TxID
	return &t
}

func txidOfGetSpendItems(items []GetSpendItem) *TxID {
	if len(items) == 0 {
		return nil
	}
	t := items[0].TxID
	return &t
}

func firstTxID(txids []TxID) *TxID {
	if len(txids) == 0 {
		return nil
	}
	t := txids[0]
	return &t
}

// SetMinedBatch marks transactions as mined in a specific block. Follows
// StatusRedirect replies in cluster mode up to MaxRedirects.
func (c *Client) SetMinedBatch(ctx context.Context, params SetMinedBatchParams, txids []TxID) (*SpendBatchResponse, error) {
	buf := getBuf(26 + len(txids)*32)
	payload := encodeSetMinedBatch(buf, params, txids)
	resp, err := c.roundTripWithFirstTxID(ctx, OpSetMinedBatch, payload, firstTxID(txids))
	putBuf(payload)
	if err != nil {
		return nil, err
	}
	return handleSignalResponse(resp)
}

// CreateBatch creates new transaction records.
//
// If any item's cold_data exceeds BlobUploadThreshold, the cold data is
// pre-uploaded via OP_STREAM_CHUNK / OP_STREAM_END and the item's Flags
// are updated to include FlagExternalBlob. The inlined TxData in the batch
// payload is cleared for those items. The caller's items slice is not modified;
// a shallow copy is made when blob uploads are needed.
func (c *Client) CreateBatch(ctx context.Context, items []CreateItem) (*BatchResult, error) {
	// Check for items that need blob upload and upload them first.
	items, err := c.uploadLargeBlobs(ctx, items)
	if err != nil {
		return nil, err
	}

	buf := getBuf(4 + len(items)*128)
	payload := encodeCreateBatch(buf, items)
	resp, err := c.roundTripWithFirstTxID(ctx, OpCreateBatch, payload, txidOfCreateItems(items))
	putBuf(payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// uploadLargeBlobs checks each item's cold_data size and pre-uploads any that
// exceed BlobUploadThreshold via chunked streaming. Items that are uploaded
// have their TxData cleared and FlagExternalBlob set. Returns a (possibly
// copied) items slice; the original is never mutated.
func (c *Client) uploadLargeBlobs(ctx context.Context, items []CreateItem) ([]CreateItem, error) {
	// Fast path: check if any item needs blob upload.
	needsCopy := false
	for i := range items {
		if coldDataSize(&items[i]) > BlobUploadThreshold {
			needsCopy = true
			break
		}
	}
	if !needsCopy {
		return items, nil
	}

	// Make a shallow copy so we don't mutate the caller's slice.
	copied := make([]CreateItem, len(items))
	copy(copied, items)

	for i := range copied {
		cdSize := coldDataSize(&copied[i])
		if cdSize <= BlobUploadThreshold {
			continue
		}

		coldBytes := encodeColdData(&copied[i])
		if err := c.uploadBlob(ctx, copied[i].TxID, coldBytes); err != nil {
			return nil, fmt.Errorf("blob upload for item %d: %w", i, err)
		}

		// Clear TxData and set the external blob flag.
		copied[i].TxData = TxData{}
		copied[i].Flags |= FlagExternalBlob
	}

	return copied, nil
}

// uploadBlob uploads large cold_data in chunks via OP_STREAM_CHUNK / OP_STREAM_END.
// All chunks are sent to the shard master for the given txid. Each chunk is
// sent as an independent request-response round trip; the server accumulates
// them keyed by txid. Follows StatusRedirect in cluster mode.
func (c *Client) uploadBlob(ctx context.Context, txid TxID, data []byte) error {
	t := txid // local copy for firstTxID-style pointer
	var offset uint64
	for offset < uint64(len(data)) {
		end := offset + BlobChunkSize
		if end > uint64(len(data)) {
			end = uint64(len(data))
		}
		chunk := data[offset:end]

		buf := getBuf(32 + 8 + 4 + len(chunk))
		payload := encodeStreamChunk(buf, txid, offset, chunk)
		resp, err := c.roundTripWithFirstTxID(ctx, OpStreamChunk, payload, &t)
		putBuf(payload)
		if err != nil {
			return fmt.Errorf("stream chunk at offset %d: %w", offset, err)
		}
		if resp.Status != StatusOK {
			code, msg, decErr := decodeErrorPayload(resp.Payload)
			recyclePayload(resp.Payload)
			if decErr != nil {
				return fmt.Errorf("stream chunk at offset %d: status %d", offset, resp.Status)
			}
			return &ServerError{Code: code, Message: msg}
		}
		recyclePayload(resp.Payload)

		offset = end
	}

	// Finalize the upload.
	buf := getBuf(40)
	payload := encodeStreamEnd(buf, txid, uint64(len(data)))
	resp, err := c.roundTripWithFirstTxID(ctx, OpStreamEnd, payload, &t)
	putBuf(payload)
	if err != nil {
		return fmt.Errorf("stream end: %w", err)
	}
	if resp.Status != StatusOK {
		code, msg, decErr := decodeErrorPayload(resp.Payload)
		recyclePayload(resp.Payload)
		if decErr != nil {
			return fmt.Errorf("stream end: status %d", resp.Status)
		}
		return &ServerError{Code: code, Message: msg}
	}
	recyclePayload(resp.Payload)

	return nil
}

// FreezeBatch freezes specific UTXO slots. Follows StatusRedirect in cluster mode.
func (c *Client) FreezeBatch(ctx context.Context, items []FreezeItem) (*BatchResult, error) {
	buf := getBuf(4 + len(items)*68)
	payload := encodeSlotItemBatch(buf, items)
	resp, err := c.roundTripWithFirstTxID(ctx, OpFreezeBatch, payload, txidOfFreezeItems(items))
	putBuf(payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// UnfreezeBatch unfreezes specific UTXO slots. Follows StatusRedirect in cluster mode.
func (c *Client) UnfreezeBatch(ctx context.Context, items []FreezeItem) (*BatchResult, error) {
	buf := getBuf(4 + len(items)*68)
	payload := encodeSlotItemBatch(buf, items)
	resp, err := c.roundTripWithFirstTxID(ctx, OpUnfreezeBatch, payload, txidOfFreezeItems(items))
	putBuf(payload)
	if err != nil {
		return nil, err
	}
	return handleMutationResponse(resp)
}

// ReassignBatch reassigns frozen UTXO slots with new hashes. Follows
// StatusRedirect in cluster mode.
func (c *Client) ReassignBatch(ctx context.Context, params ReassignBatchParams, items []ReassignItem) (*BatchResult, error) {
	buf := getBuf(12 + len(items)*100)
	payload := encodeReassignBatch(buf, params, items)
	resp, err := c.roundTripWithFirstTxID(ctx, OpReassignBatch, payload, txidOfReassignItems(items))
	putBuf(payload)
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
//
// Returns a [GetBatchResult] that bundles the field mask with the per-item
// results, enabling zero-alloc field accessors.
func (c *Client) GetBatch(ctx context.Context, fieldMask uint32, txids []TxID) (*GetBatchResult, error) {
	buf := getBuf(8 + len(txids)*32)
	payload := encodeGetBatch(buf, fieldMask, txids)
	resp, err := c.roundTripWithFirstTxID(ctx, OpGetBatch, payload, firstTxID(txids))
	putBuf(payload)
	if err != nil {
		return nil, err
	}
	switch resp.Status {
	case StatusOK:
		items, err := decodeGetResponse(resp.Payload)
		recyclePayload(resp.Payload)
		if err != nil {
			return nil, err
		}
		return &GetBatchResult{FieldMask: fieldMask, Items: items}, nil
	case StatusError:
		code, msg, err := decodeErrorPayload(resp.Payload)
		recyclePayload(resp.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, &ServerError{Code: code, Message: msg}
	case StatusRedirect:
		// In cluster mode roundTripWithFirstTxID already follows redirects;
		// reaching this branch means single-node mode received a redirect or
		// the redirect cap was exceeded (in which case we returned earlier).
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

// GetSpendBatch looks up spend status for specific UTXO slots. Follows
// StatusRedirect in cluster mode up to MaxRedirects.
func (c *Client) GetSpendBatch(ctx context.Context, items []GetSpendItem) ([]GetSpendResult, error) {
	buf := getBuf(getSpendBatchSize(len(items)))
	payload := encodeGetSpendBatch(buf, items)
	resp, err := c.roundTripWithFirstTxID(ctx, OpGetSpendBatch, payload, txidOfGetSpendItems(items))
	putBuf(payload)
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
// Follows StatusRedirect in cluster mode.
func (c *Client) PreserveTransactions(ctx context.Context, blockHeight uint32, txids []TxID) (*BatchResult, error) {
	buf := getBuf(8 + len(txids)*32)
	payload := encodePreserveTransactions(buf, blockHeight, txids)
	resp, err := c.roundTripWithFirstTxID(ctx, OpPreserveTransactions, payload, firstTxID(txids))
	putBuf(payload)
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
