// Package teraslab provides a Go client for the TeraSlab binary wire protocol.
//
// TeraSlab is a purpose-built UTXO store for BSV Teranode. This client implements
// the full wire protocol including connection pooling, request pipelining,
// cluster-aware shard routing, and typed error handling.
//
// # Single-node usage
//
//	client, err := teraslab.New(ctx, teraslab.ClientConfig{
//	    Addr: "localhost:3300",
//	})
//	defer client.Close()
//
//	resp, err := client.SpendBatch(ctx, params, items)
//
// # Cluster usage
//
//	client, err := teraslab.New(ctx, teraslab.ClientConfig{
//	    Seeds: []string{"node1:3300", "node2:3300", "node3:3300"},
//	})
//	defer client.Close()
//
// All batch operations accept context.Context for cancellation and timeouts.
// The Client is safe for concurrent use by multiple goroutines.
package teraslab
