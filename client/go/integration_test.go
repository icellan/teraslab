//go:build integration

package teraslab

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// Integration tests require a running TeraSlab server.
// Set TERASLAB_ADDR environment variable to the server address.
//
// Run with: go test -tags integration -v ./...

func getTestAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("TERASLAB_ADDR")
	if addr == "" {
		addr = "localhost:3300"
	}
	return addr
}

func newTestClient(t *testing.T) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, ClientConfig{
		Addr: getTestAddr(t),
		Pool: PoolConfig{
			MinConns:    1,
			MaxConns:    4,
			DialTimeout: 5 * time.Second,
			HealthCheck: 30 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestIntegrationPing(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	rtt, err := client.Ping(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ping RTT: %v", rtt)
}

func TestIntegrationHealth(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	if err := client.Health(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationGetPartitionMap(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	pm, err := client.GetPartitionMap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("partition map: version=%d, nodes=%d", pm.Version, len(pm.Nodes))
	for _, n := range pm.Nodes {
		t.Logf("  node %d: %s", n.ID, n.Addr)
	}
}

// ---------------------------------------------------------------------------
// Live multi-node cluster tests.
//
// These require a running TeraSlab cluster. Bring one up with e.g.:
//
//	docker compose -f docker/docker-compose.ts01.yml up -d
//
// then export the seed list (host:port of the published client ports), e.g.:
//
//	TERASLAB_CLUSTER_SEEDS=127.0.0.1:13010,127.0.0.1:13011,127.0.0.1:13012
//
// Tests skip when TERASLAB_CLUSTER_SEEDS is unset.
//
// IMPORTANT: fan-out tests dial every node by the address advertised in the
// partition map. With docker-compose the advertised addresses are the cluster's
// internal IPs, which are not routable from the host — run these from inside the
// cluster network, or point at a host-networked cluster whose advertised
// addresses are reachable from the test process.
// ---------------------------------------------------------------------------

func newTestClusterClient(t *testing.T) *Client {
	t.Helper()
	seedsStr := os.Getenv("TERASLAB_CLUSTER_SEEDS")
	if seedsStr == "" {
		t.Skip("set TERASLAB_CLUSTER_SEEDS to run live cluster tests")
	}
	seeds := strings.Split(seedsStr, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := New(ctx, ClientConfig{
		Seeds:                  seeds,
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 4, DialTimeout: 5 * time.Second},
		ClusterRefreshInterval: 5 * time.Second,
		MaxRedirects:           5,
		ClusterSecret:          envSecret(),
	})
	if err != nil {
		t.Fatalf("cluster connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func envSecret() []byte {
	if s := os.Getenv("TERASLAB_CLUSTER_SECRET"); s != "" {
		return []byte(s)
	}
	return nil
}

// TestIntegrationClusterMultiNode asserts the cluster actually spans >1 node,
// otherwise the fan-out tests below would be meaningless.
func TestIntegrationClusterMultiNode(t *testing.T) {
	client := newTestClusterClient(t)
	pm, err := client.GetPartitionMap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pm.Nodes) < 2 {
		t.Fatalf("expected a multi-node cluster, got %d node(s)", len(pm.Nodes))
	}
	t.Logf("cluster: version=%d nodes=%d, negotiated protocol v%d", pm.Version, len(pm.Nodes), client.NegotiatedVersion())
}

// TestIntegrationClusterGetBatchFanOut sends a batch whose txids span many
// shards (hence multiple owning nodes) and asserts the client fans out, then
// reassembles exactly one result per input in original order. The records do
// not exist, so each result is NOT_FOUND — but the routing + reassembly is what
// is under test.
func TestIntegrationClusterGetBatchFanOut(t *testing.T) {
	client := newTestClusterClient(t)
	ctx := context.Background()

	const n = 64
	txids := make([]TxID, n)
	for i := range txids {
		// Vary the first two bytes so the 12-bit shard differs across items.
		txids[i][0] = byte(i)
		txids[i][1] = byte(i * 7)
		txids[i][31] = byte(i)
	}

	res, err := client.GetBatch(ctx, FieldFlags, txids)
	if err != nil {
		t.Fatalf("GetBatch fan-out: %v", err)
	}
	if len(res.Items) != n {
		t.Fatalf("got %d results, want %d", len(res.Items), n)
	}
	for i := range res.Items {
		if res.Items[i].Status != StatusNotFound && res.Items[i].Status != StatusOK {
			t.Fatalf("item %d unexpected status %d", i, res.Items[i].Status)
		}
	}
}

// TestIntegrationClusterQueryUnion exercises the cross-node query fan-out: the
// call must succeed and return the deduplicated union without error.
func TestIntegrationClusterQueryUnion(t *testing.T) {
	client := newTestClusterClient(t)
	ctx := context.Background()
	if _, err := client.QueryOldUnmined(ctx, ^uint32(0)); err != nil {
		t.Fatalf("QueryOldUnmined union: %v", err)
	}
	if _, err := client.QueryConflicting(ctx); err != nil {
		t.Fatalf("QueryConflicting union: %v", err)
	}
}
