//go:build integration

package teraslab

import (
	"context"
	"os"
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
