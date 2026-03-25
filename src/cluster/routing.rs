//! Client routing information.
//!
//! Provides structured types for serving the shard table to clients,
//! so they can route requests directly to the correct master node.

use crate::cluster::shards::{NodeId, NUM_SHARDS};
use std::net::SocketAddr;

/// Complete routing information for the cluster.
///
/// Served to clients via `OP_GET_PARTITION_MAP`. Contains enough
/// information for the client to route any key to the correct node
/// without contacting the wrong node first.
#[derive(Debug, Clone)]
pub struct RoutingInfo {
    /// Monotonically increasing version derived from the member list.
    pub shard_table_version: u64,
    /// All known nodes (alive and recently-dead).
    pub nodes: Vec<NodeInfo>,
    /// Shard-to-master mapping: `shard_assignments[shard] = master NodeId`.
    pub shard_assignments: Vec<(u16, NodeId)>,
}

/// Information about a single cluster node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: NodeId,
    /// TCP address for client connections.
    pub addr: SocketAddr,
    /// Whether SWIM considers this node alive.
    pub is_alive: bool,
}

impl RoutingInfo {
    /// Build routing info from the current cluster state.
    ///
    /// `nodes` should include all known nodes (alive ones are marked as such).
    /// `shard_table_version` comes from the current `ShardTable`.
    /// `assignments` maps each shard (0–4095) to its master `NodeId`.
    pub fn new(
        shard_table_version: u64,
        nodes: Vec<NodeInfo>,
        assignments: Vec<(u16, NodeId)>,
    ) -> Self {
        Self {
            shard_table_version,
            nodes,
            shard_assignments: assignments,
        }
    }

    /// Encode routing info to a binary payload for the wire protocol.
    ///
    /// Format:
    /// ```text
    /// [version:8][node_count:4]
    /// [node_id:8][addr_len:2][addr:N][is_alive:1] × node_count
    /// [master_node_id:8] × 4096
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.shard_table_version.to_le_bytes());
        buf.extend_from_slice(&(self.nodes.len() as u32).to_le_bytes());

        for node in &self.nodes {
            buf.extend_from_slice(&node.id.0.to_le_bytes());
            let addr_str = node.addr.to_string();
            let addr_bytes = addr_str.as_bytes();
            buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(addr_bytes);
            buf.push(if node.is_alive { 1 } else { 0 });
        }

        // Build O(1) lookup from the shard assignments, then encode
        // all 4096 shards as 8-byte master NodeIds. This replaces the
        // previous O(n) .find() per shard which was O(4096²) total.
        let mut shard_masters = [NodeId(0); NUM_SHARDS];
        for &(shard, master) in &self.shard_assignments {
            shard_masters[shard as usize] = master;
        }
        for master in &shard_masters {
            buf.extend_from_slice(&master.0.to_le_bytes());
        }

        buf
    }

    /// Decode routing info from a binary payload.
    ///
    /// Returns `None` if the payload is malformed or truncated.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        let version = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let node_count = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;

        let mut pos = 12;
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            if pos + 10 > data.len() {
                return None;
            }
            let id = NodeId(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            let addr_len =
                u16::from_le_bytes(data[pos + 8..pos + 10].try_into().ok()?) as usize;
            pos += 10;
            if pos + addr_len + 1 > data.len() {
                return None;
            }
            let addr_str = std::str::from_utf8(&data[pos..pos + addr_len]).ok()?;
            let addr: SocketAddr = addr_str.parse().ok()?;
            pos += addr_len;
            let is_alive = data[pos] != 0;
            pos += 1;
            nodes.push(NodeInfo { id, addr, is_alive });
        }

        let mut assignments = Vec::with_capacity(NUM_SHARDS);
        for shard in 0..NUM_SHARDS as u16 {
            if pos + 8 > data.len() {
                return None;
            }
            let master = NodeId(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
            assignments.push((shard, master));
        }

        Some(Self {
            shard_table_version: version,
            nodes,
            shard_assignments: assignments,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_info_round_trip() {
        let info = RoutingInfo::new(
            42,
            vec![
                NodeInfo {
                    id: NodeId(1),
                    addr: "127.0.0.1:3000".parse().unwrap(),
                    is_alive: true,
                },
                NodeInfo {
                    id: NodeId(2),
                    addr: "127.0.0.1:3001".parse().unwrap(),
                    is_alive: false,
                },
            ],
            (0..NUM_SHARDS as u16)
                .map(|s| (s, NodeId(if s % 2 == 0 { 1 } else { 2 })))
                .collect(),
        );

        let encoded = info.encode();
        let decoded = RoutingInfo::decode(&encoded).expect("decode should succeed");

        assert_eq!(decoded.shard_table_version, 42);
        assert_eq!(decoded.nodes.len(), 2);
        assert_eq!(decoded.nodes[0].id, NodeId(1));
        assert!(decoded.nodes[0].is_alive);
        assert_eq!(decoded.nodes[1].id, NodeId(2));
        assert!(!decoded.nodes[1].is_alive);
        assert_eq!(decoded.shard_assignments.len(), NUM_SHARDS);
        assert_eq!(decoded.shard_assignments[0].1, NodeId(1));
        assert_eq!(decoded.shard_assignments[1].1, NodeId(2));
    }

    #[test]
    fn routing_info_decode_truncated() {
        assert!(RoutingInfo::decode(&[0u8; 5]).is_none());
    }

    #[test]
    fn routing_info_single_node() {
        let info = RoutingInfo::new(
            1,
            vec![NodeInfo {
                id: NodeId(100),
                addr: "10.0.0.1:5000".parse().unwrap(),
                is_alive: true,
            }],
            (0..NUM_SHARDS as u16)
                .map(|s| (s, NodeId(100)))
                .collect(),
        );

        let encoded = info.encode();
        let decoded = RoutingInfo::decode(&encoded).unwrap();
        assert_eq!(decoded.nodes.len(), 1);
        assert_eq!(decoded.nodes[0].id, NodeId(100));

        for (shard, master) in &decoded.shard_assignments {
            assert_eq!(*master, NodeId(100), "shard {shard} should map to node 100");
        }
    }
}
