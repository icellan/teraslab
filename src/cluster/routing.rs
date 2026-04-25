//! Client routing information.
//!
//! Provides structured types for serving the shard table to clients,
//! so they can route requests directly to the correct master node.

use crate::cluster::shards::{NUM_SHARDS, NodeId};
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
    /// Members of the committed topology term (sorted by NodeId).
    /// Used by the topology catch-up mechanism to construct synthetic
    /// commits with the correct digest. Empty if not present in the wire
    /// format (backward compatibility with older servers).
    pub committed_members: Vec<NodeId>,
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
            committed_members: Vec::new(),
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
            let addr_len = u16::from_le_bytes(data[pos + 8..pos + 10].try_into().ok()?) as usize;
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

        // Parse optional committed_members appended after shard assignments.
        // Backward compatible: if not present, committed_members is empty.
        let mut committed_members = Vec::new();
        if pos + 4 <= data.len() {
            let cm_count =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap_or([0; 4])) as usize;
            pos += 4;
            for _ in 0..cm_count {
                if pos + 8 > data.len() {
                    break;
                }
                committed_members.push(NodeId(u64::from_le_bytes(
                    data[pos..pos + 8].try_into().unwrap_or([0; 8]),
                )));
                pos += 8;
            }
        }

        Some(Self {
            shard_table_version: version,
            nodes,
            shard_assignments: assignments,
            committed_members,
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
    fn routing_info_committed_members_backward_compat() {
        // RoutingInfo encoded WITHOUT committed_members (old server)
        // should decode with empty committed_members.
        let info = RoutingInfo::new(
            42,
            vec![NodeInfo {
                id: NodeId(1),
                addr: "127.0.0.1:3000".parse().unwrap(),
                is_alive: true,
            }],
            (0..NUM_SHARDS as u16).map(|s| (s, NodeId(1))).collect(),
        );
        let encoded = info.encode();
        let decoded = RoutingInfo::decode(&encoded).unwrap();
        assert!(
            decoded.committed_members.is_empty(),
            "old format should have empty committed_members"
        );
    }

    #[test]
    fn routing_info_committed_members_present() {
        // Simulate a partition map payload that includes committed_members
        // appended after the shard assignments.
        let info = RoutingInfo::new(
            5,
            vec![
                NodeInfo {
                    id: NodeId(1),
                    addr: "127.0.0.1:3000".parse().unwrap(),
                    is_alive: true,
                },
                NodeInfo {
                    id: NodeId(2),
                    addr: "127.0.0.1:3001".parse().unwrap(),
                    is_alive: true,
                },
                NodeInfo {
                    id: NodeId(3),
                    addr: "127.0.0.1:3002".parse().unwrap(),
                    is_alive: false,
                },
            ],
            (0..NUM_SHARDS as u16).map(|s| (s, NodeId(1))).collect(),
        );
        let mut encoded = info.encode();

        // Append committed_members [NodeId(1), NodeId(3)] — different from
        // the alive nodes — to simulate the scenario where the committed
        // topology has a different member set than SWIM's current view.
        encoded.extend_from_slice(&2u32.to_le_bytes()); // count = 2
        encoded.extend_from_slice(&1u64.to_le_bytes()); // NodeId(1)
        encoded.extend_from_slice(&3u64.to_le_bytes()); // NodeId(3)

        let decoded = RoutingInfo::decode(&encoded).unwrap();
        assert_eq!(decoded.committed_members.len(), 2);
        assert_eq!(decoded.committed_members[0], NodeId(1));
        assert_eq!(decoded.committed_members[1], NodeId(3));
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
            (0..NUM_SHARDS as u16).map(|s| (s, NodeId(100))).collect(),
        );

        let encoded = info.encode();
        let decoded = RoutingInfo::decode(&encoded).unwrap();
        assert_eq!(decoded.nodes.len(), 1);
        assert_eq!(decoded.nodes[0].id, NodeId(100));

        for (shard, master) in &decoded.shard_assignments {
            assert_eq!(*master, NodeId(100), "shard {shard} should map to node 100");
        }
    }

    // -----------------------------------------------------------------------
    // Part 5.1: Stale routing — version in response
    // -----------------------------------------------------------------------

    #[test]
    fn routing_info_preserves_version_through_encode_decode() {
        for version in [0, 1, 42, u64::MAX] {
            let info = RoutingInfo::new(
                version,
                vec![NodeInfo {
                    id: NodeId(1),
                    addr: "127.0.0.1:3000".parse().unwrap(),
                    is_alive: true,
                }],
                (0..NUM_SHARDS as u16).map(|s| (s, NodeId(1))).collect(),
            );
            let decoded = RoutingInfo::decode(&info.encode()).unwrap();
            assert_eq!(decoded.shard_table_version, version);
        }
    }

    // -----------------------------------------------------------------------
    // Part 5: All 4096 shards present in decoded routing info
    // -----------------------------------------------------------------------

    #[test]
    fn decoded_routing_info_covers_all_shards() {
        let members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 5);
        let assignments: Vec<(u16, NodeId)> = (0..NUM_SHARDS as u16)
            .map(|s| (s, table.target_assignment(s).master))
            .collect();
        let nodes = members
            .iter()
            .map(|&id| NodeInfo {
                id,
                addr: format!("127.0.0.1:{}", 3000 + id.0).parse().unwrap(),
                is_alive: true,
            })
            .collect();

        let info = RoutingInfo::new(5, nodes, assignments);
        let decoded = RoutingInfo::decode(&info.encode()).unwrap();

        assert_eq!(decoded.shard_assignments.len(), NUM_SHARDS);
        for (shard, master) in &decoded.shard_assignments {
            assert!(
                members.contains(master),
                "shard {shard} has master {master:?} not in member list"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Part 5: Routing info with many nodes
    // -----------------------------------------------------------------------

    #[test]
    fn routing_info_10_nodes_round_trip() {
        let nodes: Vec<NodeInfo> = (1..=10u64)
            .map(|id| NodeInfo {
                id: NodeId(id),
                addr: format!("127.0.0.1:{}", 3000 + id).parse().unwrap(),
                is_alive: id % 3 != 0, // some dead
            })
            .collect();
        let assignments: Vec<(u16, NodeId)> = (0..NUM_SHARDS as u16)
            .map(|s| (s, NodeId((s as u64 % 10) + 1)))
            .collect();

        let info = RoutingInfo::new(42, nodes, assignments);
        let decoded = RoutingInfo::decode(&info.encode()).unwrap();

        assert_eq!(decoded.shard_table_version, 42);
        assert_eq!(decoded.nodes.len(), 10);
        // Check alive flags
        for node in &decoded.nodes {
            assert_eq!(node.is_alive, node.id.0 % 3 != 0);
        }
    }

    // -----------------------------------------------------------------------
    // Part 5: Malformed routing info handled gracefully
    // -----------------------------------------------------------------------

    #[test]
    fn routing_info_decode_too_short_for_shards() {
        // Valid header but not enough bytes for 4096 shard entries
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes()); // version
        data.extend_from_slice(&0u32.to_le_bytes()); // 0 nodes
        // Only a few shard bytes instead of 4096 * 8
        data.extend_from_slice(&[0u8; 16]);
        assert!(RoutingInfo::decode(&data).is_none());
    }
}
