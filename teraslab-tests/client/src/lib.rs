//! TeraSlab test support library.
//!
//! Re-exports the production [`teraslab_client`] crate and provides
//! additional test-specific modules: Docker helpers, workload generation,
//! state verification, and metrics reporting.

// Test-specific modules
pub mod helpers;
pub mod verifier;
pub mod reporter;
pub mod workload;

// Re-export the production client for convenient use in tests
pub use teraslab_client as client;
pub use teraslab_client::{Client, ClientConfig, ClientError, PartialError, PoolConfig};
pub use teraslab_client::types;
pub use teraslab_client::errors;

// Legacy re-exports for backwards compatibility with existing test scenarios.
// These map the old test client API to the new production client.
pub mod connection {
    pub use teraslab_client::errors::ClientError;
}

/// Convenience type alias used by existing tests.
pub type TeraSlabClient = teraslab_client::Client;
