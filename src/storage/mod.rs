//! Tiered storage for transaction cold data (inputs, outputs, inpoints).
//!
//! Production tiers:
//! - **Inline** (`<=` 8 KiB: 8192 bytes serialized, 8180 bytes user data
//!   after the 12-byte `ColdData` length prefixes): appended to record at
//!   deterministic offset
//! - **External blob** (> 8192 bytes serialized): file or S3 backend

pub mod blob_gc;
pub mod blobstore;
pub mod input_refs;
pub mod manager;
pub mod tiers;
pub mod uploader;
