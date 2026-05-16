//! Tiered storage for transaction cold data (inputs, outputs, inpoints).
//!
//! Production tiers:
//! - **Inline** (< 8 KiB): appended to record at deterministic offset
//! - **External blob** (> 8 KiB): file or S3 backend

pub mod blob_gc;
pub mod blobstore;
pub mod input_refs;
pub mod manager;
pub mod tiers;
pub mod uploader;
