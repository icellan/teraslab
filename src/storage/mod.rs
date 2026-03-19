//! Tiered storage for transaction cold data (inputs, outputs, inpoints).
//!
//! Three tiers:
//! - **Inline** (< 8 KiB): appended to record at deterministic offset
//! - **Separate NVMe** (8 KiB – 1 MiB): separate device allocation
//! - **External blob** (> 1 MiB): file or S3 backend

pub mod blobstore;
pub mod input_refs;
pub mod manager;
pub mod tiers;
pub mod uploader;
