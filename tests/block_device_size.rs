//! J-03 — exercise the block-device size-query ioctl branch against a real
//! block device.
//!
//! Every other `DirectDevice` test opens a regular file, so `is_block` is
//! always `false` and the macOS ioctl size-query branch
//! (`DKIOCGETBLOCKCOUNT` x `DKIOCGETBLOCKSIZE` -> `block_device_size_from_geometry`)
//! never runs against a real `S_IFBLK` node. The size it computes is the
//! `actual_size` every on-device OOB bounds check trusts, so a wrong value
//! means lost capacity or out-of-bounds record I/O on a real `/dev/nvme0n1`.
//!
//! On macOS `hdiutil attach -nomount ram://<sectors>` creates an
//! unprivileged RAM-backed block device owned by the current user — no
//! `sudo`, no loop device. The buffered `/dev/diskN` node is `S_IFBLK`
//! (`brw-r-----`), which is exactly what `DirectDevice::open`'s `fstat`
//! detection keys on; the raw `/dev/rdiskN` node is `S_IFCHR` and would
//! NOT drive `is_block == true`, so we deliberately target `/dev/diskN`.
//!
//! If `hdiutil` is unavailable or the attach fails (e.g. a CI sandbox), the
//! test returns early with an explanatory `eprintln!` rather than failing —
//! a `#[ignore]` is banned by project rules, so this is a runtime capability
//! check. On a developer macOS box with `hdiutil` the body runs in full and
//! asserts the size end-to-end.

#![cfg(target_os = "macos")]
#![allow(clippy::disallowed_macros)] // integration test uses eprintln! to report a clean capability-absent skip

use std::path::PathBuf;
use std::process::Command;

use teraslab::device::{AlignedBuf, BlockDevice, DeviceError, DirectDevice};

/// Sector size `hdiutil` uses for `ram://` devices (512-byte sectors).
const SECTOR_SIZE: u64 = 512;
/// Number of sectors to request: 4096 * 512 = 2 MiB. Non-trivial and well
/// above the 4096-byte I/O alignment we open the device with.
const SECTORS: u64 = 4096;
/// Expected total byte size of the RAM disk.
const EXPECTED_SIZE: u64 = SECTORS * SECTOR_SIZE;

/// RAII guard that detaches the RAM disk on drop, including on panic /
/// assertion failure. Never leak a RAM disk.
struct RamDisk {
    dev_node: String,
}

impl RamDisk {
    /// Attach a RAM disk of `SECTORS` 512-byte sectors and parse the
    /// `/dev/diskN` block node from `hdiutil`'s stdout.
    ///
    /// Returns `Ok(None)` if `hdiutil` is missing or the attach failed in a
    /// way that indicates the capability is simply unavailable (sandbox),
    /// so the caller can skip cleanly. Returns `Err` only for an
    /// unexpected-but-present-hdiutil parse failure, which is a real test
    /// problem worth surfacing.
    fn attach() -> Result<Option<Self>, String> {
        let spec = format!("ram://{SECTORS}");
        let output = match Command::new("hdiutil")
            .args(["attach", "-nomount", &spec])
            .output()
        {
            Ok(o) => o,
            Err(e) => {
                // hdiutil not on PATH / not executable -> capability absent.
                eprintln!("J-03 skip: hdiutil not available ({e}); cannot exercise block device");
                return Ok(None);
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stdout) + String::from_utf8_lossy(&output.stderr);
            eprintln!(
                "J-03 skip: `hdiutil attach -nomount {spec}` failed (status {:?}): {}",
                output.status.code(),
                stderr.trim()
            );
            return Ok(None);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // hdiutil prints e.g. "/dev/disk10        \t                \n".
        // The block node is the first whitespace-delimited token on the
        // first line that starts with "/dev/disk".
        let dev_node = stdout
            .split_whitespace()
            .find(|tok| tok.starts_with("/dev/disk"))
            .map(|s| s.to_string());

        match dev_node {
            Some(dev_node) => Ok(Some(Self { dev_node })),
            None => {
                // hdiutil succeeded but we could not parse a node — detach is
                // impossible without the node, but to avoid leaking we make a
                // best-effort blanket detach attempt is not feasible here.
                Err(format!(
                    "hdiutil attach succeeded but no /dev/disk node found in output: {stdout:?}"
                ))
            }
        }
    }

    /// The buffered block node path (`S_IFBLK`), which drives `is_block`.
    fn block_path(&self) -> PathBuf {
        PathBuf::from(&self.dev_node)
    }
}

impl Drop for RamDisk {
    fn drop(&mut self) {
        // Best-effort detach; never panic in Drop. Use `force` so a transient
        // busy state from the just-closed fd does not leave the disk attached.
        let _ = Command::new("hdiutil")
            .args(["detach", &self.dev_node, "-force"])
            .output();
    }
}

#[test]
fn block_device_size_query_against_real_ram_disk() {
    let ram = match RamDisk::attach() {
        Ok(Some(ram)) => ram,
        Ok(None) => return, // capability absent — skip cleanly, assert nothing
        Err(e) => panic!("RAM disk setup failed: {e}"),
    };

    let block_path = ram.block_path();

    // Open through the production constructor. `size` here is the regular-file
    // grow hint; for a block device it MUST be ignored and the kernel ioctl
    // geometry used instead — pass a deliberately wrong, tiny value to prove
    // the ioctl branch (not the hint) supplies the size.
    let bogus_hint = 4096u64;
    let dev = DirectDevice::open(&block_path, bogus_hint, 4096)
        .expect("DirectDevice::open on RAM-disk block node must succeed");

    // 1. The node must be detected as a block device (S_IFBLK), proving we
    //    targeted /dev/diskN and not /dev/rdiskN, and that the ioctl branch
    //    actually ran.
    assert!(
        dev.is_block_device(),
        "RAM-disk node {} must be detected as S_IFBLK; if false, the ioctl \
         size-query branch never ran and J-03 is not covered",
        block_path.display()
    );

    // 2. End-to-end size assertion: this is the J-03 gap. The size returned
    //    must equal block_count * block_size from the ioctls, NOT the bogus
    //    4096-byte open hint.
    assert_eq!(
        dev.size(),
        EXPECTED_SIZE,
        "block-device size-query ioctl branch returned the wrong size: \
         expected {EXPECTED_SIZE} ({SECTORS} sectors x {SECTOR_SIZE}), got {} \
         (open hint was {bogus_hint}, which must have been ignored)",
        dev.size()
    );
    assert_ne!(
        dev.size(),
        bogus_hint,
        "size() returned the open hint, not the ioctl-reported geometry"
    );

    // 3. The bounds check that consumes `actual_size` must fire on a real
    //    device: a pread starting exactly at size() (one aligned block past
    //    the last valid block) is out of bounds.
    let mut oob_buf = AlignedBuf::new(4096, 4096);
    let oob_offset = EXPECTED_SIZE; // == size(), first offset past the end
    match dev.pread(&mut oob_buf, oob_offset) {
        Err(DeviceError::OutOfBounds {
            offset,
            device_size,
            ..
        }) => {
            assert_eq!(offset, oob_offset, "OutOfBounds reported wrong offset");
            assert_eq!(
                device_size, EXPECTED_SIZE,
                "OutOfBounds reported a device_size that disagrees with size()"
            );
        }
        Err(other) => panic!("expected OutOfBounds past end of device, got {other:?}"),
        Ok(n) => panic!("expected OOB error reading past device end, but read {n} bytes"),
    }

    // 4. Aligned positional I/O round-trip at offset 0 on the real block node.
    //    macOS does not require O_DIRECT buffer alignment the way Linux does
    //    (it uses F_NOCACHE, advisory), so a 4096-aligned AlignedBuf round
    //    trip on a 512-sector RAM disk works reliably. This proves real
    //    positional pread/pwrite against the S_IFBLK node, not just the size
    //    query.
    //
    //    Note: we deliberately do NOT call `dev.sync()` here. `sync_all()`
    //    issues `fsync(2)`, which a macOS `hdiutil` RAM disk rejects with
    //    `ENOTTY` ("Inappropriate ioctl for device") — that is a property of
    //    the RAM-disk backend, not of TeraSlab, and is unrelated to the J-03
    //    size-query gap. The pread/pwrite round-trip below already proves the
    //    write reached the device.
    let mut write_buf = AlignedBuf::new(4096, 4096);
    for (i, b) in write_buf.iter_mut().enumerate() {
        *b = (i % 251) as u8; // 251 prime -> distinct, non-trivial pattern
    }
    dev.pwrite_all_at(&write_buf, 0)
        .expect("aligned pwrite at offset 0 on RAM-disk block node must succeed");

    let mut read_buf = AlignedBuf::new(4096, 4096);
    dev.pread_exact_at(&mut read_buf, 0)
        .expect("aligned pread at offset 0 on RAM-disk block node must succeed");
    assert_eq!(
        &*read_buf, &*write_buf,
        "round-trip data mismatch on real block device"
    );

    // `ram` (RamDisk) drops here -> hdiutil detach runs. Drop also runs on any
    // panic above, so the RAM disk is never leaked.
}
