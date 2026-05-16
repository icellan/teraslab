//! F-G9-017 regression: subdirectory walk failures during `list`/`list_for_gc`
//! must be counted and exposed via `FileBlobStore::walk_failures()`, not just
//! logged at `warn` and forgotten.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

#[test]
fn unreadable_subdir_increments_walk_failures_counter() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileBlobStore::new(dir.path(), 2));

    // Seed two blobs so the prefix tree has real subdirs.
    let key_a = {
        let mut k = [0u8; 32];
        k[0] = 0xAA;
        k
    };
    let key_b = {
        let mut k = [0u8; 32];
        k[0] = 0xBB;
        k
    };
    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-b").unwrap();

    // Baseline list: both entries enumerate, no failures.
    let listed = store.list().unwrap();
    assert_eq!(listed.len(), 2);
    let baseline = store.walk_failures();
    assert_eq!(baseline, 0, "no failures expected with intact filesystem");

    // Make one of the top-level prefix directories (the `aa/` branch) entirely
    // unreadable so `read_dir` on it returns Err.
    let aa_dir = dir.path().join("aa");
    assert!(aa_dir.exists(), "expected prefix dir to exist");
    let original = std::fs::metadata(&aa_dir).unwrap().permissions();
    // 0o000 — no permission at all. The `read_dir` of `aa/` will succeed (it's
    // listed in the root) but `read_dir` of `aa/aa/` (the next prefix level)
    // will fail because we can't traverse `aa/`.
    std::fs::set_permissions(&aa_dir, std::fs::Permissions::from_mode(0o000)).unwrap();

    // Make sure we restore perms even if the test panics later.
    struct RestorePerms<'a>(&'a std::path::Path, std::fs::Permissions);
    impl Drop for RestorePerms<'_> {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(self.0, self.1.clone());
        }
    }
    let _restore = RestorePerms(&aa_dir, original);

    // Walk again — the unreadable subdir produces a `walk_failures`
    // increment. We don't strictly require how many failures (depends on
    // tree depth) — only that the count strictly increases.
    let _ = store.list().unwrap();
    let after = store.walk_failures();
    assert!(
        after > baseline,
        "F-G9-017 regression: walk_failures did not increment for unreadable subdir \
         (baseline={baseline}, after={after})"
    );
}
