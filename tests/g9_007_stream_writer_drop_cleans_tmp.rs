//! F-G9-007 regression: a `FileStreamWriter` dropped without an explicit
//! `finish` or `abort` (e.g. after a panic between `begin_stream` and the
//! dispatcher's stream-registration) must not leave its `.tmp` file behind.
//!
//! Pre-fix: only the dispatcher's `abort` path removed the temp file. A
//! drop on the unwind path leaked it for up to five minutes (the
//! `STALE_TMP_AGE_SECS` sweep window).

use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

#[test]
fn dropping_stream_writer_without_finish_or_abort_removes_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileBlobStore::new(dir.path(), 2);
    let key = [0x37u8; 32];

    {
        let mut writer = store.begin_stream(&key).unwrap();
        writer.write_chunk(b"some-bytes").unwrap();
        // Intentional: drop without calling `finish` or `abort`.
        drop(writer);
    }

    // Walk the entire tempdir tree and assert no `.tmp` file remains.
    let mut tmp_files: Vec<std::path::PathBuf> = Vec::new();
    let mut stack = vec![dir.path().to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = match std::fs::read_dir(&d) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(path);
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.contains(".tmp")
            {
                tmp_files.push(path);
            }
        }
    }

    assert!(
        tmp_files.is_empty(),
        "F-G9-007 regression: dropped writer left {} tmp files: {:?}",
        tmp_files.len(),
        tmp_files
    );

    // The blob itself must not be present (we never finished).
    assert!(store.get(&key).unwrap().is_none(), "blob must not exist after drop without finish");
}

#[test]
fn finished_stream_writer_leaves_payload_present() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileBlobStore::new(dir.path(), 2);
    let key = [0x38u8; 32];

    let mut writer = store.begin_stream(&key).unwrap();
    writer.write_chunk(b"finished-payload").unwrap();
    writer.finish().unwrap();

    let bytes = store.get(&key).unwrap().expect("blob present after finish");
    assert_eq!(bytes, b"finished-payload");
}

#[test]
fn aborted_stream_writer_leaves_no_payload_and_no_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileBlobStore::new(dir.path(), 2);
    let key = [0x39u8; 32];

    let mut writer = store.begin_stream(&key).unwrap();
    writer.write_chunk(b"will-be-aborted").unwrap();
    writer.abort().unwrap();

    assert!(store.get(&key).unwrap().is_none(), "blob must not exist after abort");
}
