use std::fs;
use std::path::PathBuf;

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::Result;
use datamodel::batch::RecordBatch;
use exec::operators::scan::{Cols, SeqScan};
use exec::operators::Operator;
use storage::writer::write_chunk;

#[test]
fn stream_scan_stitches_batches() -> Result<()> {
    let batch = make_batch(DEFAULT_CHUNK_ROWS);
    let (dir, path) = temp_paths();
    fs::create_dir_all(&dir)?;

    write_chunk(&path, &batch)?;
    let mut scan = SeqScan::open(path, 1000, 2000, 128, Cols::all())?;

    let mut ts = Vec::new();
    let mut series_id = Vec::new();
    let mut value = Vec::new();

    while let Some(batch) = scan.next_batch()? {
        ts.extend_from_slice(&batch.ts);
        series_id.extend_from_slice(&batch.series_id);
        value.extend_from_slice(&batch.value);
    }

    assert_eq!(ts.len(), 1000);
    assert_eq!(series_id.len(), 1000);
    assert_eq!(value.len(), 1000);
    assert_eq!(ts.first().copied().unwrap(), 1000);
    assert_eq!(ts.last().copied().unwrap(), 1999);

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

fn make_batch(len: usize) -> RecordBatch {
    let mut ts = Vec::with_capacity(len);
    let mut series_id = Vec::with_capacity(len);
    let mut value = Vec::with_capacity(len);

    for i in 0..len {
        ts.push(i as i64);
        series_id.push((i as u32) % 1000);
        value.push((i as f64).sin());
    }

    RecordBatch {
        ts,
        series_id,
        value,
    }
}

fn temp_paths() -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "tsdb_exec_scan_stream_{}_{}",
        std::process::id(),
        0x5EEDu64
    ));
    let path = dir.join("chunk.bin");
    (dir, path)
}
