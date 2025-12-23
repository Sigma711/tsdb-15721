use std::fs;
use std::path::PathBuf;

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::Result;
use datamodel::batch::RecordBatch;
use storage::reader::open_chunk;
use storage::writer::write_chunk;

#[test]
fn read_range_columns() -> Result<()> {
    let batch = make_batch(DEFAULT_CHUNK_ROWS);
    let (dir, path) = temp_paths();
    fs::create_dir_all(&dir)?;

    write_chunk(&path, &batch)?;
    let mut chunk = open_chunk(&path)?;

    let lo = 1000usize;
    let hi = 2000usize;
    let ts = chunk.read_range_i64(0, lo, hi)?;
    let series_id = chunk.read_range_u32(1, lo, hi)?;
    let value = chunk.read_range_f64(2, lo, hi)?;

    assert_eq!(ts.len(), hi - lo);
    assert_eq!(series_id.len(), hi - lo);
    assert_eq!(value.len(), hi - lo);

    assert_eq!(ts.first().copied().unwrap(), 1000);
    assert_eq!(ts.last().copied().unwrap(), 1999);
    assert_eq!(series_id.first().copied().unwrap(), 0);
    assert_eq!(series_id.last().copied().unwrap(), 999);

    let first_val = value.first().copied().unwrap();
    let last_val = value.last().copied().unwrap();
    assert!((first_val - (1000f64).sin()).abs() < 1e-12);
    assert!((last_val - (1999f64).sin()).abs() < 1e-12);

    let _ = fs::remove_file(&path);
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
        "tsdb_storage_range_read_{}_{}",
        std::process::id(),
        0xC0FFEEu64
    ));
    let path = dir.join("chunk.bin");
    (dir, path)
}
