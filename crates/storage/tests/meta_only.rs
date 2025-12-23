use std::fs::{self, OpenOptions};
use std::path::PathBuf;

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::Result;
use datamodel::batch::RecordBatch;
use storage::format::HEADER_LEN;
use storage::reader::open_meta;
use storage::writer::write_chunk;

#[test]
fn read_meta_without_data() -> Result<()> {
    let batch = make_batch(DEFAULT_CHUNK_ROWS);
    let (dir, path) = temp_paths();
    fs::create_dir_all(&dir)?;

    write_chunk(&path, &batch)?;
    truncate_to_meta_only(&path)?;

    let meta = open_meta(&path)?;
    assert_eq!(meta.row_count, batch.len() as u32);

    let ts_min = *batch.ts.iter().min().unwrap();
    let ts_max = *batch.ts.iter().max().unwrap();
    assert_eq!(meta.ts_min, ts_min);
    assert_eq!(meta.ts_max, ts_max);

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

fn truncate_to_meta_only(path: &PathBuf) -> Result<()> {
    let truncate_len = HEADER_LEN as u64 + meta_len_for_cols(3) as u64;
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(truncate_len)?;
    Ok(())
}

fn meta_len_for_cols(col_count: usize) -> usize {
    let base_len = 4 + 8 + 8 + 4;
    let col_len = 2 + 2 + 8 + 8;
    base_len + col_count * col_len
}

fn temp_paths() -> (PathBuf, PathBuf) {
    let mut rng = Lcg::new(0xF1EE_5EED_D00D_BEEF);
    let suffix = rng.next_u64();
    let dir = std::env::temp_dir().join(format!(
        "tsdb_storage_meta_only_{}_{}",
        std::process::id(),
        suffix
    ));
    let path = dir.join("chunk.bin");
    (dir, path)
}

struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }
}
