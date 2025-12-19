use std::fs;
use std::path::PathBuf;

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::Result;
use datamodel::batch::RecordBatch;
use storage::reader::{open_chunk, read_batch};
use storage::writer::write_chunk;

#[test]
fn roundtrip_chunk() -> Result<()> {
    let batch = make_batch(DEFAULT_CHUNK_ROWS);

    let (dir, path) = temp_paths();
    fs::create_dir_all(&dir)?;

    write_chunk(&path, &batch)?;
    let mut chunk = open_chunk(&path)?;
    let read = read_batch(&mut chunk)?;

    assert_eq!(read.len(), batch.len());
    assert_eq!(read.ts, batch.ts);
    assert_eq!(read.series_id, batch.series_id);
    assert_eq!(read.value, batch.value);

    let ts_min = *batch.ts.iter().min().unwrap();
    let ts_max = *batch.ts.iter().max().unwrap();
    assert_eq!(chunk.meta.ts_min, ts_min);
    assert_eq!(chunk.meta.ts_max, ts_max);

    let _ = fs::remove_file(&path);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

fn make_batch(len: usize) -> RecordBatch {
    let mut rng = Lcg::new(0x9E37_79B9_7F4A_7C15);
    let mut ts = Vec::with_capacity(len);
    let mut series_id = Vec::with_capacity(len);
    let mut value = Vec::with_capacity(len);

    for _ in 0..len {
        ts.push(rng.next_i64());
        series_id.push(rng.next_u32());
        value.push(rng.next_f64());
    }

    RecordBatch {
        ts,
        series_id,
        value,
    }
}

fn temp_paths() -> (PathBuf, PathBuf) {
    let mut rng = Lcg::new(0xD1B5_4A32_D192_ED03);
    let suffix = rng.next_u64();
    let dir = std::env::temp_dir().join(format!(
        "tsdb_storage_roundtrip_{}_{}",
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

    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }

    fn next_f64(&mut self) -> f64 {
        let bits = self.next_u64() >> 11;
        let unit = bits as f64 / ((1u64 << 53) as f64);
        unit * 10_000.0 - 5_000.0
    }
}
