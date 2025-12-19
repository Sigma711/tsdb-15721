use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::{Error, Result};
use datamodel::batch::RecordBatch;
use storage::reader::{open_chunk, read_batch};
use storage::writer::write_chunk;

fn main() -> Result<()> {
    let batch = make_batch(DEFAULT_CHUNK_ROWS);
    let path = chunk_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let write_start = Instant::now();
    write_chunk(&path, &batch)?;
    let write_time = write_start.elapsed();

    let read_start = Instant::now();
    let mut chunk = open_chunk(&path)?;
    let read = read_batch(&mut chunk)?;
    let read_time = read_start.elapsed();

    if read.len() != batch.len()
        || read.series_id.len() != batch.series_id.len()
        || read.value.len() != batch.value.len()
    {
        return Err(Error::Corrupt("read batch size mismatch".into()));
    }

    let file_size = fs::metadata(&path)?.len();
    let write_rps = rows_per_sec(batch.len(), write_time);
    let read_rps = rows_per_sec(batch.len(), read_time);

    println!("write_time: {:?}", write_time);
    println!("read_time: {:?}", read_time);
    println!("read_rows: {}", read.len());
    println!("file_size_bytes: {}", file_size);
    println!("write_rows_per_sec: {:.2}", write_rps);
    println!("read_rows_per_sec: {:.2}", read_rps);

    Ok(())
}

fn make_batch(len: usize) -> RecordBatch {
    let mut ts = Vec::with_capacity(len);
    let mut series_id = Vec::with_capacity(len);
    let mut value = Vec::with_capacity(len);

    for i in 0..len {
        let ts_val = i as i64;
        ts.push(ts_val);
        series_id.push((i as u32) % 1000);
        value.push((i as f64).sin());
    }

    RecordBatch {
        ts,
        series_id,
        value,
    }
}

fn chunk_path() -> PathBuf {
    PathBuf::from("data/chunks/lab01_0001.tschunk")
}

fn rows_per_sec(rows: usize, duration: Duration) -> f64 {
    let secs = duration.as_secs_f64();
    if secs == 0.0 {
        return f64::INFINITY;
    }
    rows as f64 / secs
}
