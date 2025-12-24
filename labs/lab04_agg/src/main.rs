use std::fs;
use std::path::PathBuf;

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::{Error, Result};
use datamodel::batch::RecordBatch;
use exec::agg::AggResult;
use exec::operators::agg_downsample::AggDownsampleOp;
use exec::operators::scan::{Cols, SeqScan};
use storage::writer::write_chunk;

fn main() -> Result<()> {
    let batch = make_batch(DEFAULT_CHUNK_ROWS);
    let path = chunk_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    write_chunk(&path, &batch)?;

    let scan = SeqScan::open(path, 0, 16_384, 1024, Cols::ts_value())?;
    let mut agg = AggDownsampleOp::new(Box::new(scan), 100)?;
    let result = agg.execute_all()?;

    print_top_rows(&result, 5);
    let total_count = validate_result(&result)?;
    println!("total_count: {}", total_count);
    println!("num_windows: {}", result.rows.len());

    Ok(())
}

fn print_top_rows(result: &AggResult, n: usize) {
    for row in result.rows.iter().take(n) {
        let avg = row.sum / row.count as f64;
        println!(
            "window_start={} count={} min={:.6} max={:.6} avg={:.6}",
            row.window_start, row.count, row.min, row.max, avg
        );
    }
}

fn validate_result(result: &AggResult) -> Result<u32> {
    let total_count: u32 = result.rows.iter().map(|r| r.count).sum();
    if total_count != 16_384 {
        return Err(Error::Corrupt(format!(
            "total_count mismatch: {}",
            total_count
        )));
    }

    for i in 1..result.rows.len() {
        let prev = result.rows[i - 1].window_start;
        let cur = result.rows[i].window_start;
        if cur <= prev {
            return Err(Error::Corrupt("window_start not increasing".into()));
        }
        if cur - prev != 100 {
            return Err(Error::Corrupt("window_start step mismatch".into()));
        }
    }
    Ok(total_count)
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

fn chunk_path() -> PathBuf {
    PathBuf::from("data/chunks/lab04_0001.tschunk")
}
