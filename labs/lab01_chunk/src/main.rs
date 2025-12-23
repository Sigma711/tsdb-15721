use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::config::DEFAULT_CHUNK_ROWS;
use common::error::{Error, Result};
use datamodel::batch::RecordBatch;
use exec::expr::{Col, Pred};
use exec::operators::filter::FilterOp;
use exec::operators::project::ProjectOp;
use exec::operators::scan::{Cols, SeqScan};
use exec::operators::Operator;
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

    let file_size = fs::metadata(&path)?.len();
    let write_rps = rows_per_sec(batch.len(), write_time);

    println!("write_time: {:?}", write_time);
    println!("file_size_bytes: {}", file_size);
    println!("write_rows_per_sec: {:.2}", write_rps);

    let queries = [(0_i64, 16_384_i64), (1000_i64, 2000_i64), (1_000_000_i64, 1_000_100_i64)];
    for (t0, t1) in queries {
        let mut scan = SeqScan::open(path.clone(), t0, t1, 1024, Cols::all())?;
        let start = Instant::now();
        let mut total_rows = 0usize;
        let mut num_batches = 0usize;
        let mut first_ts: Option<i64> = None;
        let mut last_ts: Option<i64> = None;
        while let Some(batch) = scan.next_batch()? {
            if batch.series_id.len() != batch.len() || batch.value.len() != batch.len() {
                return Err(Error::Corrupt("filtered batch size mismatch".into()));
            }
            num_batches += 1;
            if !batch.is_empty() {
                if first_ts.is_none() {
                    first_ts = batch.ts.first().copied();
                }
                last_ts = batch.ts.last().copied();
                total_rows += batch.len();
            }
        }
        let time_ms = start.elapsed().as_secs_f64() * 1000.0;

        let (lo, hi) = scan.range();
        println!("query_range: [{}, {})", t0, t1);
        println!("total_rows: {}", total_rows);
        println!("skipped_chunk: {}", scan.skipped());
        println!("bytes_read: {}", scan.bytes_read());
        println!("num_batches: {}", num_batches);
        println!("time_ms: {:.3}", time_ms);

        if t0 == 1000 && t1 == 2000 {
            println!("slice_range: [{}, {})", lo, hi);
            if total_rows == 0 {
                return Err(Error::Corrupt("Q1 returned empty batch".into()));
            }
            let first_ts = first_ts.unwrap();
            let last_ts = last_ts.unwrap();
            println!("first_ts: {}", first_ts);
            println!("last_ts: {}", last_ts);
            assert_eq!(first_ts, 1000);
            assert_eq!(last_ts, 1999);
        }
    }

    let pred = Pred::GtF64(Col::Value, 0.5);
    let scan =
        SeqScan::open(path.clone(), 0, 16_384, 1024, Cols::ts_value())?.with_predicate(Some(
            pred.clone(),
        ));
    let scan_stats = scan.stats_handle();
    let filter = FilterOp::new(Box::new(scan), pred);
    let filter_stats = filter.stats_handle();
    let mut project = ProjectOp::new(Box::new(filter), true, false, true);
    let project_stats = project.stats_handle();

    println!("{}", project.explain(0));

    let start = Instant::now();
    let mut total_rows = 0usize;
    while let Some(batch) = project.next_batch()? {
        total_rows += batch.len();
    }
    let time_ms = start.elapsed().as_secs_f64() * 1000.0;
    let scan_stats = scan_stats.borrow().clone();
    let filter_stats = filter_stats.borrow().clone();
    let project_stats = project_stats.borrow().clone();

    println!("pipeline_query: [0, 16384) value > 0.5 project(ts,value)");
    println!("before_rows: {}", scan_stats.output_rows);
    println!("after_filter_rows: {}", filter_stats.output_rows);
    println!("after_project_rows: {}", project_stats.output_rows);
    println!("bytes_read: {}", scan_stats.bytes_read);
    println!("num_batches_scan: {}", scan_stats.num_batches);
    println!("num_batches_filter: {}", filter_stats.num_batches);
    println!("num_batches_project: {}", project_stats.num_batches);
    println!("total_rows: {}", total_rows);
    println!("time_ms: {:.3}", time_ms);

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
