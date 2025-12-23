use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use common::{Error, Result};
use datamodel::batch::RecordBatch;
use storage::reader::{open_chunk, open_meta, ChunkFile};

use crate::expr::Pred;

use super::{OpStats, Operator};

#[derive(Debug, Clone, Copy)]
pub struct Cols {
    pub ts: bool,
    pub series_id: bool,
    pub value: bool,
}

impl Cols {
    pub fn all() -> Self {
        Self {
            ts: true,
            series_id: true,
            value: true,
        }
    }

    pub fn ts_value() -> Self {
        Self {
            ts: true,
            series_id: false,
            value: true,
        }
    }

    fn describe(&self) -> String {
        let mut parts = Vec::new();
        if self.ts {
            parts.push("ts");
        }
        if self.series_id {
            parts.push("series_id");
        }
        if self.value {
            parts.push("value");
        }
        if parts.is_empty() {
            return "none".to_string();
        }
        parts.join(",")
    }
}

pub struct SeqScan {
    file: ChunkFile,
    t0: i64,
    t1: i64,
    lo: usize,
    hi: usize,
    cur: usize,
    batch_rows: usize,
    skipped: bool,
    bytes_read: u64,
    pred: Option<Pred>,
    cols: Cols,
    stats: Rc<RefCell<OpStats>>,
}

impl SeqScan {
    pub fn open(path: PathBuf, t0: i64, t1: i64, batch_rows: usize, cols: Cols) -> Result<Self> {
        if batch_rows == 0 {
            return Err(Error::Unsupported("batch_rows must be > 0".into()));
        }

        let meta = open_meta(&path)?;
        let mut file = open_chunk(&path)?;
        let mut skipped = false;
        let (lo, hi) = if t1 <= meta.ts_min || t0 > meta.ts_max {
            skipped = true;
            (0, 0)
        } else {
            let lo = lower_bound_in_file(&mut file, t0)?;
            let hi = lower_bound_in_file(&mut file, t1)?;
            (lo, hi)
        };

        Ok(Self {
            file,
            t0,
            t1,
            lo,
            hi,
            cur: lo,
            batch_rows,
            skipped,
            bytes_read: 0,
            pred: None,
            cols,
            stats: Rc::new(RefCell::new(OpStats::default())),
        })
    }

    pub fn with_predicate(mut self, pred: Option<Pred>) -> Self {
        self.pred = pred;
        self
    }

    pub fn skipped(&self) -> bool {
        self.skipped
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub fn range(&self) -> (usize, usize) {
        (self.lo, self.hi)
    }

    pub fn time_range(&self) -> (i64, i64) {
        (self.t0, self.t1)
    }

    pub fn stats_handle(&self) -> Rc<RefCell<OpStats>> {
        self.stats.clone()
    }
}

impl Operator for SeqScan {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.skipped || self.cur >= self.hi {
            return Ok(None);
        }

        let end = (self.cur + self.batch_rows).min(self.hi);
        let mut bytes = 0u64;
        let ts = if self.cols.ts {
            bytes += (end - self.cur) as u64 * 8;
            self.file.read_range_i64(0, self.cur, end)?
        } else {
            Vec::new()
        };
        let series_id = if self.cols.series_id {
            bytes += (end - self.cur) as u64 * 4;
            self.file.read_range_u32(1, self.cur, end)?
        } else {
            Vec::new()
        };
        let value = if self.cols.value {
            bytes += (end - self.cur) as u64 * 8;
            self.file.read_range_f64(2, self.cur, end)?
        } else {
            Vec::new()
        };
        self.bytes_read = self.bytes_read.saturating_add(bytes);
        self.cur = end;

        let batch = RecordBatch {
            ts,
            series_id,
            value,
        };
        let mut stats = self.stats.borrow_mut();
        stats.output_rows += batch.len();
        stats.num_batches += 1;
        stats.bytes_read = stats.bytes_read.saturating_add(bytes);

        Ok(Some(batch))
    }

    fn explain(&self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        format!(
            "{pad}SeqScan(range=[{}, {}), slice_range=[{}, {}), cols={}, batch_rows={})",
            self.t0,
            self.t1,
            self.lo,
            self.hi,
            self.cols.describe(),
            self.batch_rows
        )
    }
}

fn lower_bound_in_file(chunk: &mut ChunkFile, target: i64) -> Result<usize> {
    let mut left = 0;
    let mut right = chunk.meta.row_count as usize;
    while left < right {
        let mid = left + (right - left) / 2;
        let ts_mid = chunk.read_ts_at(mid)?;
        if ts_mid < target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    Ok(left)
}
