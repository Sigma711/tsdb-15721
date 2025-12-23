use std::cell::RefCell;
use std::rc::Rc;

use common::Result;
use datamodel::batch::RecordBatch;

use super::{OpStats, Operator};

pub struct ProjectOp {
    child: Box<dyn Operator>,
    keep_ts: bool,
    keep_series: bool,
    keep_value: bool,
    stats: Rc<RefCell<OpStats>>,
}

impl ProjectOp {
    pub fn new(
        child: Box<dyn Operator>,
        keep_ts: bool,
        keep_series: bool,
        keep_value: bool,
    ) -> Self {
        Self {
            child,
            keep_ts,
            keep_series,
            keep_value,
            stats: Rc::new(RefCell::new(OpStats::default())),
        }
    }

    pub fn stats_handle(&self) -> Rc<RefCell<OpStats>> {
        self.stats.clone()
    }
}

impl Operator for ProjectOp {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = match self.child.next_batch()? {
            Some(batch) => batch,
            None => return Ok(None),
        };
        let input_rows = batch.len();

        let ts = if self.keep_ts { batch.ts } else { Vec::new() };
        let series_id = if self.keep_series {
            batch.series_id
        } else {
            Vec::new()
        };
        let value = if self.keep_value {
            batch.value
        } else {
            Vec::new()
        };

        let output_len = if self.keep_ts {
            ts.len()
        } else if self.keep_series {
            series_id.len()
        } else if self.keep_value {
            value.len()
        } else {
            0
        };

        let projected = RecordBatch {
            ts,
            series_id,
            value,
        };
        let mut stats = self.stats.borrow_mut();
        stats.input_rows += input_rows;
        stats.output_rows += output_len;
        stats.num_batches += 1;

        Ok(Some(projected))
    }

    fn explain(&self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        let cols = describe_cols(self.keep_ts, self.keep_series, self.keep_value);
        let mut out = format!("{pad}Project(cols={})", cols);
        let child = self.child.explain(indent + 2);
        out.push('\n');
        out.push_str(&child);
        out
    }
}

fn describe_cols(keep_ts: bool, keep_series: bool, keep_value: bool) -> String {
    let mut parts = Vec::new();
    if keep_ts {
        parts.push("ts");
    }
    if keep_series {
        parts.push("series_id");
    }
    if keep_value {
        parts.push("value");
    }
    if parts.is_empty() {
        return "none".to_string();
    }
    parts.join(",")
}
