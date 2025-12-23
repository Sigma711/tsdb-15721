use std::cell::RefCell;
use std::rc::Rc;

use common::Result;
use datamodel::batch::RecordBatch;

use crate::expr::Pred;

use super::{OpStats, Operator};

pub struct FilterOp {
    child: Box<dyn Operator>,
    pred: Pred,
    stats: Rc<RefCell<OpStats>>,
}

impl FilterOp {
    pub fn new(child: Box<dyn Operator>, pred: Pred) -> Self {
        Self {
            child,
            pred,
            stats: Rc::new(RefCell::new(OpStats::default())),
        }
    }

    pub fn stats_handle(&self) -> Rc<RefCell<OpStats>> {
        self.stats.clone()
    }
}

impl Operator for FilterOp {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = match self.child.next_batch()? {
            Some(batch) => batch,
            None => return Ok(None),
        };

        let mask = self.pred.eval_batch(&batch);
        let has_ts = batch.ts.len() == batch.len();
        let has_series = batch.series_id.len() == batch.len();
        let has_value = batch.value.len() == batch.len();
        let mut ts = Vec::new();
        let mut series_id = Vec::new();
        let mut value = Vec::new();

        for (i, keep) in mask.iter().enumerate() {
            if *keep {
                if has_ts {
                    ts.push(batch.ts[i]);
                }
                if has_series {
                    series_id.push(batch.series_id[i]);
                }
                if has_value {
                    value.push(batch.value[i]);
                }
            }
        }

        let filtered = RecordBatch {
            ts,
            series_id,
            value,
        };
        let mut stats = self.stats.borrow_mut();
        stats.input_rows += batch.len();
        stats.output_rows += filtered.len();
        stats.num_batches += 1;

        Ok(Some(filtered))
    }

    fn explain(&self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        let mut out = format!("{pad}Filter(pred={})", self.pred);
        let child = self.child.explain(indent + 2);
        out.push('\n');
        out.push_str(&child);
        out
    }
}
