use common::{Error, Result};
use datamodel::batch::RecordBatch;

use crate::agg::{AggResult, AggRow};
use crate::operators::Operator;

pub struct AggDownsampleOp {
    child: Box<dyn Operator>,
    window: i64,
    current_window_start: Option<i64>,
    current_acc: Option<AggRow>,
    output: Vec<AggRow>,
}

impl AggDownsampleOp {
    pub fn new(child: Box<dyn Operator>, window: i64) -> Result<Self> {
        if window <= 0 {
            return Err(Error::Unsupported("window must be > 0".into()));
        }
        Ok(Self {
            child,
            window,
            current_window_start: None,
            current_acc: None,
            output: Vec::new(),
        })
    }

    pub fn execute_all(&mut self) -> Result<AggResult> {
        self.reset_state();
        while let Some(batch) = self.child.next_batch()? {
            self.consume_batch(&batch)?;
        }
        self.flush_current()?;
        let rows = std::mem::take(&mut self.output);
        Ok(AggResult { rows })
    }

    fn consume_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.ts.len() != batch.value.len() {
            return Err(Error::Corrupt("ts/value length mismatch".into()));
        }

        for (ts, value) in batch.ts.iter().zip(batch.value.iter()) {
            let window_start = (*ts / self.window) * self.window;
            match self.current_window_start {
                None => {
                    self.current_window_start = Some(window_start);
                    self.current_acc = Some(new_acc(window_start, *value));
                }
                Some(current_start) => {
                    if window_start == current_start {
                        if let Some(acc) = self.current_acc.as_mut() {
                            add_value(acc, *value)?;
                        }
                    } else {
                        self.flush_current()?;
                        self.current_window_start = Some(window_start);
                        self.current_acc = Some(new_acc(window_start, *value));
                    }
                }
            }
        }
        Ok(())
    }

    fn flush_current(&mut self) -> Result<()> {
        if let Some(acc) = self.current_acc.take() {
            self.output.push(acc);
            self.current_window_start = None;
        }
        Ok(())
    }

    fn reset_state(&mut self) {
        self.current_window_start = None;
        self.current_acc = None;
        self.output.clear();
    }
}

fn new_acc(window_start: i64, value: f64) -> AggRow {
    AggRow {
        window_start,
        count: 1,
        sum: value,
        min: value,
        max: value,
    }
}

fn add_value(acc: &mut AggRow, value: f64) -> Result<()> {
    acc.count = acc
        .count
        .checked_add(1)
        .ok_or_else(|| Error::Unsupported("count overflow".into()))?;
    acc.sum += value;
    if value < acc.min {
        acc.min = value;
    }
    if value > acc.max {
        acc.max = value;
    }
    Ok(())
}
