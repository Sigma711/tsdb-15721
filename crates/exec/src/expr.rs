use datamodel::batch::RecordBatch;
use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum Col {
    Ts,
    SeriesId,
    Value,
}

#[derive(Debug, Clone)]
pub enum Pred {
    GtF64(Col, f64),
    LtI64(Col, i64),
    And(Box<Pred>, Box<Pred>),
}

impl Pred {
    pub fn eval_batch(&self, batch: &RecordBatch) -> Vec<bool> {
        match self {
            Pred::GtF64(col, threshold) => eval_gt_f64(*col, *threshold, batch),
            Pred::LtI64(col, threshold) => eval_lt_i64(*col, *threshold, batch),
            Pred::And(left, right) => {
                let left_mask = left.eval_batch(batch);
                let right_mask = right.eval_batch(batch);
                left_mask
                    .into_iter()
                    .zip(right_mask)
                    .map(|(l, r)| l && r)
                    .collect()
            }
        }
    }
}

impl fmt::Display for Col {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Col::Ts => write!(f, "ts"),
            Col::SeriesId => write!(f, "series_id"),
            Col::Value => write!(f, "value"),
        }
    }
}

impl fmt::Display for Pred {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pred::GtF64(col, value) => write!(f, "{} > {}", col, value),
            Pred::LtI64(col, value) => write!(f, "{} < {}", col, value),
            Pred::And(left, right) => write!(f, "({}) AND ({})", left, right),
        }
    }
}

fn eval_gt_f64(col: Col, threshold: f64, batch: &RecordBatch) -> Vec<bool> {
    let mut mask = Vec::with_capacity(batch.len());
    for i in 0..batch.len() {
        let value = match col {
            Col::Ts => batch.ts[i] as f64,
            Col::SeriesId => batch.series_id[i] as f64,
            Col::Value => batch.value[i],
        };
        mask.push(value > threshold);
    }
    mask
}

fn eval_lt_i64(col: Col, threshold: i64, batch: &RecordBatch) -> Vec<bool> {
    let mut mask = Vec::with_capacity(batch.len());
    for i in 0..batch.len() {
        let value = match col {
            Col::Ts => batch.ts[i],
            Col::SeriesId => batch.series_id[i] as i64,
            Col::Value => batch.value[i] as i64,
        };
        mask.push(value < threshold);
    }
    mask
}
