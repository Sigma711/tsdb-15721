#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub ts: Vec<i64>,
    pub series_id: Vec<u32>,
    pub value: Vec<f64>,
}

impl RecordBatch {
    pub fn len(&self) -> usize {
        self.ts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> RecordBatch {
        RecordBatch {
            ts: self.ts[range.clone()].to_vec(),
            series_id: self.series_id[range.clone()].to_vec(),
            value: self.value[range].to_vec(),
        }
    }
}
