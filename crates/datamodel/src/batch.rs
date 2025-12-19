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
}
