#[derive(Debug, Clone)]
pub struct AggRow {
    pub window_start: i64,
    pub count: u32,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Default)]
pub struct AggResult {
    pub rows: Vec<AggRow>,
}
