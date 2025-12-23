pub mod filter;
pub mod project;
pub mod scan;

#[derive(Debug, Default, Clone)]
pub struct OpStats {
    pub input_rows: usize,
    pub output_rows: usize,
    pub num_batches: usize,
    pub bytes_read: u64,
}

pub trait Operator {
    fn next_batch(&mut self) -> common::Result<Option<datamodel::batch::RecordBatch>>;
    fn explain(&self, indent: usize) -> String;
}
