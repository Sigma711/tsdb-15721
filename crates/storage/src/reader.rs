use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use common::error::{Error, Result};
use crc32fast::Hasher;
use datamodel::batch::RecordBatch;

use crate::format;
use crate::meta::{ChunkMeta, ColumnMeta};

pub struct ChunkFile {
    pub meta: ChunkMeta,
    file: File,
}

impl ChunkFile {
    pub fn read_ts_at(&mut self, idx: usize) -> Result<i64> {
        if idx >= self.meta.row_count as usize {
            return Err(Error::Corrupt("ts index out of bounds".into()));
        }
        let ts_col = self
            .meta
            .cols
            .iter()
            .find(|col| col.col_id == 0)
            .ok_or_else(|| Error::Corrupt("missing ts column".into()))?;
        if ts_col.encoding != 0 {
            return Err(Error::Unsupported("unsupported encoding".into()));
        }

        let pos = ts_col
            .offset
            .checked_add((idx as u64) * 8)
            .ok_or_else(|| Error::Corrupt("ts offset overflow".into()))?;
        self.file.seek(SeekFrom::Start(pos))?;
        let mut buf = [0u8; 8];
        self.file.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    pub fn read_range_i64(&mut self, col_id: u16, start: usize, end: usize) -> Result<Vec<i64>> {
        let col = self.find_col(col_id)?.clone();
        if col.encoding != 0 {
            return Err(Error::Unsupported("unsupported encoding".into()));
        }
        let buf = self.read_range_bytes(&col, start, end, 8)?;
        let mut out = Vec::with_capacity(buf.len() / 8);
        for chunk in buf.chunks_exact(8) {
            out.push(i64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Ok(out)
    }

    pub fn read_range_u32(&mut self, col_id: u16, start: usize, end: usize) -> Result<Vec<u32>> {
        let col = self.find_col(col_id)?.clone();
        if col.encoding != 0 {
            return Err(Error::Unsupported("unsupported encoding".into()));
        }
        let buf = self.read_range_bytes(&col, start, end, 4)?;
        let mut out = Vec::with_capacity(buf.len() / 4);
        for chunk in buf.chunks_exact(4) {
            out.push(u32::from_le_bytes(chunk.try_into().unwrap()));
        }
        Ok(out)
    }

    pub fn read_range_f64(&mut self, col_id: u16, start: usize, end: usize) -> Result<Vec<f64>> {
        let col = self.find_col(col_id)?.clone();
        if col.encoding != 0 {
            return Err(Error::Unsupported("unsupported encoding".into()));
        }
        let buf = self.read_range_bytes(&col, start, end, 8)?;
        let mut out = Vec::with_capacity(buf.len() / 8);
        for chunk in buf.chunks_exact(8) {
            out.push(f64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Ok(out)
    }

    fn find_col(&self, col_id: u16) -> Result<&ColumnMeta> {
        self.meta
            .cols
            .iter()
            .find(|col| col.col_id == col_id)
            .ok_or_else(|| Error::Corrupt("missing column".into()))
    }

    fn read_range_bytes(
        &mut self,
        col: &ColumnMeta,
        start: usize,
        end: usize,
        width: usize,
    ) -> Result<Vec<u8>> {
        if start > end {
            return Err(Error::Corrupt("range start > end".into()));
        }
        let row_count = self.meta.row_count as usize;
        if end > row_count {
            return Err(Error::Corrupt("range end out of bounds".into()));
        }
        let end_bytes = end
            .checked_mul(width)
            .ok_or_else(|| Error::Corrupt("range byte overflow".into()))?;
        if end_bytes as u64 > col.len {
            return Err(Error::Corrupt("range exceeds column length".into()));
        }
        let start_bytes = start
            .checked_mul(width)
            .ok_or_else(|| Error::Corrupt("range byte overflow".into()))?;
        let byte_len = end_bytes
            .checked_sub(start_bytes)
            .ok_or_else(|| Error::Corrupt("range byte underflow".into()))?;

        let offset = col
            .offset
            .checked_add(start_bytes as u64)
            .ok_or_else(|| Error::Corrupt("range offset overflow".into()))?;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; byte_len];
        if !buf.is_empty() {
            self.file.read_exact(&mut buf)?;
        }
        Ok(buf)
    }
}

pub fn open_chunk(path: &Path) -> Result<ChunkFile> {
    let mut file = File::open(path)?;
    let header = format::read_header(&mut file)?;
    let meta = read_meta(&mut file, &header)?;
    Ok(ChunkFile { meta, file })
}

pub fn open_meta(path: &Path) -> Result<ChunkMeta> {
    let mut file = File::open(path)?;
    let header = format::read_header(&mut file)?;
    read_meta(&mut file, &header)
}

pub fn read_batch(chunk: &mut ChunkFile) -> Result<RecordBatch> {
    let row_count = chunk.meta.row_count as usize;
    let min_data_offset = format::HEADER_LEN as u64 + meta_total_len(&chunk.meta) as u64;
    let mut ts: Option<Vec<i64>> = None;
    let mut series_id: Option<Vec<u32>> = None;
    let mut value: Option<Vec<f64>> = None;

    for col in &chunk.meta.cols {
        if col.offset < min_data_offset {
            return Err(Error::Corrupt("column offset before data section".into()));
        }
        if col.encoding != 0 {
            return Err(Error::Unsupported("unsupported encoding".into()));
        }
        match col.col_id {
            0 => {
                ts = Some(read_i64_col(&mut chunk.file, col, row_count)?);
            }
            1 => {
                series_id = Some(read_u32_col(&mut chunk.file, col, row_count)?);
            }
            2 => {
                value = Some(read_f64_col(&mut chunk.file, col, row_count)?);
            }
            _ => {}
        }
    }

    let ts = ts.ok_or_else(|| Error::Corrupt("missing ts column".into()))?;
    let series_id = series_id.ok_or_else(|| Error::Corrupt("missing series_id column".into()))?;
    let value = value.ok_or_else(|| Error::Corrupt("missing value column".into()))?;

    Ok(RecordBatch {
        ts,
        series_id,
        value,
    })
}

fn read_meta(file: &mut File, header: &format::Header) -> Result<ChunkMeta> {
    if header.meta_len == 0 {
        return Err(Error::Corrupt("meta_len is zero".into()));
    }

    let meta_len =
        usize::try_from(header.meta_len).map_err(|_| Error::Corrupt("meta_len overflow".into()))?;
    let mut meta_buf = vec![0u8; meta_len];
    file.read_exact(&mut meta_buf)?;

    let mut hasher = Hasher::new();
    hasher.update(&meta_buf);
    let expected_crc = hasher.finalize();
    if expected_crc != header.meta_crc32 {
        return Err(Error::Corrupt("meta crc mismatch".into()));
    }

    crate::meta::decode_meta(&meta_buf)
}

fn meta_total_len(meta: &ChunkMeta) -> usize {
    let base_len = 4 + 8 + 8 + 4;
    let col_len = 2 + 2 + 8 + 8;
    base_len + meta.cols.len() * col_len
}

fn read_i64_col(file: &mut File, col: &ColumnMeta, row_count: usize) -> Result<Vec<i64>> {
    let buf = read_col_bytes(file, col)?;
    if buf.len() % 8 != 0 {
        return Err(Error::Corrupt("i64 column length mismatch".into()));
    }
    let count = buf.len() / 8;
    if count != row_count {
        return Err(Error::Corrupt("row_count mismatch".into()));
    }
    let mut out = Vec::with_capacity(count);
    for chunk in buf.chunks_exact(8) {
        let val = i64::from_le_bytes(chunk.try_into().unwrap());
        out.push(val);
    }
    Ok(out)
}

fn read_u32_col(file: &mut File, col: &ColumnMeta, row_count: usize) -> Result<Vec<u32>> {
    let buf = read_col_bytes(file, col)?;
    if buf.len() % 4 != 0 {
        return Err(Error::Corrupt("u32 column length mismatch".into()));
    }
    let count = buf.len() / 4;
    if count != row_count {
        return Err(Error::Corrupt("row_count mismatch".into()));
    }
    let mut out = Vec::with_capacity(count);
    for chunk in buf.chunks_exact(4) {
        let val = u32::from_le_bytes(chunk.try_into().unwrap());
        out.push(val);
    }
    Ok(out)
}

fn read_f64_col(file: &mut File, col: &ColumnMeta, row_count: usize) -> Result<Vec<f64>> {
    let buf = read_col_bytes(file, col)?;
    if buf.len() % 8 != 0 {
        return Err(Error::Corrupt("f64 column length mismatch".into()));
    }
    let count = buf.len() / 8;
    if count != row_count {
        return Err(Error::Corrupt("row_count mismatch".into()));
    }
    let mut out = Vec::with_capacity(count);
    for chunk in buf.chunks_exact(8) {
        let val = f64::from_le_bytes(chunk.try_into().unwrap());
        out.push(val);
    }
    Ok(out)
}

fn read_col_bytes(file: &mut File, col: &ColumnMeta) -> Result<Vec<u8>> {
    let len = usize::try_from(col.len).map_err(|_| Error::Corrupt("column len overflow".into()))?;
    file.seek(SeekFrom::Start(col.offset))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}
