use common::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub col_id: u16,
    pub encoding: u16,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub row_count: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    pub cols: Vec<ColumnMeta>,
}

pub fn encode_meta(meta: &ChunkMeta) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&meta.row_count.to_le_bytes());
    buf.extend_from_slice(&meta.ts_min.to_le_bytes());
    buf.extend_from_slice(&meta.ts_max.to_le_bytes());
    let col_count = meta.cols.len() as u32;
    buf.extend_from_slice(&col_count.to_le_bytes());
    for col in &meta.cols {
        buf.extend_from_slice(&col.col_id.to_le_bytes());
        buf.extend_from_slice(&col.encoding.to_le_bytes());
        buf.extend_from_slice(&col.offset.to_le_bytes());
        buf.extend_from_slice(&col.len.to_le_bytes());
    }
    buf
}

pub fn decode_meta(buf: &[u8]) -> Result<ChunkMeta> {
    const BASE_LEN: usize = 4 + 8 + 8 + 4;
    const COL_LEN: usize = 2 + 2 + 8 + 8;

    if buf.len() < BASE_LEN {
        return Err(Error::Corrupt("meta too short".into()));
    }

    let mut offset = 0;
    let row_count = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let ts_min = i64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let ts_max = i64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let col_count = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let col_count_usize =
        usize::try_from(col_count).map_err(|_| Error::Corrupt("col_count overflow".into()))?;
    let cols_len = col_count_usize
        .checked_mul(COL_LEN)
        .ok_or_else(|| Error::Corrupt("col_count too large".into()))?;
    let expected_len = BASE_LEN
        .checked_add(cols_len)
        .ok_or_else(|| Error::Corrupt("meta length overflow".into()))?;
    if buf.len() != expected_len {
        return Err(Error::Corrupt("meta length mismatch".into()));
    }

    let mut cols = Vec::with_capacity(col_count_usize);
    for _ in 0..col_count_usize {
        let col_id = u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let encoding = u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let col_offset = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let len = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;
        cols.push(ColumnMeta {
            col_id,
            encoding,
            offset: col_offset,
            len,
        });
    }

    Ok(ChunkMeta {
        row_count,
        ts_min,
        ts_max,
        cols,
    })
}
