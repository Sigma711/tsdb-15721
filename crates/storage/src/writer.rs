use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use common::error::{Error, Result};
use crc32fast::Hasher;
use datamodel::batch::RecordBatch;

use crate::format::{self, Header};
use crate::meta::{self, ChunkMeta, ColumnMeta};

pub fn write_chunk(path: &Path, batch: &RecordBatch) -> Result<()> {
    let row_count = batch.len();
    if batch.ts.len() != batch.series_id.len() || batch.ts.len() != batch.value.len() {
        return Err(Error::Corrupt("column length mismatch".into()));
    }
    if row_count > u32::MAX as usize {
        return Err(Error::Unsupported("row_count exceeds u32".into()));
    }

    let (ts_min, ts_max) = if batch.ts.is_empty() {
        (0, 0)
    } else {
        let mut min = batch.ts[0];
        let mut max = batch.ts[0];
        for &ts in &batch.ts[1..] {
            if ts < min {
                min = ts;
            }
            if ts > max {
                max = ts;
            }
        }
        (min, max)
    };

    let col_count = 3usize;
    let meta_len = meta_len_for_cols(col_count);

    let mut file = File::create(path)?;
    format::write_header(
        &mut file,
        &Header {
            meta_len: 0,
            meta_crc32: 0,
        },
    )?;

    if meta_len > 0 {
        file.write_all(&vec![0u8; meta_len])?;
    }

    let ts_offset = file.seek(SeekFrom::Current(0))?;
    for &ts in &batch.ts {
        file.write_all(&ts.to_le_bytes())?;
    }

    let series_offset = file.seek(SeekFrom::Current(0))?;
    for &series_id in &batch.series_id {
        file.write_all(&series_id.to_le_bytes())?;
    }

    let value_offset = file.seek(SeekFrom::Current(0))?;
    for &value in &batch.value {
        file.write_all(&value.to_le_bytes())?;
    }

    let row_count_u32 = row_count as u32;
    let cols = vec![
        ColumnMeta {
            col_id: 0,
            encoding: 0,
            offset: ts_offset,
            len: row_count as u64 * 8,
        },
        ColumnMeta {
            col_id: 1,
            encoding: 0,
            offset: series_offset,
            len: row_count as u64 * 4,
        },
        ColumnMeta {
            col_id: 2,
            encoding: 0,
            offset: value_offset,
            len: row_count as u64 * 8,
        },
    ];

    let meta = ChunkMeta {
        row_count: row_count_u32,
        ts_min,
        ts_max,
        cols,
    };
    let meta_bytes = meta::encode_meta(&meta);
    if meta_bytes.len() != meta_len {
        return Err(Error::Corrupt("meta length mismatch".into()));
    }

    let mut hasher = Hasher::new();
    hasher.update(&meta_bytes);
    let meta_crc32 = hasher.finalize();
    let meta_len_u32 =
        u32::try_from(meta_bytes.len()).map_err(|_| Error::Unsupported("meta too large".into()))?;

    file.seek(SeekFrom::Start(0))?;
    format::write_header(
        &mut file,
        &Header {
            meta_len: meta_len_u32,
            meta_crc32,
        },
    )?;
    file.write_all(&meta_bytes)?;
    Ok(())
}

fn meta_len_for_cols(col_count: usize) -> usize {
    let base_len = 4 + 8 + 8 + 4;
    let col_len = 2 + 2 + 8 + 8;
    base_len + col_count * col_len
}
