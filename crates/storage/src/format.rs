use std::fs::File;
use std::io::{Read, Write};

use common::error::{Error, Result};

pub const MAGIC: [u8; 4] = *b"TSDB";
pub const VERSION: u16 = 1;
pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub meta_len: u32,
    pub meta_crc32: u32,
}

pub fn write_header(file: &mut File, header: &Header) -> Result<()> {
    let header_len = HEADER_LEN as u16;
    file.write_all(&MAGIC)?;
    file.write_all(&VERSION.to_le_bytes())?;
    file.write_all(&header_len.to_le_bytes())?;
    file.write_all(&header.meta_len.to_le_bytes())?;
    file.write_all(&header.meta_crc32.to_le_bytes())?;
    Ok(())
}

pub fn read_header(file: &mut File) -> Result<Header> {
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(Error::Corrupt("bad magic".into()));
    }

    let mut version_bytes = [0u8; 2];
    file.read_exact(&mut version_bytes)?;
    let version = u16::from_le_bytes(version_bytes);
    if version != VERSION {
        return Err(Error::Unsupported(format!(
            "unsupported version: {}",
            version
        )));
    }

    let mut header_len_bytes = [0u8; 2];
    file.read_exact(&mut header_len_bytes)?;
    let header_len = u16::from_le_bytes(header_len_bytes);
    if header_len != HEADER_LEN as u16 {
        return Err(Error::Corrupt("header length mismatch".into()));
    }

    let mut meta_len_bytes = [0u8; 4];
    file.read_exact(&mut meta_len_bytes)?;
    let meta_len = u32::from_le_bytes(meta_len_bytes);

    let mut meta_crc_bytes = [0u8; 4];
    file.read_exact(&mut meta_crc_bytes)?;
    let meta_crc32 = u32::from_le_bytes(meta_crc_bytes);

    Ok(Header {
        meta_len,
        meta_crc32,
    })
}
