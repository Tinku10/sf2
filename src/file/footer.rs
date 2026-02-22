use crate::serde;
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};

pub(crate) const ROWGROUP_SIZE: usize = 10;
const PLANK_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
pub(crate) struct Footer {
    schema: Vec<(String, String)>,
    offsets: Vec<u32>,
    row_count: u32,
    col_count: u32,
    row_group_count: u32,
}

impl Footer {
    pub fn new(
        schema: Vec<(String, String)>,
        offsets: Vec<u32>,
        row_count: u32,
        col_count: u32,
        row_group_count: u32,
    ) -> Self {
        Footer {
            schema,
            offsets,
            row_count,
            col_count,
            row_group_count,
        }
    }

    pub fn schema(&self) -> &Vec<(String, String)> {
        &self.schema
    }

    pub fn offsets(&self) -> &Vec<u32> {
        // TODO: Provide a better way to request limited offsets
        &self.offsets
    }

    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    pub fn col_count(&self) -> u32 {
        self.col_count
    }

    pub fn row_group_count(&self) -> u32 {
        self.row_group_count
    }

    fn parse_schema(line: &str) -> Vec<(String, String)> {
        line.trim()
            .split('\x1F')
            .filter_map(|item| {
                let mut it = item.split(':');
                match (it.next(), it.next()) {
                    (Some(col), Some(ty)) => Some((col.to_string(), ty.to_string())),
                    _ => None,
                }
            })
            .collect()
    }

    fn parse_offsets(bytes: &Vec<u8>) -> Vec<u32> {
        let mut v = Vec::new();
        for chunk in bytes.chunks_exact(4) {
            v.push(u32::from_le_bytes(chunk.try_into().unwrap()));
        }
        v
    }
}

impl serde::Serialize for Footer {
    fn to_bytes(&self) -> Vec<u8> {
        let mut s = Vec::new();

        // s.extend_from_slice(format!("!PLANK_VERSION={}", PLANK_VERSION).as_bytes());
        // s.push(b'\n');
        s.extend_from_slice(b"!SCHEMA=");
        for (k, v) in &self.schema {
            s.extend_from_slice(format!("{}:{}", k, v).as_bytes());
            s.push(0x1F)
        }
        s.push(b'\n');

        let row_group_cnt = self.row_group_count as u32;

        s.extend_from_slice(b"!ROW_COUNT=");
        s.extend(self.row_count().to_le_bytes());
        s.push(b'\n');
        s.extend_from_slice(b"!COLUMN_COUNT=");
        s.extend(self.col_count.to_le_bytes());
        s.push(b'\n');
        s.extend_from_slice(b"!ROWGROUP_COUNT=");
        s.extend(row_group_cnt.to_le_bytes().iter());
        s.push(b'\n');
        s.extend_from_slice(b"!ROWGROUP_OFFSETS=");
        for offset in &self.offsets {
            s.extend(offset.to_le_bytes());
        }
        s.push(b'\n');

        let checksum = Sha256::digest(&s);
        s.extend_from_slice(b"!CHECKSUM=");
        s.extend_from_slice(&checksum.to_vec());
        s.push(b'\n');

        s
    }
}

impl serde::Deserialize for Footer {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Self> {
        let mut br = BufReader::new(Cursor::new(bytes));

        let before = br.stream_position()?;

        // Skip !SCHEMA=
        // br.consume(8);
        br.seek(SeekFrom::Current(8))?;
        // Can be interpreted as valid text data
        let mut s = String::new();
        br.read_line(&mut s)?;
        let schema = Self::parse_schema(&s);

        // Skip !ROW_COUNT=
        br.seek(SeekFrom::Current(11))?;
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        let row_count = u32::from_le_bytes(s);
        br.seek(SeekFrom::Current(1))?;

        // Skip !COLUMN_COUNT=
        br.seek(SeekFrom::Current(14))?;
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        br.seek(SeekFrom::Current(1))?;
        let col_count = u32::from_le_bytes(s);

        // Skip !ROWGROUP_COUNT=
        br.seek(SeekFrom::Current(16))?;
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        br.seek(SeekFrom::Current(1))?;
        let row_group_count = u32::from_le_bytes(s);

        // Skip !ROWGROUP_OFFSETS=
        br.seek(SeekFrom::Current(18))?;
        let mut s = vec![0u8; ((row_group_count + 1) * 4) as usize];
        br.read_exact(&mut s)?;
        br.seek(SeekFrom::Current(1))?;
        let offsets = Self::parse_offsets(&s);

        let after = br.stream_position()?;

        // Skip !CHECKSUM=
        br.seek(SeekFrom::Current(10))?;
        // Sha256 is 32 bytes
        let mut s = [0u8; 32];
        br.read(&mut s);
        br.seek(SeekFrom::Current(1))?;

        br.seek(SeekFrom::Start(before))?;

        let mut buf = vec![0u8; (after - before) as usize];
        br.read_exact(&mut buf)?;

        if s != Sha256::digest(&buf)[..] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksums do not match",
            ));
        }

        Ok(Footer::new(
            schema,
            offsets,
            row_count,
            col_count,
            row_group_count,
        ))
    }
}
