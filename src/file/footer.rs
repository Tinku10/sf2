use crate::serde;
use std::fmt::Write;
use std::io::{BufRead, Read, Cursor, BufReader};

pub(crate) const ROWGROUP_SIZE: usize = 10;

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
            .split(',')
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

        s.extend_from_slice(b"!SCHEMA=");
        for (k, v) in &self.schema {
            s.extend_from_slice(format!("{}:{}", k, v).as_bytes());
            s.push(b',')
        }
        s.push(b'\n');

        let row_group_cnt = self.offsets.len() as u32;

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

        s
    }
}

impl serde::Deserialize for Footer {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Self> {
        let mut br = BufReader::new(Cursor::new(bytes));

        // Skip !SCHEMA=
        br.consume(8);
        // Can be interpreted as valid text data
        let mut s = String::new();
        br.read_line(&mut s)?;
        let schema = Self::parse_schema(&s);

        // Skip !ROW_COUNT=
        br.consume(11);
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        let row_count = u32::from_le_bytes(s);
        br.consume(1);

        // Skip !COLUMN_COUNT=
        br.consume(14);
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        br.consume(1);
        let col_count = u32::from_le_bytes(s);

        // Skip !ROWGROUP_COUNT=
        br.consume(16);
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        br.consume(1);
        let row_group_count = u32::from_le_bytes(s);

        // Skip !ROWGROUP_OFFSETS=
        br.consume(18);
        let mut s = vec![0u8; (row_group_count * 4) as usize];
        br.read_exact(&mut s)?;
        br.consume(1);
        let offsets = Self::parse_offsets(&s);

        Ok(Footer::new(schema, offsets, row_count, col_count, row_group_count))
    }
}
