pub mod column;

use crate::serde;
use column::Column;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};

#[derive(Debug, Clone)]
pub struct RowGroup {
    // id: u32,
    columns: Vec<Column>,
}

impl RowGroup {
    pub fn new(columns: Vec<Column>) -> Self {
        RowGroup { columns }
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }
}

impl serde::Serialize for RowGroup {
    fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        for col in &self.columns {
            v.push(col.to_bytes());
        }

        let mut s = v.join(&b'\n');
        s.push(b'\n');
        s
    }
}

impl serde::Deserialize for RowGroup {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Self> {
        let mut br = BufReader::new(Cursor::new(bytes));

        let mut columns = Vec::new();

        while !br.fill_buf()?.is_empty() {
            let mut line = String::new();
            br.read_line(&mut line)?;
            columns.push(Column::from_bytes(line.as_bytes())?);
        }

        Ok(RowGroup {columns})

    }
}
