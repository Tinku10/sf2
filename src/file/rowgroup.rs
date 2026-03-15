pub mod column;

use crate::serde;
use crate::types::fields::PlankField;
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
            let column_bytes = col.to_bytes();
            v.extend_from_slice(&(column_bytes.len() as u32).to_le_bytes());
            v.extend_from_slice(&column_bytes);
        }

        v
    }
}

impl<'a> serde::Deserialize<'a> for RowGroup {
    type Schema = Vec<PlankField>;
    fn from_bytes(bytes: &[u8], schema: &'a Self::Schema) -> std::io::Result<Self> {
        let mut br = BufReader::new(Cursor::new(bytes));
        let mut columns = Vec::new();

        let mut pos = 0;
        let mut schema_id = 0;

        while pos + 4 < bytes.len() {
            let size = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u32")
            })?) as usize;

            pos += 4;

            columns.push(Column::from_bytes(
                &bytes[pos..pos + size],
                &schema[schema_id],
            )?);
            schema_id += 1;

            pos += size;
        }

        Ok(RowGroup { columns })
    }
}
