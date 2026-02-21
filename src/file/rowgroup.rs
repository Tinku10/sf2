pub mod column;

use crate::serde;
use column::Column;

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
