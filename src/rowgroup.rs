pub mod column;

use crate::serde;
use column::Column;

pub struct RowGroup {
    // id: u32,
    columns: Vec<Column>,
}

impl RowGroup {
    pub fn new(columns: Vec<Column>) -> Self {
        RowGroup { columns }
    }
}

impl serde::Serialize for RowGroup {
    fn to_string(&self) -> String {
        let mut v = Vec::new();
        for col in &self.columns {
            v.push(col.to_string());
        }

        let mut s = v.join("\n");
        s.push_str("\n");
        s
    }
}
