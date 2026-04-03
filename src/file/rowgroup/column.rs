use crate::serde;
use crate::serde::{Deserialize, Serialize};
use crate::types::{data::PlankData, fields::PlankField, types::PlankType};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use flate2::read::ZlibDecoder;
use std::io::prelude::*;


#[derive(Debug, Clone)]
pub(crate) struct Column {
    // id: u32,
    pub(crate) records: Vec<PlankData>,
}

impl Default for Column {
    fn default() -> Self {
        Column {records: Vec::new()}
    }
}

impl Column {
    pub fn new(records: Vec<PlankData>) -> Self {
        Column { records }
    }
}

impl serde::Serialize for Column {
    fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        for record in &self.records {
            buf.extend_from_slice(&record.to_bytes()?);
        }

        let mut c = ZlibEncoder::new(Vec::new(), Compression::default());
        c.write_all(&buf)?;

        c.finish()
    }
}

impl<'a> serde::Deserialize<'a> for Column {
    type Schema = PlankField;
    fn from_bytes(bytes: &[u8], schema: &'a Self::Schema) -> std::io::Result<Self> {
        let mut c = ZlibDecoder::new(bytes);
        let mut bytes = Vec::new();
        c.read_to_end(&mut bytes)?;

        let mut pos = 0;
        let mut v = Vec::new();
        while pos < bytes.len() {
            let item = PlankData::from_bytes(&bytes[pos..], schema.field_type())?;
            let size = item.to_bytes()?.len();
            pos += size;
            v.push(item);
        }

        Ok(Column { records: v })
    }
}
