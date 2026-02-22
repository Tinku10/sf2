use crate::serde;

#[derive(Debug, Clone)]
pub(crate) struct Column {
    // id: u32,
    records: Vec<String>,
}

impl Column {
    pub fn new(records: Vec<String>) -> Self {
        Column { records }
    }

    pub fn records(&self) -> &Vec<String> {
        &self.records
    }
}

impl serde::Serialize for Column {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        for record in &self.records {
            buf.extend_from_slice(record.as_bytes());
            buf.push(0x1F);
        }

        buf
    }
}

impl serde::Deserialize for Column {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Self> {
        Ok(Column {
            records: String::from_utf8_lossy(bytes)
                .split('\x1F')
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        })
    }
}
