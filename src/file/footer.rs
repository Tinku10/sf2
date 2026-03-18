use crate::serde::{Deserialize, Serialize};
use crate::types::{fields::PlankField, types::PlankType};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};

pub(crate) const ROWGROUP_SIZE: usize = 10;
const PLANK_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
pub(crate) struct Footer {
    pub(crate) schema: Vec<PlankField>,
    pub(crate) offsets: Vec<u32>,
    pub(crate) row_count: u32,
    pub(crate) col_count: u32,
    pub(crate) row_group_count: u32,
}

#[derive(Debug)]
enum FooterFieldType {
    Schema,
    Offsets,
    RowCount,
    ColCount,
    RowGroupCount,
}

impl Default for Footer {
    fn default() -> Self {
        Footer {
            schema: Vec::new(),
            offsets: Vec::new(),
            row_count: 0,
            col_count: 0,
            row_group_count: 0,
        }
    }
}

impl Footer {
    pub fn new(
        schema: Vec<PlankField>,
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

    fn get_footer_layout() -> Vec<FooterFieldType> {
        // Use the plank_version if there are layout changes
        match PLANK_VERSION {
            _ => vec![
                FooterFieldType::Schema,
                FooterFieldType::RowCount,
                FooterFieldType::ColCount,
                FooterFieldType::RowGroupCount,
                FooterFieldType::Offsets,
            ],
        }
    }

    fn parse_field(reader: &mut BufReader<Cursor<&[u8]>>) -> std::io::Result<Vec<u8>> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;

        let size = u32::from_le_bytes(buf);

        let mut buf = vec![0u8; size as usize];
        reader.read_exact(&mut buf)?;

        Ok(buf)
    }

    fn parse_count(bytes: &[u8]) -> std::io::Result<u32> {
        Ok(u32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "expected bytes to be u32")
        })?))
    }

    pub fn parse_schema(bytes: &[u8]) -> std::io::Result<Vec<PlankField>> {
        let mut pos = 0;
        let mut v: Vec<PlankField> = Vec::new();

        while pos + 4 < bytes.len() {
            let size = u32::from_le_bytes(bytes[pos..pos + 4].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "expected bytes to be u32")
            })?);

            pos += 4;

            let field_name =
                std::str::from_utf8(&bytes[pos..pos + size as usize]).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected a field name")
                })?;

            pos += size as usize;

            let field_type = PlankType::from_bytes(&bytes[pos..], &())?;

            pos += field_type.encoded_size();

            v.push(PlankField::new(field_name, field_type))
        }

        Ok(v)
    }

    pub fn parse_offsets(bytes: &[u8]) -> std::io::Result<Vec<u32>> {
        let mut v = Vec::new();
        for chunk in bytes.chunks_exact(4) {
            v.push(u32::from_le_bytes(chunk.try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u32")
            })?));
        }
        Ok(v)
    }
}

impl Serialize for Footer {
    fn to_bytes(&self) -> Vec<u8> {
        let mut s = Vec::new();

        // s.extend_from_slice(format!("!PLANK_VERSION={}", PLANK_VERSION).as_bytes());
        // s.push(b'\n');

        for field in Self::get_footer_layout() {
            let bytes: Vec<u8> = match field {
                FooterFieldType::Schema => {
                    self.schema.iter().flat_map(|f| f.to_bytes()).collect()
                }
                FooterFieldType::RowCount => self.row_count.to_le_bytes().to_vec(),
                FooterFieldType::ColCount => self.col_count.to_le_bytes().to_vec(),
                FooterFieldType::RowGroupCount => self.row_group_count.to_le_bytes().to_vec(),
                FooterFieldType::Offsets => self
                    .offsets
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect(),
            };

            s.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            s.extend_from_slice(&bytes);
        }

        let checksum = Sha256::digest(&s);
        s.extend_from_slice(&checksum.to_vec());

        s
    }
}

impl<'a> Deserialize<'a> for Footer {
    type Schema = ();
    fn from_bytes(bytes: &[u8], _: &'a Self::Schema) -> std::io::Result<Self> {
        let mut br = BufReader::new(Cursor::new(bytes));
        let before = br.stream_position()?;

        let mut footer = Footer::default();

        for field in Self::get_footer_layout() {
            match field {
                FooterFieldType::Schema => {
                    footer.schema = Self::parse_schema(&Self::parse_field(&mut br)?)?
                }
                FooterFieldType::RowCount => {
                    footer.row_count = Self::parse_count(&Self::parse_field(&mut br)?)?
                }
                FooterFieldType::ColCount => {
                    footer.col_count = Self::parse_count(&Self::parse_field(&mut br)?)?
                }
                FooterFieldType::RowGroupCount => {
                    footer.row_group_count = Self::parse_count(&Self::parse_field(&mut br)?)?
                }
                FooterFieldType::Offsets => {
                    footer.offsets = Self::parse_offsets(&Self::parse_field(&mut br)?)?
                }
            }
        }

        let after = br.stream_position()?;

        // Sha256 is 32 bytes
        let mut provided = [0u8; 32];
        br.read_exact(&mut provided);

        let buf = &bytes[before as usize..after as usize];

        if provided != Sha256::digest(buf)[..] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "incorrect checksum found",
            ));
        }

        Ok(footer)
    }
}
