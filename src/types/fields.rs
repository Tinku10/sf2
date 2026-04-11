use crate::serde::{Deserialize, Serialize};
use crate::types::types::PlankType;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlankField {
    name: String,
    field_type: PlankType,
}

impl PlankField {
    pub fn new(name: &str, field_type: PlankType) -> Self {
        PlankField {
            name: String::from(name),
            field_type,
        }
    }

    pub fn encoded_size(&self) -> usize {
        4 + self.name.len() + self.field_type.encoded_size()
    }

    pub fn field_type(&self) -> &PlankType {
        &self.field_type
    }

    pub fn field_name(&self) -> &String {
        &self.name
    }

    pub fn from_value(name: &str, value: &str) -> Self {
        let field_type = if value.parse::<i32>().is_ok() {
            PlankType::Int32
        } else if value.parse::<i64>().is_ok() {
            PlankType::Int64
        } else if value.parse::<bool>().is_ok() {
            PlankType::Bool
        } else if let Ok(t) = PlankType::infer_extended_type(value) {
            t
        } else {
            PlankType::Str
        };
        PlankField::new(name, field_type)
    }
}

impl Serialize for PlankField {
    fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        // Format: field_size field_name type_size type_name
        let mut v = Vec::new();
        let name_bytes = self.name.as_bytes();

        v.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        v.extend_from_slice(name_bytes);
        // Type ID will always be a u32
        // v.extend_from_slice(4u32.to_le_bytes());
        v.extend_from_slice(&self.field_type.to_bytes()?);

        Ok(v)
    }
}

impl<'a> Deserialize<'a> for PlankField {
    type Schema = ();
    fn from_bytes(bytes: &[u8], schema: &'a Self::Schema) -> std::io::Result<Self> {
        let size =
            u32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u32")
            })?) as usize;

        let field_name = std::str::from_utf8(&bytes[4..4 + size as usize])
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("expected to read {} bytes", size),
                )
            })?
            .to_string();

        let field_type = PlankType::from_bytes(
            bytes[4 + size..].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "expected type")
            })?,
            schema,
        )?;

        Ok(PlankField {
            name: field_name,
            field_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_plankfield() {
        let field = PlankField::new("name", PlankType::Str);

        let serialized = field.to_bytes().unwrap();
        let deserialized = PlankField::from_bytes(&serialized, &()).unwrap();

        assert_eq!(deserialized.name, field.name);
        assert_eq!(deserialized.field_type, field.field_type);
    }

    #[test]
    fn test_encoded_size_planfield() {
        let field = PlankField::new("name", PlankType::Str);

        assert_eq!(
            field.encoded_size(),
            4 + 4 + PlankType::encoded_size(&PlankType::Str)
        );
    }

    #[test]
    fn test_infer_key_value_into_plankfield() {
        assert_eq!(
            PlankField::from_value("person", r#"{"name": "me", "age": 10}"#),
            PlankField::new(
                "person",
                PlankType::Struct(vec![
                    PlankField::new("name", PlankType::Str),
                    PlankField::new("age", PlankType::Int32)
                ])
            )
        )
    }
}
