use crate::serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use crate::types::fields::PlankField;

#[derive(Debug, Clone)]
pub enum PlankType {
    Str,
    Int32,
    Int64,
    // Float64,
    Bool,
    List(Box<PlankType>),
    Struct(Vec<PlankField>),
}

impl PlankType {
    pub fn encoded_size(&self) -> usize {
        // 1 byte is always reserved for type_id (u8)
        match self {
            Self::Str => 1,
            Self::Int32 => 1,
            Self::Int64 => 1,
            Self::Bool => 1,
            Self::Struct(fields) => 1 + 4 + fields.iter().map(|f| f.encoded_size()).sum::<usize>(),
            Self::List(list_type) => 1 + list_type.encoded_size(),
        }
    }

    pub fn infer_type(value: &str) -> Self {
        if value.parse::<i32>().is_ok() {
            return PlankType::Int32;
        }
        if value.parse::<i64>().is_ok() {
            return PlankType::Int64;
        }
        if value.parse::<bool>().is_ok() {
            return PlankType::Bool;
        }
        if let Ok(t) = PlankType::infer_extended_type(value) {
            return t;
        }
        PlankType::Str
    }

    pub fn infer_extended_type(s: &str) -> std::io::Result<PlankType> {
        let s = serde_json::from_str(s)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        match s {
            serde_json::Value::Number(n) => {
                if let Some(_) = n.as_i64() {
                    Ok(PlankType::Int64)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unsupported number",
                    ))
                }
            }
            serde_json::Value::Bool(_) => Ok(PlankType::Bool),
            serde_json::Value::String(_) => Ok(PlankType::Str),
            serde_json::Value::Object(o) => {
                let fields = o
                    .iter()
                    .map(|(k, v)| {
                        Ok(PlankField::new(
                            k,
                            Self::infer_extended_type(&v.to_string())?,
                        ))
                    })
                    .collect::<std::io::Result<Vec<PlankField>>>()?;

                Ok(PlankType::Struct(fields))
            }
            serde_json::Value::Array(a) => {
                let items = a
                    .iter()
                    .map(|v| Self::infer_extended_type(&v.to_string()))
                    .collect::<std::io::Result<Vec<_>>>()?;

                // if !(items.is_empty() || items.iter().all(|e| items[0] == e)) {
                //     Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "list should be homogeneous"))
                // }

                // Need a way to infer type if the list is empty
                Ok(PlankType::List(Box::new(items[0].clone())))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported data type",
            )),
        }
    }
}

impl fmt::Display for PlankType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Str => write!(f, "Str"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::Bool => write!(f, "Bool"),
            Self::Struct(_) => write!(f, "Struct"),
            Self::List(_) => write!(f, "List"),
        }
    }
}

impl Serialize for PlankType {
    fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        let id: u8 = match self {
            Self::Str => 1,
            Self::Int32 => 2,
            Self::Int64 => 3,
            Self::Bool => 4,
            Self::Struct(_) => 5,
            Self::List(_) => 6,
        };
        let mut v = id.to_le_bytes().to_vec();

        if let Self::Struct(fields) = self {
            v.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for field in fields {
                v.extend_from_slice(&field.to_bytes()?);
            }
        } else if let Self::List(list_type) = self {
            v.extend_from_slice(&list_type.to_bytes()?);
        }

        Ok(v)
    }
}

impl<'a> Deserialize<'a> for PlankType {
    type Schema = ();
    fn from_bytes(bytes: &[u8], schema: &'a Self::Schema) -> std::io::Result<Self> {
        let id = bytes[0]
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u8"))?;
        match id {
            1 => Ok(Self::Str),
            2 => Ok(Self::Int32),
            3 => Ok(Self::Int64),
            4 => Ok(Self::Bool),
            5 => {
                let mut v = Vec::new();
                let fields_size = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u32")
                })?) as usize;
                let mut pos = 5;
                for _ in 0..fields_size {
                    let t = PlankField::from_bytes(&bytes[pos..], schema)?;
                    // let t = PlankType::from_bytes(&bytes[pos..])?;
                    pos += t.encoded_size();
                    v.push(t);
                }

                Ok(Self::Struct(v))
            }
            6 => Ok(Self::List(Box::new(PlankType::from_bytes(
                &bytes[1..],
                &(),
            )?))),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown type id {}", id),
            )),
        }
    }
}
