use crate::serde::{Deserialize, Serialize};
use crate::types::types::PlankType;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum PlankData {
    Str(String),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    List(Vec<PlankData>),
    Struct(Vec<PlankData>),
}

impl PlankData {
    pub fn parse_value(value: &str) -> Self {
        if let Ok(n) = value.parse::<i32>() {
            return PlankData::Int32(n);
        } else if let Ok(n) = value.parse::<i64>() {
            return PlankData::Int64(n);
        } else if let Ok(b) = value.parse::<bool>() {
            return PlankData::Bool(b);
        } else if let Ok(t) = Self::parse_extended_value(value) {
            return t;
        }

        PlankData::Str(String::from(value))
    }

    fn parse_extended_value(s: &str) -> std::io::Result<PlankData> {
        let s = serde_json::from_str(s)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        match s {
            serde_json::Value::Number(n) => {
                if let Some(n) = n.as_i64() {
                    Ok(PlankData::Int64(n))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unsupported number",
                    ))
                }
            }
            serde_json::Value::Bool(b) => Ok(PlankData::Bool(b)),
            serde_json::Value::String(s) => Ok(PlankData::Str(s)),
            serde_json::Value::Object(o) => {
                let fields = o
                    .iter()
                    .map(|(_, v)| Self::parse_extended_value(&v.to_string()))
                    .collect::<std::io::Result<Vec<_>>>()?;

                Ok(PlankData::Struct(fields))
            }
            serde_json::Value::Array(a) => {
                let items = a
                    .iter()
                    .map(|v| Self::parse_extended_value(&v.to_string()))
                    .collect::<std::io::Result<Vec<_>>>()?;
                Ok(PlankData::List(items))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported data type",
            )),
        }
    }

    pub fn parse(s: &str, data_type: &PlankType) -> std::io::Result<Self> {
        match data_type {
            PlankType::Int32 => Ok(PlankData::Int32(s.parse::<i32>().unwrap())),
            PlankType::Int64 => Ok(PlankData::Int64(s.parse::<i64>().unwrap())),
            PlankType::Bool => Ok(PlankData::Bool(s.parse::<bool>().unwrap())),
            PlankType::Struct(_) | PlankType::List(_) => Self::parse_extended_value(s),
            _ => Ok(PlankData::Str(String::from(s))),
        }
    }

    pub fn get_struct_field(&self, schema: &PlankType, field_name: &str) -> Option<&Self> {
        // TODO: field_name can be made to get a dot(.) separated names
        match (self, schema) {
            (Self::Struct(fields), PlankType::Struct(types)) => {
                for (k, v) in types.iter().zip(fields) {
                    if k.field_name() == field_name {
                        return Some(v);
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn get(&self, index: usize) -> Option<&Self> {
        match self {
            PlankData::Struct(fields) => fields.get(index),
            PlankData::List(items) => items.get(index),
            _ => None,
        }
    }
}

impl fmt::Display for PlankData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Str(s) => write!(f, "'{}'", s),
            Self::Int32(n) => write!(f, "{}", n),
            Self::Int64(n) => write!(f, "{}", n),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Struct(fields) => {
                write!(f, "{{")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, "}}")
            }
            Self::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
        }
    }
}

impl Serialize for PlankData {
    fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        match self {
            PlankData::Str(s) => {
                let mut v = Vec::new();
                let bytes = s.as_bytes();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Str));
                v.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                v.extend_from_slice(bytes);
                Ok(v)
            }
            PlankData::Int32(n) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Int32));
                v.extend_from_slice(&n.to_le_bytes());
                Ok(v)
            }
            PlankData::Int64(n) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Int64));
                v.extend_from_slice(&n.to_le_bytes());
                Ok(v)
            }
            PlankData::Bool(b) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Bool));
                v.extend_from_slice(&[*b as u8]);
                Ok(v)
            }
            PlankData::Struct(s) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::from(st).to_bytes());
                v.extend_from_slice(&(s.len() as u32).to_le_bytes());
                for val in s {
                    v.extend_from_slice(&val.to_bytes()?);
                }
                Ok(v)
            }
            PlankData::List(l) => {
                let mut v = Vec::new();
                v.extend_from_slice(&(l.len() as u32).to_le_bytes());
                for val in l {
                    v.extend_from_slice(&val.to_bytes()?);
                }
                Ok(v)
            }
        }
    }
}

impl<'a> Deserialize<'a> for PlankData {
    type Schema = PlankType;
    fn from_bytes(bytes: &[u8], schema: &'a Self::Schema) -> std::io::Result<Self> {
        // let value_type = schema.field_type();
        match schema {
            PlankType::Str => {
                let size = u32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u32")
                })?);
                let field_value = std::str::from_utf8(&bytes[4..4 + size as usize])
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("expected to read {} bytes", size),
                        )
                    })?
                    .to_string();
                Ok(PlankData::Str(field_value))
            }
            PlankType::Int32 => {
                let n = i32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u32")
                })?);
                Ok(PlankData::Int32(n))
            }
            PlankType::Int64 => {
                let n = i64::from_le_bytes(bytes[..8].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected u64")
                })?);
                Ok(PlankData::Int64(n))
            }
            PlankType::Bool => match bytes[0] {
                0 => Ok(PlankData::Bool(false)),
                1 => Ok(PlankData::Bool(true)),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "expected bool",
                )),
            },
            PlankType::Struct(fields) => {
                let size = u32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected struct size")
                })?) as usize;
                let mut v = Vec::new();
                let mut pos = 4;
                for i in 0..size {
                    let data = PlankData::from_bytes(&bytes[pos..], &fields[i].field_type())?;
                    pos += data.to_bytes()?.len();
                    v.push(data);
                }
                Ok(PlankData::Struct(v))
            }
            PlankType::List(list_type) => {
                let size = u32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected struct size")
                })?) as usize;
                let mut v = Vec::new();
                let mut pos = 4;
                for _ in 0..size {
                    let data = PlankData::from_bytes(&bytes[pos..], list_type.as_ref())?;
                    pos += data.to_bytes()?.len();
                    v.push(data);
                }
                Ok(PlankData::List(v))
            }
        }
    }
}
