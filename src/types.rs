use crate::serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

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

#[derive(Debug, Clone)]
pub struct PlankField {
    name: String,
    field_type: PlankType,
}

#[derive(Debug, Clone)]
pub enum PlankData {
    Str(String),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    List(Vec<PlankData>),
    Struct(Vec<PlankData>),
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

    fn infer_extended_type(s: &str) -> std::io::Result<PlankType> {
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

// This is mostly not needed, plank files are encoded with numeric values for types
// impl FromStr for PlankType {
//     type Err = std::io::Error;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s {
//             "Str" => Ok(Self::Str),
//             "Int32" => Ok(Self::Int32),
//             "Int64" => Ok(Self::Int64),
//             "Bool" => Ok(Self::Bool),
//             _ => Err(std::io::Error::new(
//                 std::io::ErrorKind::InvalidData,
//                 format!("unable to infer type of {}", s),
//             )),
//         }
//     }
// }

// impl From<&PlankData> for PlankType {
//     fn from(data: &PlankData) -> Self {
//         // PlankType cannot be completely derived from PlankData without the schema
//         // But this gets the job done for what is needed by filling the names of keys with empty
//         // string
//         match data {
//             PlankData::Str(_) => PlankType::Str,
//             PlankData::Int32(_) => PlankType::Int32,
//             PlankData::Int64(_) => PlankType::Int64,
//             PlankData::Bool(_) => PlankType::Bool,
//             PlankData::Struct(s) => {
//                 let mut v = Vec::new();
//                 for field in s {
//                     v.push(PlankField::new("", PlankType::from(field)))
//                 }
//                 PlankType::Struct(v)
//             },
//             PlankData::List(l) => {
//                 let mut t = l.get(0)
//                 PlankType::List(t)
//             }
//         }
//     }
// }

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
}

impl FromStr for PlankData {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(n) = s.parse::<i32>() {
            return Ok(PlankData::Int32(n));
        } else if let Ok(n) = s.parse::<i64>() {
            return Ok(PlankData::Int64(n));
        } else if let Ok(b) = s.parse::<bool>() {
            return Ok(PlankData::Bool(b));
        } else if let Ok(t) = Self::parse_extended_value(s) {
            return Ok(t);
        }

        Ok(PlankData::Str(String::from(s)))
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
    fn to_bytes(&self) -> Vec<u8> {
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
                v.extend_from_slice(&field.to_bytes());
            }
        } else if let Self::List(list_type) = self {
            v.extend_from_slice(&list_type.to_bytes());
        }

        v
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

impl Serialize for PlankField {
    fn to_bytes(&self) -> Vec<u8> {
        // Format: field_size field_name type_size type_name
        let mut v = Vec::new();
        let name_bytes = self.name.as_bytes();

        v.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        v.extend_from_slice(name_bytes);
        // Type ID will always be a u32
        // v.extend_from_slice(4u32.to_le_bytes());
        v.extend_from_slice(&self.field_type.to_bytes());

        v
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

impl Serialize for PlankData {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            PlankData::Str(s) => {
                let mut v = Vec::new();
                let bytes = s.as_bytes();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Str));
                v.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                v.extend_from_slice(bytes);
                v
            }
            PlankData::Int32(n) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Int32));
                v.extend_from_slice(&n.to_le_bytes());
                v
            }
            PlankData::Int64(n) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Int64));
                v.extend_from_slice(&n.to_le_bytes());
                v
            }
            PlankData::Bool(b) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::to_bytes(&PlankType::Bool));
                v.extend_from_slice(&[*b as u8]);
                v
            }
            PlankData::Struct(s) => {
                let mut v = Vec::new();
                // v.extend_from_slice(&PlankType::from(st).to_bytes());
                v.extend_from_slice(&(s.len() as u32).to_le_bytes());
                for val in s {
                    v.extend_from_slice(&val.to_bytes());
                }
                v
            }
            PlankData::List(l) => {
                let mut v = Vec::new();
                v.extend_from_slice(&(l.len() as u32).to_le_bytes());
                for val in l {
                    v.extend_from_slice(&val.to_bytes());
                }
                v
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
            PlankType::Bool => {
                let b = bytes[0].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected bool")
                })?;
                Ok(PlankData::Bool(b))
            }
            PlankType::Struct(fields) => {
                let size = u32::from_le_bytes(bytes[..4].try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected struct size")
                })?) as usize;
                let mut v = Vec::new();
                let mut pos = 4;
                for i in 0..size {
                    let data = PlankData::from_bytes(&bytes[pos..], &fields[i].field_type())?;
                    pos += data.to_bytes().len();
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
                    pos += data.to_bytes().len();
                    v.push(data);
                }
                Ok(PlankData::List(v))
            }
        }
    }
}
