#![feature(bufreader_peek)]
#![feature(iter_array_chunks)]

mod file;
// mod query;
mod serde;
mod types;

mod bindings;

pub use crate::file::reader::PlankReader;
pub use crate::file::writer::PlankWriter;
pub use crate::types::{types::PlankType, data::PlankData, fields::PlankField};

