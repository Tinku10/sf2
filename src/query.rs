pub mod components;

use crate::file::reader::PlankReader;
use crate::file::rowgroup::RowGroup;
use crate::query::components::{Cmp, PlankQuery, QueryKey};
use crate::types::{data::PlankData, fields::PlankField, types::PlankType};
use std::path::Path;

fn apply_selects(query: &PlankQuery, schema: &Vec<PlankField>, row_group: &RowGroup) {}

fn apply_filters<'a>(
    query: &PlankQuery,
    schema: &Vec<PlankField>,
    row_group: &'a RowGroup,
) -> Option<&'a RowGroup> {
    if let Some(filter) = &query.filter {
        match filter {
            Cmp::Eq(QueryKey::RowGroup, PlankData::Int32(id)) => {
                if row_group.id() == *id as u32 {
                    return Some(row_group);
                }
            }
            _ => {}
        }
    }

    None
}

pub fn run_query(
    reader: &mut PlankReader,
    query: &PlankQuery,
) -> std::io::Result<Vec<Vec<PlankData>>> {
    let schema = reader.get_schema().clone();
    let cnt = reader.meta().row_group_count() as usize;

    let mut rows = Vec::new();

    for rg in reader {
        if let Ok(r) = rg {
            if let Some(x) = apply_filters(query, &schema, &r) {
                let columns = x.columns();

                for i in 0..cnt {
                    let row: Vec<PlankData> =
                        columns.iter().map(|col| col.records()[i].clone()).collect();
                    rows.push(row);
                }
            }
        }
    }

    Ok(rows)
}
