use crate::file::SF2;
use crate::footer::Footer;
use crate::rowgroup::column::Column;
use crate::rowgroup::RowGroup;

use std::error::Error;

pub fn read_csv(file_path: &str) -> std::io::Result<Vec<Vec<String>>> {
    let mut reader = csv::Reader::from_path(file_path).unwrap();

    let headers = reader
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let body = reader
        .records()
        .map(|r| r.unwrap().iter().map(|s| s.to_string()).collect())
        .collect::<Vec<Vec<String>>>();

    let mut rows = Vec::with_capacity(body.len() + 1);
    rows.push(headers);
    rows.extend(body);

    Ok(rows)
}

pub fn csv_to_sf2(csv_path: &str) -> std::io::Result<SF2> {
    let v = read_csv(csv_path)?;

    println!("{:?}", v);
    let chunk = 2;

    let mut rowgroups = Vec::new();
    let mut offsets = Vec::new();
    let row_count = v.len();
    let mut file_offset = 0;

    for rows in v[1..].chunks(chunk) {
        let mut rowgroup = Vec::new();
        for i in 0..rows[0].len() {
            let mut column = Vec::new();
            let mut offset = file_offset;
            offsets.push(offset as u32);

            for j in 0..std::cmp::min(chunk, rows.len()) {
                column.push(rows[j][i].clone());
                offset += rows[j][i].len() + 1;
            }

            rowgroup.push(Column::new(column));
            file_offset = offset;
        }

        rowgroups.push(RowGroup::new(rowgroup));
    }

    let col_count = v[0].len();

    let footer = Footer::new(
        v[0].iter()
            .map(|s| (s.clone(), "str".to_string()))
            .collect(),
        offsets,
        row_count as u32,
        col_count as u32,
    );

    Ok(SF2::new(rowgroups, footer))
}
