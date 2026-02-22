use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::file::footer::{self, Footer};
use crate::file::rowgroup::column::Column;
use crate::file::rowgroup::RowGroup;
use crate::file::PlankMeta;
use crate::serde::Serialize;

pub struct PlankWriter {
    file: BufWriter<File>,
}

impl PlankWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let mut file = File::create(path)?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    fn write_rowgroup(&mut self, rg: &RowGroup) -> std::io::Result<u32> {
        self.file.write_all(&rg.to_bytes())?;
        self.file.stream_position()?.try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "offset does not fit into u32",
            )
        })
    }

    fn write_footer(&mut self, footer: &Footer) -> std::io::Result<()> {
        let before: u32 = self.file.stream_position()?.try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "offset does not fit into u32",
            )
        })?;
        self.file.write_all(&footer.to_bytes())?;
        self.file.write_all(b"!FOOTER_OFFSET=")?;
        self.file.write_all(&before.to_le_bytes())?;
        self.file.write_all(b"\n");
        Ok(())
    }

    pub fn write_from_csv<P: AsRef<Path>>(&mut self, input: P) -> std::io::Result<()> {
        let mut reader = csv::Reader::from_path(input).unwrap();
        let mut offsets = Vec::new();
        let mut curr_offset = 0;

        let schema = reader
            .headers()?
            .iter()
            .map(|s| (s.to_string(), "str".to_string()))
            .collect::<Vec<(String, String)>>();

        let col_count = schema.len() as u32;
        let mut row_count = 0u32;

        let mut row_groups = Vec::new();

        for chunk in &reader.records().chunks(footer::ROWGROUP_SIZE) {
            let mut row_group = vec![Vec::new(); schema.len()];

            for row in chunk {
                let row = row?;
                for i in 0..schema.len() {
                    let item = row[i].to_string();
                    row_group[i].push(item);
                }
                row_count += 1;
            }

            let mut columns = Vec::new();
            for rg in row_group {
                columns.push(Column::new(rg));
            }

            row_groups.push(RowGroup::new(columns));
        }

        for rg in &row_groups {
            offsets.push(curr_offset);
            curr_offset = self.write_rowgroup(rg)?
        }

        // Add an extra offset pointing to the beginning of the footer
        // This will be used to know the byte size of any rowgroup N (offsets[N + 1] - offsets[N])
        offsets.push(curr_offset);

        let footer = Footer::new(schema, offsets, row_count, col_count, row_groups.len() as u32);
        self.write_footer(&footer)?;

        Ok(())
    }
}
