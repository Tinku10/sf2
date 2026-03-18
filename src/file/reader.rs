use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::file::footer::{self, Footer};
use crate::file::rowgroup::column::Column;
use crate::file::rowgroup::RowGroup;
use crate::serde::Deserialize;
use crate::types::{data::PlankData, fields::PlankField, types::PlankType};

pub struct PlankReader {
    file: BufReader<File>,
    footer: Footer,
}

#[derive(Debug)]
pub struct RecordBatch {
    schema: Vec<PlankField>,
    columns: Vec<Column>,
    row_count: usize,
}

pub struct RowGroupIterator<'a> {
    reader: &'a mut PlankReader,
    offsets: Vec<u32>,
    index: usize,
}

pub struct RowIterator {
    row_group: Option<RowGroup>,
    row: usize,
}

impl PlankReader {
    fn parse_schema(bytes: &[u8]) -> std::io::Result<Vec<PlankField>> {
        Footer::parse_schema(bytes)
    }

    fn parse_offsets(bytes: &[u8]) -> std::io::Result<Vec<u32>> {
        Footer::parse_offsets(bytes)
    }

    pub fn open<P: AsRef<Path>>(file_path: P) -> std::io::Result<Self> {
        let mut f = File::open(file_path)?;
        // Footer offset f;or u32
        f.seek(SeekFrom::End(-4))?;

        let mut footer_offset = [0u8; 4];
        f.read_exact(&mut footer_offset)?;

        let footer_offset = u32::from_le_bytes(footer_offset);
        // println!("{} reader", footer_offset);

        f.seek(SeekFrom::Start(footer_offset as u64))?;

        let mut br = BufReader::new(f);

        let mut footer_buf = Vec::new();
        br.read_to_end(&mut footer_buf);

        let footer = Footer::from_bytes(&footer_buf, &())?;

        Ok(Self { file: br, footer })
    }

    pub fn schema(&self) -> &[PlankField] {
        &self.footer.schema
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    fn read_row_group_raw(&mut self, id: usize) -> std::io::Result<RowGroup> {
        let footer = &self.footer;
        let rg_offsets = &footer.offsets;

        if id as u32 >= footer.row_group_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "requested row group does not exist",
            ));
        }

        let br = &mut self.file;

        // Go to the beginning of the row group
        br.seek(SeekFrom::Start(rg_offsets[id] as u64))?;

        let mut buf = [0u8; 4];
        br.read_exact(&mut buf)?;

        let row_group_size = u32::from_le_bytes(buf);

        let mut buf = vec![0u8; row_group_size as usize];
        br.read_exact(&mut buf)?;

        RowGroup::from_bytes(&buf, &self.footer.schema)
    }

    pub fn read_row_group(&mut self, id: usize) -> std::io::Result<RecordBatch> {
        let rg = self.read_row_group_raw(id)?;

        Ok(RecordBatch {
            schema: self.footer.schema.clone(),
            columns: rg.columns,
            row_count: 0,
        })
    }

    pub fn read_row_group_columns(
        &mut self,
        id: usize,
        column_ids: &[usize],
    ) -> std::io::Result<RecordBatch> {
        let rg = self.read_row_group_raw(id)?;
        let mut columns = rg.columns;

        Ok(RecordBatch {
            schema: self.footer.schema.clone(),
            columns: column_ids
                .iter()
                .map(|&i| std::mem::replace(&mut columns[i], Column::default()))
                .collect(),
            row_count: 0,
        })
    }
}

impl<'a> Iterator for RowGroupIterator<'a> {
    type Item = std::io::Result<RowGroup>;

    fn next(&mut self) -> Option<Self::Item> {
        let rg = self.reader.read_row_group_raw(self.index).ok()?;
        self.index += 1;
        Some(Ok(rg))
    }
}

impl Iterator for RowIterator {
    type Item = std::io::Result<Vec<PlankData>>;

    fn next(&mut self) -> Option<Self::Item> {
        let columns = self.row_group.as_ref()?.columns();

        if self.row >= columns[0].records.len() {
            return None;
        }

        let row: Vec<PlankData> = columns
            .iter()
            .map(|col| col.records[self.row].clone())
            .collect();

        self.row += 1;
        Some(Ok(row))
    }
}

impl<'a> IntoIterator for &'a mut PlankReader {
    type Item = std::io::Result<RowGroup>;
    type IntoIter = RowGroupIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let offsets = self.footer.offsets.clone();
        RowGroupIterator {
            reader: self,
            offsets,
            index: 0,
        }
    }
}

impl IntoIterator for RowGroup {
    type Item = std::io::Result<Vec<PlankData>>;
    type IntoIter = RowIterator;

    fn into_iter(self) -> Self::IntoIter {
        RowIterator {
            row_group: Some(self),
            row: 0,
        }
    }
}
