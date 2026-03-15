use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::file::footer::{self, Footer};
use crate::file::rowgroup::column::Column;
use crate::file::rowgroup::RowGroup;
use crate::file::PlankMeta;
use crate::serde::Deserialize;
use crate::types::{data::PlankData, fields::PlankField, types::PlankType};

pub struct PlankReader {
    file: BufReader<File>,
    meta: PlankMeta,
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

        let meta = PlankMeta { footer };

        Ok(Self { file: br, meta })
    }

    pub fn get_schema(&self) -> &Vec<PlankField> {
        &self.meta.footer.schema()
    }

    // pub fn iter(self) -> RowGroupIterator<'_> {
    //     let offsets = self.meta.footer.offsets().clone();
    //     RowGroupIterator {
    //         reader: self,
    //         offsets,
    //         curr_row_group: None,
    //         index: 0,
    //     }
    // }
}

impl<'a> Iterator for RowGroupIterator<'a> {
    type Item = std::io::Result<RowGroup>;

    fn next(&mut self) -> Option<Self::Item> {
        let br = &mut self.reader.file;
        let meta = &self.reader.meta;

        if self.index as u32 >= meta.footer.row_group_count() {
            return None;
        }

        // Go to the beginning of the row group
        br.seek(SeekFrom::Start(self.offsets[self.index] as u64));

        let mut buf = [0u8; 4];
        br.read_exact(&mut buf);

        let row_group_size = u32::from_le_bytes(buf);

        // Not good, I should instead return a iterator that can read the row group on demand
        let mut buf = vec![0u8; row_group_size as usize];
        br.read(&mut buf);

        self.index += 1;

        Some(RowGroup::from_bytes(&buf, meta.schema()))
    }
}

impl Iterator for RowIterator {
    type Item = std::io::Result<Vec<PlankData>>;

    fn next(&mut self) -> Option<Self::Item> {
        let columns = self.row_group.as_ref()?.columns();

        if self.row >= columns[0].records().len() {
            return None;
        }

        let row: Vec<PlankData> = columns
            .iter()
            .map(|col| col.records()[self.row].clone())
            .collect();

        self.row += 1;
        Some(Ok(row))
    }
}

impl<'a> IntoIterator for &'a mut PlankReader {
    type Item = std::io::Result<RowGroup>;
    type IntoIter = RowGroupIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let offsets = self.meta.footer.offsets().clone();
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
