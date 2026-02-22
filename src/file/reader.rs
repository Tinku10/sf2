use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::file::footer::{self, Footer};
use crate::file::rowgroup::column::Column;
use crate::file::rowgroup::RowGroup;
use crate::file::PlankMeta;
use crate::serde::Deserialize;

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
    fn parse_schema(line: &str) -> Vec<(String, String)> {
        line.trim()
            .split(',')
            .filter_map(|item| {
                let mut it = item.split(':');
                match (it.next(), it.next()) {
                    (Some(col), Some(ty)) => Some((col.to_string(), ty.to_string())),
                    _ => None,
                }
            })
            .collect()
    }

    fn parse_offsets(bytes: &Vec<u8>) -> Vec<u32> {
        let mut v = Vec::new();
        for chunk in bytes.chunks_exact(4) {
            v.push(u32::from_le_bytes(chunk.try_into().unwrap()));
        }
        v
    }

    pub fn open<P: AsRef<Path>>(file_path: P) -> std::io::Result<Self> {
        let mut f = File::open(file_path)?;
        // TODO: Check if file ends in newline

        // Footer offset for u32 including one byte for newline
        f.seek(SeekFrom::End(-5))?;

        let mut footer_offset = [0u8; 4];
        f.read_exact(&mut footer_offset)?;

        let footer_offset = u32::from_le_bytes(footer_offset);

        f.seek(SeekFrom::Start(footer_offset as u64))?;

        let mut br = BufReader::new(f);

        let mut footer_buf = Vec::new();
        br.read_to_end(&mut footer_buf);

        let footer = Footer::from_bytes(&footer_buf)?;

        let meta = PlankMeta { footer };

        Ok(Self { file: br, meta })
    }

    pub fn get_schema(&self) -> &Vec<(String, String)> {
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
        let col_count = meta.footer.col_count() as usize;

        if self.index as u32 >= meta.footer.row_group_count() {
            return None;
        }

        // TODO: Find a way to cast the data to the correct types
        // This is not the right place to cast it, but the stored type should be created in a way
        // to support it
        // self.curr_row_group = self.parse_rowgroup().ok();
        // Not what I want, but there seems to be no good way to store the current rowgroup and
        // return a reference to it
        // Some(Ok(self.curr_row_group.clone()?))
        let br = &mut self.reader.file;
        let meta = &self.reader.meta;
        // Go to the beginning of the row group
        br.seek(SeekFrom::Start(self.offsets[self.index] as u64));
        // Parse col_count lines
        let col_count = meta.footer.col_count() as usize;

        let row_group_size = self.offsets[self.index + 1] - self.offsets[self.index];

        let mut buf = vec![0u8; row_group_size as usize];
        br.read(&mut buf);

        self.index += 1;
        Some(RowGroup::from_bytes(&buf))
    }
}

impl Iterator for RowIterator {
    type Item = std::io::Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        let columns = self.row_group.as_ref()?.columns();

        if self.row >= columns[0].records().len() {
            return None;
        }

        let row: Vec<String> = columns
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
    type Item = std::io::Result<Vec<String>>;
    type IntoIter = RowIterator;

    fn into_iter(self) -> Self::IntoIter {
        RowIterator {
            row_group: Some(self),
            row: 0,
        }
    }
}
