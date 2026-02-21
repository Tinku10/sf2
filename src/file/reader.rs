use itertools::Itertools;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::file::footer::{self, Footer};
use crate::file::rowgroup::column::Column;
use crate::file::rowgroup::RowGroup;
use crate::file::SF2Meta;

pub struct SF2Reader {
    file: BufReader<File>,
    meta: SF2Meta,
}

pub struct RowGroupIterator<'a> {
    reader: &'a mut SF2Reader,
    offsets: Vec<u32>,
    index: usize,
}

pub struct RowIterator {
    row_group: Option<RowGroup>,
    row: usize,
}

impl SF2Reader {
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

        // Skip !SCHEMA=
        br.consume(8);
        // Can be interpreted as valid text data
        let mut s = String::new();
        br.read_line(&mut s)?;
        let schema = Self::parse_schema(&s);

        // Skip !ROW_COUNT=
        br.consume(11);
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        let row_count = u32::from_le_bytes(s);
        br.consume(1);

        // Skip !COLUMN_COUNT=
        br.consume(14);
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        br.consume(1);
        let col_count = u32::from_le_bytes(s);

        // Skip !ROWGROUP_COUNT=
        br.consume(16);
        let mut s = [0u8; 4];
        br.read_exact(&mut s)?;
        br.consume(1);
        let row_group_count = u32::from_le_bytes(s);

        // Skip !ROWGROUP_OFFSETS=
        br.consume(18);
        let mut s = vec![0u8; (row_group_count * 4) as usize];
        br.read_exact(&mut s)?;
        br.consume(1);
        let offsets = Self::parse_offsets(&s);

        let footer = Footer::new(schema, offsets, row_count, col_count, row_group_count);
        let meta = SF2Meta { footer };

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

impl<'a> RowGroupIterator<'a> {
    fn parse_rowgroup(&mut self) -> std::io::Result<RowGroup> {
        let br = &mut self.reader.file;
        let meta = &self.reader.meta;
        // Go to the beginning of the row group
        br.seek(SeekFrom::Start(self.offsets[self.index] as u64));
        // Parse col_count lines
        let col_count = meta.footer.col_count() as usize;

        let mut columns = Vec::new();

        for _ in 0..col_count {
            let mut line = String::new();
            br.read_line(&mut line)?;
            let mut v = line.split(',').map(|s| s.to_string()).collect::<Vec<_>>();
            v.pop();
            columns.push(Column::new(v));
        }

        self.index += 1;

        Ok(RowGroup::new(columns))
    }

    // pub fn iter(self) -> RowIterator<'_> {
    //     RowIterator {
    //         row_group: self.curr_row_group.as_ref(),
    //         row: 0,
    //     }
    // }
}

impl<'a> Iterator for RowGroupIterator<'a> {
    type Item = std::io::Result<RowGroup>;

    fn next(&mut self) -> Option<Self::Item> {
        let br = &mut self.reader.file;
        let meta = &self.reader.meta;
        let col_count = meta.footer.col_count() as usize;

        if self.index as u32 == meta.footer.row_group_count() {
            return None;
        }

        // TODO: Find a way to cast the data to the correct types
        // This is not the right place to cast it, but the stored type should be created in a way
        // to support it
        // self.curr_row_group = self.parse_rowgroup().ok();
        // Not what I want, but there seems to be no good way to store the current rowgroup and
        // return a reference to it
        // Some(Ok(self.curr_row_group.clone()?))
        Some(self.parse_rowgroup())
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

impl<'a> IntoIterator for &'a mut SF2Reader {
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
