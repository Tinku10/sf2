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
    row_count: u32,
    row_group: usize,
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

    fn parse_offsets(line: &str) -> Vec<u32> {
        let mut v = Vec::new();
        for chunk in line.as_bytes().chunks_exact(5) {
            v.push(u32::from_le_bytes(chunk[..4].try_into().unwrap()));
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

        f.seek(SeekFrom::Start(footer_offset as u64));

        let mut br = BufReader::new(f);

        let mut s = String::new();

        br.read_line(&mut s);
        let schema = s
            .strip_prefix("!SCHEMA=")
            .ok_or_else(|| std::io::ErrorKind::InvalidData)?
            .to_string();

        let schema = Self::parse_schema(&schema);
        s.clear();

        br.read_line(&mut s);
        let offsets = s
            .strip_prefix("!ROWGROUP_OFFSETS=")
            .ok_or_else(|| std::io::ErrorKind::InvalidData)?
            .to_string();

        let offsets = Self::parse_offsets(&offsets);
        s.clear();


        br.read_line(&mut s);

        let row_count = u32::from_le_bytes(
            s.strip_prefix("!ROW_COUNT=")
                .ok_or_else(|| std::io::ErrorKind::InvalidData)?
                .trim()
                .as_bytes()[..4]
                .try_into()
                .unwrap(),
        );
        s.clear();

        br.read_line(&mut s);
        let col_count = u32::from_le_bytes(
            s.strip_prefix("!COLUMN_COUNT=")
                .ok_or_else(|| std::io::ErrorKind::InvalidData)?
                .trim()
                .as_bytes()[..4]
                .try_into()
                .unwrap(),
        );
        s.clear();

        br.read_line(&mut s);
        let row_group_count = u32::from_le_bytes(
            s.strip_prefix("!ROWGROUP_COUNT=")
                .ok_or_else(|| std::io::ErrorKind::InvalidData)?
                .trim()
                .as_bytes()[..4]
                .try_into()
                .unwrap(),
        );


        let footer = Footer::new(schema, offsets, row_count, col_count, row_group_count);
        let meta = SF2Meta { footer };

        Ok(Self { file: br, meta })
    }

    pub fn get_schema(&self) -> &Vec<(String, String)> {
        &self.meta.footer.schema()
    }

    // pub fn head(&mut self, rows: Option<u32>) -> Option<Vec<Vec<String>>> {
    //     // Read up to first two rows
    //     let col_count = self.meta.footer.col_count() as usize;
    //     let mut offsets = self.meta.footer.offsets().clone();

    //     let mut rg = 0;

    //     let row_count = match rows {
    //         Some(x) => std::cmp::min(x, self.meta.row_count()),
    //         None => std::cmp::min(2, self.meta.row_count()),
    //     } as usize;

    //     // TODO: Find a way to cast the data to the correct types
    //     // This is not the right place to cast it, but the stored type should be created in a way
    //     // to support it
    //     let mut result = vec![Vec::new(); row_count];

    //     for j in 0..row_count {
    //         for i in 0..col_count {
    //             let idx: usize = (rg * col_count + i) as usize;
    //             let offset = offsets[idx];
    //             self.file.seek(SeekFrom::Start(offset.into()));

    //             let mut item = Vec::new();
    //             let bytes = self.file.read_until(b',', &mut item).ok()?;
    //             item = item.strip_suffix(&[b',']).unwrap_or(&item).to_vec();

    //             result[j].push(String::from_utf8(item).ok()?);

    //             offsets[idx] += bytes as u32;
    //         }

    //         if self.file.peek(1).unwrap() == b"\n" {
    //             rg += 1;
    //         }
    //     }
    //     Some(result)
    // }

    pub fn iter(&mut self) -> RowGroupIterator {
        let offsets = self.meta.footer.offsets().clone();
        RowGroupIterator {
            reader: self,
            offsets,
            row_count: 0,
            row_group: 0,
        }
    }
}

impl<'a> RowGroupIterator<'a> {
    fn parse_rowgroup(&mut self) -> std::io::Result<RowGroup> {
        let br = &mut self.reader.file;
        let meta = &self.reader.meta;
        // Go to the beginning of the row group
        br.seek(SeekFrom::Start(self.offsets[self.row_group] as u64));
        // Parse col_count lines
        let col_count = meta.footer.col_count() as usize;

        let mut columns = Vec::new();

        for i in 0..col_count {
            let mut line = String::new();
            br.read_line(&mut line)?;
            let mut v = line.split(',').map(|s| s.to_string()).collect::<Vec<_>>();
            v.pop();
            columns.push(Column::new(v));
        }

        self.row_group += 1;

        Ok(RowGroup::new(columns))
    }
}

impl<'a> Iterator for RowGroupIterator<'a> {
    type Item = std::io::Result<RowGroup>;

    fn next(&mut self) -> Option<Self::Item> {
        let br = &mut self.reader.file;
        let meta = &self.reader.meta;
        let col_count = meta.footer.col_count() as usize;

        if self.row_group as u32 == meta.footer.row_group_count() {
            return None;
        }

        // TODO: Find a way to cast the data to the correct types
        // This is not the right place to cast it, but the stored type should be created in a way
        // to support it
        // let mut result = Vec::with_capacity(col_count);
        // let mut result = vec![Vec::with_capacity(footer::ROWGROUP_SIZE); col_count];

        Some(self.parse_rowgroup())
        // for i in 0..col_count {
        //     let idx: usize = (self.row_group * col_count + i) as usize;
        //     let offset = self.offsets[idx];
        //     br.seek(SeekFrom::Start(offset.into()));

        //     let mut item = Vec::new();
        //     let bytes = br.read_until(b',', &mut item).ok()?;
        //     item = item.strip_suffix(&[b',']).unwrap_or(&item).to_vec();

        //     result.push(String::from_utf8(item).ok()?);

        //     self.offsets[idx] += bytes as u32;
        //     // offsets[idx] += bytes as u32;
        // }

        // if br.peek(1).unwrap() == b"\n" {
        //     self.row_group += 1;
        // }

        // self.row_count += 1;
    }
}
