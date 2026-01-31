use crate::footer::Footer;
use crate::rowgroup::RowGroup;
use crate::serde::Serialize;

use std::fs::File;
use std::io::Write;

pub struct SF2 {
    rowgroups: Vec<RowGroup>,
    footer: Footer,
}

impl SF2 {
    pub fn new(rowgroups: Vec<RowGroup>, footer: Footer) -> Self {
        SF2 { rowgroups, footer }
    }

    pub fn write(&self, file_path: &str) -> std::io::Result<()> {
        println!("writing");
        let mut f = File::create(file_path)?;
        let mut bytes = 0;

        for rg in &self.rowgroups {
            let s = rg.to_string();
            bytes += s.len();
            f.write_all(s.as_bytes())?;
        }

        f.write_all(self.footer.to_string().as_bytes())?;
        write!(f, "!FOOTER={}", bytes);

        Ok(())
    }
}
