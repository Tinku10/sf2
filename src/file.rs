pub(crate) mod footer;
pub mod reader;
pub(crate) mod rowgroup;
pub mod writer;

use footer::Footer;

#[derive(Debug)]
pub struct PlankMeta {
    footer: Footer,
}

impl PlankMeta {
    pub fn schema(&self) -> &Vec<(String, String)> {
        self.footer.schema()
    }

    pub fn col_count(&self) -> u32 {
        self.footer.col_count()
    }

    pub fn row_count(&self) -> u32 {
        self.footer.row_count()
    }
}
