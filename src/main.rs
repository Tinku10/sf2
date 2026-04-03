mod file;
// mod query;
mod serde;
mod types;

mod bindings;

use crate::file::reader::PlankReader;
use crate::file::writer::PlankWriter;
use crate::types::data::PlankData;

fn main() {
    {
        let mut f = PlankWriter::new("./data/100000.plank").unwrap();
        f.write_from_csv("./data/100000.csv").unwrap();
    }

    let mut f = PlankReader::open("./data/100000.plank").unwrap();

    let result = f.read_row_group(0).unwrap();
    print!("{:#?}, ", result.schema[1]);
    print!("{:#?}, ", result.columns[1]);
    print!("{:#?}, ", f.footer())
}
