#![feature(bufreader_peek)]
#![feature(iter_array_chunks)]

mod file;
mod serde;

use crate::file::reader::SF2Reader;
use crate::file::writer::SF2Writer;

fn main() {
    let mut f = SF2Reader::open("./data/100.sf2").unwrap();

    for rg in &mut f {
        if let Ok(rg) = rg {
            for row in rg {
                println!("{:?}", row);
            }
        }
    }

    // let mut f = SF2Writer::new("./data/100.sf2").unwrap();
    // f.write_from_csv("./data/100.csv").unwrap();
}
