mod file;
mod footer;
mod rowgroup;
mod serde;
mod util;

use crate::file::SF2;
use crate::rowgroup::column::Column;
use crate::rowgroup::RowGroup;
use crate::serde::Serialize;

fn main() {
    let rg = vec![RowGroup::new(
        vec![
            Column::new(vec![
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string(),
            ]),
            Column::new(vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
            ]),
        ],
    )];
    let f = footer::Footer::new(
        vec![
            ("Firstname".to_string(), "str".to_string()),
            ("Lastname".to_string(), "str".to_string()),
        ],
        vec![10, 20, 30, 40, 50, 60],
        10,
        2,
    );

    util::csv_to_sf2("./data/addresses.csv").unwrap().write("./data/test.sf2").unwrap();

    // SF2::new(rg, f).write("./data/test.sf2");

    // println!("{}", f.to_string());
}
