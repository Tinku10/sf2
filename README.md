## Simple File Format (SF2)

A very simple file format for structured data

## Format Specification

```
Jack,Emily,
Johnson,Clark,
28,34,
New York,London,
Aarav,Sophia,
Sharma,Miller,
22,29,
Delhi,Berlin,
Liam,
O'Connor,
41,
Dublin,
!SCHEMA=first_name:str,last_name:str,age:str,city:str,
!ROWGROUP_OFFSETS=<binary: vector<u32>>
!ROW_COUNT=<binary: u32>
!COLUMN_COUNT=<binary: u32>
!ROWGROUP_COUNT=<binary: u32>
!FOOTER_OFFSET=<binary: u32>
```

The example above is organized into a few sections:

1. Row Groups:
    It is a collection of a fixed chunk of rows with the columns from each row in one line.

2. Column:
    A column contains all the enties in that column in a line for the row group.

3. Footer:
    A footer contains the complete metadata. It contains the table headers along with offsets to each row group along with the offsets to each row in the rowgroup.

It is not a complete text format. Numbers are encoded in binary.

## Example

### Reading all the rows

```rs

let mut f = SF2Reader::open("/path/to/file.sf2")?;

for rg in &mut f {
    if let Ok(rg) = rg {
        for row in rg {
            println!("{:?}", row);
        }
    }
}

```

### Converting a CSV into SF2

```rs

let mut f = SF2Writer::new("/path/to/file.sf2")?;
f.write_from_csv("/path/to/file.csv")?;

```
