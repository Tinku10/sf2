# Plank

A simple and efficient binary file format for structured tabular data. Plank stores data in a columnar layout for fast metadata access.

---

## Format Specification

Plank files are organized into two sections: **row groups**, and a **footer**.

### Layout

```
[row group-1 size]
    [column-1 size]
        [data size]?[data]
    [column-2]
    ...
    [column-n]
[row group-2]
...
[row group-n]
[schema size]
    [field-1 name size][field-1 name][field-1 type]
    [field-2]
    ...
    [field-n]
[row count size: 4 bytes][u32]
[column count size: 4 bytes][u32]
[row group count size: 4 bytes][u32]
[offset size]
    [row group-1 offset: 4 bytes]..[row group-n offset]
[sha256 checksum]
[footer offset: 4 bytes]
```

### Row Groups

A row group is a fixed-size chunk of rows. Each line in a row group represents one column.

```
Jack,Emily,
Johnson,Clark,
28,34,
New York,London,
```

The above encodes two rows across four columns (`first_name`, `last_name`, `age`, `city`). The example uses comma-separated values for visualization. The actual values are binary-encoded.

### Footer

The footer contains complete file metadata and is located at the end of the file. The footer offset (a little-endian `u32`) is stored in the last 4 bytes of the file, allowing readers to seek directly to the footer without scanning the file.

---

### Data Types

The following types are supported yet.

- `Str`: Variable size text
- `Int32`
- `Int64`
- `Bool`
- `Struct`: Supports fields of any of the supported types
- `List`: A homogeneous list of items (homogeneity is not yet enforced)

## Usage

### Reading all rows

```rust
let mut f = PlankReader::open("/path/to/file.plank")?;

for rg in &mut f {
    if let Ok(rg) = rg {
        for row in rg {
            println!("{:?}", row);
        }
    }
}
```

### Converting a CSV

```rust
let mut f = PlankWriter::new("/path/to/file.plank")?;
f.write_from_csv("/path/to/file.csv")?;
```

---

### Reading using the new Query API

```rust


let mut f = PlankReader::open("./data/100.plank").unwrap();
let query = PlankQuery::new()
    .filter(Cmp::Eq(QueryKey::RowGroup, PlankData::Int32(0)));

let data = query::run_query(&mut f, &query).unwrap();
for row in data {
    println!("{:?}", row);
}
```

## Possible Improvements

- Entire row group is read into memory per call currently
- Lists are not checked for homogeneity and cannot recognize the type in some scenarios
- A column in a row group contains the full column irrespective of the byte size (maybe good, maybe not)
- Row groups are divided into fixed number of collection of rows and cannot be configured (no metadata of this is kept in the footer)

## File Extension

`.plank`
