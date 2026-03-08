# Plank

A simple and efficient binary file format for structured tabular data. Plank stores data in a columnar layout for fast metadata access.

---

## Format Specification

Plank files are organized into two sections: **row groups**, and a **footer**.

### Layout

```
[row group 1 size]
    [column 1 size]
        [data type id][data size]?[data]
    [column 2 size]
    ...
    [column n size]
[row group 2]
...
[row group n]
[schema size]
    [field 1 name][data type id]
    ...
    [field n name][data type id]
[row count size: 4 bytes][u32]
[column count size: 4 bytes][u32]
[row group count size: 4 bytes][u32]
[offset size]
    [u32]..[u32]
[sha256 checksum]
[footer offset: 4 bytes]
```

### Row Groups

A row group is a fixed-size chunk of rows. Each line in a row group represents one column, with all values for that column in the current chunk listed comma-separated.

```
Jack,Emily,
Johnson,Clark,
28,34,
New York,London,
```

The above encodes two rows across four columns (`first_name`, `last_name`, `age`, `city`). The example uses comma-separated values for visualization. The actual values are binary-encoded.

### Footer

The footer contains complete file metadata and is located at the end of the file. The footer offset (a little-endian `u32`) is stored in the last 5 bytes of the file (4 bytes + newline), allowing readers to seek directly to the footer without scanning the file.

---

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

## File Extension

`.plank`
