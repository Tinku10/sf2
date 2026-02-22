# Plank

A simple and efficient file format for structured tabular data. Plank stores data in a columnar layout with a binary-encoded footer for fast metadata access.

---

## Format Specification

Plank files are organized into two sections: **row groups**, and a **footer**.

### Layout

```
<row group 0>
<row group 1>
...
<row group N>
<footer>
```

### Row Groups

A row group is a fixed-size chunk of rows. Each line in a row group represents one column, with all values for that column in the current chunk listed comma-separated.

```
Jack,Emily,
Johnson,Clark,
28,34,
New York,London,
```

The above encodes two rows across four columns (`first_name`, `last_name`, `age`, `city`).

### Footer

The footer contains complete file metadata and is located at the end of the file. The footer offset (a little-endian `u32`) is stored in the last 5 bytes of the file (4 bytes + newline), allowing readers to seek directly to the footer without scanning the file.

```
!SCHEMA=first_name:str|last_name:str|age:str|city:str
!ROW_COUNT=<binary: u32 LE>
!COLUMN_COUNT=<binary: u32 LE>
!ROWGROUP_COUNT=<binary: u32 LE>
!ROWGROUP_OFFSETS=<binary: u32[] LE>
!FOOTER_OFFSET=<binary: u32>
```

> **Note:** Plank is not a pure text format. All numeric fields in the footer are encoded as little-endian binary integers. Only `!SCHEMA` is plain text.

### Field Reference

| Field | Type | Description |
|---|---|---|
| `!SCHEMA` | text | Column names and types, comma-separated |
| `!ROW_COUNT` | `u32` LE | Number of rows in the file |
| `!COLUMN_COUNT` | `u32` LE | Number of columns in the file |
| `!ROWGROUP_COUNT` | `u32` LE | Number of row groups |
| `!ROWGROUP_OFFSETS` | `u32[]` LE | Byte offset of each row group from the start of the file |
| `!FOOTER_OFFSET` | `u32` LE | Byte offset of the begining of the footer section |

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
