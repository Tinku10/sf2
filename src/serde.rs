pub trait Serialize {
    fn to_bytes(&self) -> std::io::Result<Vec<u8>>;
}

pub trait Deserialize<'a>: Sized {
    type Schema;
    // type Query;
    fn from_bytes(bytes: &[u8], schema: &'a Self::Schema) -> std::io::Result<Self>;
    // fn from_bytes(bytes: &[u8], schema: &'a Vec<PlankField>, query: &RowGroupQuery) -> std::io::Result<Self>;
}
