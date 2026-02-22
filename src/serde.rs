pub trait Serialize {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Deserialize: Sized {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Self>;
}
