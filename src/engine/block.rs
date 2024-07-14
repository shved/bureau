#[derive(Debug)]
pub struct Block {
    checksum: Vec<u16>,
    data: Vec<u8>,
    footer: Vec<u8>,
    len: u16,
}

#[derive(Debug)]
struct Key {
    len: u8,
    key: Vec<u8>, // TODO: Or String?
}

#[derive(Debug)]
struct Value {
    len: u16,
    value: Vec<u8>, // TODO: Or String?
}
