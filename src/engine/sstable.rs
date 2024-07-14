use crate::engine::block::Block;

// TODO: Check how it really lays out on disk and play with this number.
// The original idea is that it should be fiting the fs page.
const SSTABLE_BYTESIZE: usize = 64 * 1024;

#[derive(Debug)]
struct SSTable {
    // size: u8,
    blocks: Vec<Block>,
    footer_offset: u16,
    footer: TableMetadata,
}

#[derive(Debug)]
struct TableMetadata {
    index_offset: u16,
    index: Vec<BlockIndex>,
    checksum: Vec<u16>,
}

#[derive(Debug)]
struct BlockIndex {
    first_key: Vec<u8>,
    offset_in_table: u16,
    // len: u16, // TODO: Either in bytes or number of keys in the block.
}
