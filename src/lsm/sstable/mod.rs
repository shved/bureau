pub mod block;
pub mod bloom;

use super::memtable::MemTable;
use crate::Result;
use crate::StorageEntry;
use block::Block;
use bloom::BloomSerializable;
use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};
use uuid::Uuid;

/*
SST layout schema. First section is to be read first to make the initial checks of the table.
-------------------------------------------------------------------------------------------------------------------------
| Bloom filter and Index len |                         Table Index                          |       Blocks Section      |
----------------------------------------------------------------------------------------------------------
|       7717 Bytes + 2B      | Entries num (2B) | Entry #1 | ... | Entry #N | Checksum (4B) | Block #1 | ... | Block #N |
-------------------------------------------------------------------------------------------------------------------------

Table index entry layout.
--------------------------------------------------------------------------
|                              Entry #1                            | ... |
--------------------------------------------------------------------------
| key_len (2B) | first_key | key_len (2B) | last_key | Offset (4B) | ... |
--------------------------------------------------------------------------

Individual block layout is given where Block is defined.
*/

/// Byte size of the first section to read in the table. It is a sum of bloom filter data,
/// a bloom filter checksum and table index byte len so we know how much to read further.
const FIRST_SECTION_LEN: usize = bloom::ENCODED_LEN + std::mem::size_of::<u16>();
const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>(); // 4.

/// Sstable is meant to be used the following way. Typical lifecicle of an instance
/// can be described as a set of calls: build -> encode -> persist and then many lookups.
#[derive(Debug)]
pub struct SsTable {
    blocks: Vec<Block>,
    pub id: Uuid,
    pub bloom: Bloom<Bytes>,
}

impl SsTable {
    pub fn build(src: MemTable) -> Self {
        assert!(src.is_full(), "Flushing a memtable that is not full yet");

        let mut blocks = Vec::new();
        let mut bf = bloom::new();
        let mut cur_block = Block::new();

        for (k, v) in src.map.iter() {
            if !cur_block.add(k.clone(), v.clone()) {
                blocks.push(cur_block); // Block is full. Put it to the blocks vector.
                cur_block = Block::new(); // Set the cursor to be a new block.
                cur_block.add(k.clone(), v.clone()); // Put the value to a new block.
            }

            bf.set(k);
        }

        Self {
            id: Self::generate_id(),
            blocks,
            bloom: bf,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut offset = 0;
        let mut blocks_encoded = Vec::<u8>::new();
        let mut index = TableIndex::new();
        for block in self.blocks.as_slice() {
            index.0.push(IndexEntry::new(
                offset,
                block.first_key.clone(),
                block.last_key.clone(),
            ));

            let block_encoded = block.encode();
            offset += block_encoded.len() as u32;
            blocks_encoded.extend(block_encoded);
        }

        let mut content = self.bloom.encode();
        content.extend(index.encode());
        content.extend(blocks_encoded);

        content
    }

    /// Generates a simple and time ordered uuid (v7).
    fn generate_id() -> Uuid {
        Uuid::now_v7()
    }

    pub fn lookup(blob: &impl StorageEntry, key: &Bytes) -> Result<Option<Bytes>> {
        if let (true, index_len) = Self::probe_bloom(blob, key)? {
            if let Some(offset) = Self::lookup_index(blob, index_len as usize, key)? {
                let block = Self::read_block(blob, offset)?;
                return Ok(block.get(key.clone()));
            }
        }

        Ok(None)
    }

    /// Reads the bloom filter and a couple extra bytes from the table index
    /// to get the table index len for the next call if it will be necessary.
    fn probe_bloom(blob: &impl StorageEntry, key: &Bytes) -> Result<(bool, u16)> {
        let mut data = vec![0; FIRST_SECTION_LEN];
        blob.read_at(&mut data, 0)?;

        let mut index_len_bytes: [u8; 2] = [0, 0];
        index_len_bytes.copy_from_slice(&data[bloom::ENCODED_LEN..]);
        let index_len = u16::from_be_bytes(index_len_bytes);
        let b = Bloom::decode(&data[..std::mem::size_of::<u16>()]); // BUG: panic_const_sub_overflow

        Ok((b.check(key), index_len))
    }

    fn lookup_index(blob: &impl StorageEntry, len: usize, key: &Bytes) -> Result<Option<u32>> {
        let mut data = vec![0; len];
        blob.read_at(&mut data, FIRST_SECTION_LEN as u64)?;

        let index = TableIndex::decode(&data);
        let entry = index
            .0
            .into_iter()
            .find(|e| e.first_key < key && e.last_key > key);
        match entry {
            Some(IndexEntry { offset, .. }) => Ok(Some(offset)),
            None => Ok(None),
        }
    }

    fn read_block(blob: &impl StorageEntry, offset: u32) -> Result<Block> {
        let mut data = vec![0; block::BLOCK_BYTESIZE];
        blob.read_at(&mut data, offset as u64)?;

        Ok(Block::decode(&data))
    }
}

#[derive(Debug)]
struct IndexEntry {
    /// Offset of a data block.
    pub offset: u32,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

impl IndexEntry {
    fn new(offset: u32, first_key: Bytes, last_key: Bytes) -> Self {
        IndexEntry {
            offset,
            first_key,
            last_key,
        }
    }
}

#[derive(Debug)]
struct TableIndex(Vec<IndexEntry>);

impl TableIndex {
    fn new() -> Self {
        TableIndex(Vec::new())
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.put_u16(0); // Reserve it for the whole index bytelen added at the end of encoding.

        let entries_num = self.0.len();
        assert!(entries_num != 0, "Attempt to endcode an empty table index");

        buf.put_u16(entries_num as u16);

        for entry in self.0.as_slice() {
            buf.put_u16(entry.first_key.len() as u16);
            buf.put_slice(entry.first_key.as_ref());
            buf.put_u16(entry.last_key.len() as u16);
            buf.put_slice(entry.last_key.as_ref());
            buf.put_u32(entry.offset);
        }

        let checksum = crc32fast::hash(&buf[..]);
        buf.put_u32(checksum);

        let index_len: [u8; 2] = (buf.len() as u16).to_be_bytes();
        buf[0] = index_len[0];
        buf[1] = index_len[1];

        buf
    }

    /// u16 reserved for byte len of a table index sector in the process of encoding
    /// should not be included in the slice passed. Only valuable data is expected here.
    /// Index bytelen will be read separately along with the bloom filter.
    fn decode(mut raw: &[u8]) -> Self {
        let checksum = crc32fast::hash(&raw[..raw.remaining() - CHECKSUM_SIZE]);
        let mut table_index = TableIndex::new();
        let entries_num = raw.get_u32() as usize;
        for _ in 0..entries_num {
            let first_key_len = raw.get_u16() as usize;
            let first_key = raw.copy_to_bytes(first_key_len);
            let last_key_len: usize = raw.get_u16() as usize;
            let last_key = raw.copy_to_bytes(last_key_len);
            let offset = raw.get_u32();
            table_index.0.push(IndexEntry {
                offset,
                first_key,
                last_key,
            });
        }

        assert!(
            raw.get_u32() == checksum,
            "Checksum mismatch in table index decode"
        );

        table_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm::memtable::{MemTable, ProbeResult};
    use bytes::Bytes;

    #[test]
    fn test_build() {
        let mt = create_full_memtable();
        let built = SsTable::build(mt);
        assert_eq!(built.blocks.len(), 16);
    }

    fn create_full_memtable() -> MemTable {
        let mut mt = MemTable::new(None);
        loop {
            // Fill it with random simple uuids.
            let key = Bytes::from(Uuid::now_v7().to_string());
            let value = Bytes::from(Uuid::now_v7().to_string());
            match mt.probe(&key, &value) {
                ProbeResult::Available(new_size) => {
                    mt.insert(key, value, Some(new_size));
                }
                ProbeResult::Full => {
                    return mt;
                }
            }
        }
    }

    // #[test]
    fn test_encode_decode_table_index() {}
}
