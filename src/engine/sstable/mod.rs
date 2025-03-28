pub mod block;
pub mod bloom;

use crate::engine::memtable::MemTable;
use crate::Result;
use crate::StorageEntry;
use block::Block;
use bloom::BloomSerializable;
use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};
use std::collections::BTreeMap;
use std::io::Cursor;
use uuid::Uuid;

/*
SST layout schema. First section is to be read first to make the initial checks of the table.
----------------------------------------------------------------------------------------------------------------------------
| Bloom filter |                                Table Index                                    |       Blocks Section      |
----------------------------------------------------------------------------------------------------------------------------
|  7718 Bytes  | Index len (2B) | Entries num (2B) | Entry #1 | ... | Entry #N | Checksum (4B) | Block #1 | ... | Block #N |
----------------------------------------------------------------------------------------------------------------------------

Table index entry layout.
--------------------------------------------------------------------------
|                              Entry #1                            | ... |
--------------------------------------------------------------------------
| key_len (2B) | first_key | key_len (2B) | last_key | Offset (4B) | ... |
--------------------------------------------------------------------------

Individual block layout is given where Block is defined.
*/

/// Byte size of the first section to read in the table. It is a sum of encoded bloom filter
/// data and table index byte len so we know how much to read in the next step if needed.
const FIRST_READ_LEN: usize = bloom::ENCODED_LEN + std::mem::size_of::<u16>();
const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>(); // 4.

/// SsTable is meant to be used the following way. Typical lifecicle of an instance
/// can be described as a set of calls: build_full -> encode -> persist and then many lookups.
#[derive(Debug)]
pub struct SsTable {
    blocks: Vec<Block>,
    pub id: Uuid,
    pub bloom: Bloom<Bytes>,
}

impl SsTable {
    /// This function is only used to build new tables before saving them to disk.
    pub fn build_full(src: MemTable) -> Self {
        assert!(src.is_full(), "flushing memtable that is not full yet");
        Self::build(src)
    }

    /// Builds an SsTable structure to encode and persist it.
    pub fn build(src: MemTable) -> Self {
        let mut blocks = Vec::new();
        let mut bf = bloom::new();
        let mut cur_block = Block::new();

        for (k, v) in src.map.iter() {
            if !cur_block.add(k.clone(), v.clone()) {
                blocks.push(cur_block); // Block is full. Put it to the blocks vector.
                cur_block = Block::new(); // Replace current block with an empty one.
                cur_block.add(k.clone(), v.clone()); // Put the value to a new block.
            }

            bf.set(k);
        }

        blocks.push(cur_block); // Finalize with the last block to add.

        Self {
            id: Self::generate_id(),
            blocks,
            bloom: bf,
        }
    }

    /// Makes a table into a vector of bytes.
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

    /// Makes a map from the given encoded table. Used in compaction to shrink duplicated data.
    /// There is no need for MemTable to be returned since the only reason having map wrapped
    /// by memtable is to track it's size. Since we only use decoded sstable to deduplicate data
    /// what we actually want is an underlying map structure.
    pub fn decode(blob: &mut impl StorageEntry) -> Result<BTreeMap<Bytes, Bytes>> {
        let mut data = Vec::new();
        blob.read_all(&mut data)?;
        let index_len =
            u16::from_be_bytes([data[bloom::ENCODED_LEN], data[bloom::ENCODED_LEN + 1]]);
        let index_start = bloom::ENCODED_LEN;
        let index_end = bloom::ENCODED_LEN + index_len as usize;
        let index = TableIndex::decode(&data[index_start..index_end]);

        let mut map = BTreeMap::new();

        for ie in index.0 {
            let block_start = index_end + ie.offset as usize;
            let block = Block::decode(&data[block_start..block_start + block::BLOCK_BYTE_SIZE]);
            for offset in block.offsets.iter() {
                let key = block.parse_frame(*offset as usize);
                let value = block.parse_frame(*offset as usize + 2 + key.len());
                map.insert(key, value);
            }
        }

        Ok(map)
    }

    /// Generates a simple and time ordered uuid (v7).
    fn generate_id() -> Uuid {
        Uuid::now_v7()
    }

    /// Touches table to find if the given key is in the table. First checks for bloom filter,
    /// then index and then reads block of data needed. If key not found, returns None.
    pub fn lookup(blob: &impl StorageEntry, key: &Bytes) -> Result<Option<Bytes>> {
        if let (true, index_len) = Self::probe_bloom(blob, key)? {
            if let Some(offset) = Self::lookup_index(blob, index_len as usize, key)? {
                let block = Self::read_block(blob, index_len, offset)?;
                return Ok(block.get(key.clone()));
            }
        }

        Ok(None)
    }

    /// Reads the bloom filter and a couple extra bytes from the table index to get the table
    /// index len for the next call if it will be necessary. Reading index len in advance is made
    /// to avoid extra read from disk on the next step.
    fn probe_bloom(blob: &impl StorageEntry, key: &Bytes) -> Result<(bool, u16)> {
        let mut data = vec![0; FIRST_READ_LEN];
        blob.read_at(&mut data, 0)?;

        let mut index_len_bytes: [u8; 2] = [0, 0];
        index_len_bytes.copy_from_slice(&data[bloom::ENCODED_LEN..]);
        let index_len = u16::from_be_bytes(index_len_bytes);
        let b = Bloom::decode(&data[..bloom::ENCODED_LEN]);

        Ok((b.check(key), index_len))
    }

    /// Checks table index for the key to find. If the key won't fall into any index section
    /// Returns None.
    fn lookup_index(blob: &impl StorageEntry, len: usize, key: &Bytes) -> Result<Option<u32>> {
        let mut data = vec![0; len];
        blob.read_at(&mut data, bloom::ENCODED_LEN as u64)?;

        // TODO: Could be optimised so that offset will be returned immediately when it is found.
        // Wont add much to performance though.
        let index = TableIndex::decode(&data);
        let entry = index
            .0
            .into_iter()
            .find(|e| e.first_key <= key && e.last_key >= key);
        match entry {
            Some(IndexEntry { offset, .. }) => Ok(Some(offset)),
            None => Ok(None),
        }
    }

    /// Reads exact block of data, decodes it and returns decoded struct.
    fn read_block(blob: &impl StorageEntry, index_len: u16, offset: u32) -> Result<Block> {
        let mut data = vec![0; block::BLOCK_BYTE_SIZE];
        // Offsets are being set in index relative to Data Section start, so to get offset
        // relative to the whole blob start we need to sum up bloom filter length and index length.
        let offset = index_len as u32 + bloom::ENCODED_LEN as u32 + offset;
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
        assert_ne!(entries_num, 0, "Attempt to endcode an empty table index");

        buf.put_u16(entries_num as u16);

        for entry in self.0.as_slice() {
            buf.put_u16(entry.first_key.len() as u16);
            buf.put_slice(entry.first_key.as_ref());
            buf.put_u16(entry.last_key.len() as u16);
            buf.put_slice(entry.last_key.as_ref());
            buf.put_u32(entry.offset);
        }

        let index_len = buf.len() + CHECKSUM_SIZE;
        let index_len_bytes: [u8; 2] = (index_len as u16).to_be_bytes();
        buf[0] = index_len_bytes[0];
        buf[1] = index_len_bytes[1];

        let checksum = crc32fast::hash(&buf[..]);
        buf.put_u32(checksum);

        buf
    }

    fn decode(raw: &[u8]) -> Self {
        let mut buf = Cursor::new(raw);
        let checksum = crc32fast::hash(&raw[..buf.remaining() - CHECKSUM_SIZE]);

        let encoded_len = buf.get_u16();
        assert_eq!(
            encoded_len as usize,
            raw.len(),
            "Blob len encoded {}, but {} was passed",
            encoded_len,
            raw.len()
        );

        let mut table_index = TableIndex::new();
        let entries_num = buf.get_u16() as usize;
        for _ in 0..entries_num {
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            let offset = buf.get_u32();
            table_index.0.push(IndexEntry {
                offset,
                first_key,
                last_key,
            });
        }

        assert_eq!(
            buf.get_u32(),
            checksum,
            "Checksum mismatch in table index decode"
        );

        table_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::memtable::{MemTable, ProbeResult, SsTableSize};
    use crate::storage::mem;
    use crate::Storage;
    use bytes::Bytes;
    use rand::seq::IndexedRandom;
    use tracing::debug;
    use tracing_test::traced_test;

    /// Generates a full memtable that is filled with UUIDs as keys and values. Returns
    /// a memtable and a random pair of key and value present in the table.
    fn create_full_memtable(size: SsTableSize) -> (MemTable, Bytes, Bytes) {
        let mut mt = MemTable::new(size, None);
        loop {
            // Fill it with random simple uuids.
            let key = Bytes::from(Uuid::now_v7().to_string());
            let value = Bytes::from(Uuid::now_v7().to_string());
            match mt.probe(&key, &value) {
                ProbeResult::Available(new_size) => {
                    mt.insert(key, value, Some(new_size));
                }
                ProbeResult::Full => {
                    let internal_map = mt.map.clone();
                    let keys = internal_map.keys().cloned().collect::<Vec<Bytes>>();
                    let key = keys.choose(&mut rand::rng()).unwrap();
                    let value = internal_map.get(key);
                    let value = value.unwrap();

                    return (mt, key.clone(), value.clone());
                }
            }
        }
    }

    fn create_shrinked_table() -> MemTable {
        let mut mt = MemTable::new(SsTableSize::Default, None);
        mt.insert(Bytes::from("Fyodor"), Bytes::from("Dostoevsky"), None);
        mt.insert(Bytes::from("Jerome"), Bytes::from("Salinger"), None);
        mt.insert(Bytes::from("Leo"), Bytes::from("Tolstoy"), None);
        mt.insert(Bytes::from("William"), Bytes::from("Shakespeare"), None);
        mt.insert(Bytes::from("Jorge"), Bytes::from("Borges"), None);
        mt.insert(Bytes::from("Vladimir"), Bytes::from("Nabokov"), None);
        mt.insert(Bytes::from("Walt"), Bytes::from("Whitman"), None);
        mt
    }

    #[test]
    fn test_build() {
        let (mt, _, _) = create_full_memtable(SsTableSize::Default);
        let built = SsTable::build(mt);

        // TODO: Not the best assertion since number of blocks is not guaranteed to be the same all the time.
        // Test could potentially be flacky.
        assert_eq!(built.blocks.len(), 16);

        let mt = create_shrinked_table();
        let built = SsTable::build(mt);
        assert_eq!(built.blocks.len(), 1);
    }

    #[test]
    fn test_lookup_shrinked_table() {
        let mt = create_shrinked_table();
        let built = SsTable::build(mt);
        let encoded = built.encode();

        let stor = mem::new();
        let write = stor.write(&built.id, encoded.as_ref());
        assert!(
            write.is_ok(),
            "persisting a table err: {:?}",
            write.err().unwrap()
        );

        let open = stor.open(&built.id);
        assert!(open.is_ok(), "opening blob err: {:?}", open.err().unwrap());

        let blob = open.unwrap();
        let res = SsTable::lookup(&blob, &Bytes::from("Fyodor"));
        assert!(res.is_ok(), "lookup err: {:?}", res.err().unwrap());
        let res = res.unwrap();
        assert!(res.is_some());

        let mut mt = MemTable::new(SsTableSize::Default, None);
        mt.insert(Bytes::from("Fyodor"), Bytes::from("Dostoevsky"), None);
        let built = SsTable::build(mt);
        let encoded = built.encode();

        let stor = mem::new();
        let write = stor.write(&built.id, encoded.as_ref());
        assert!(
            write.is_ok(),
            "persisting a table err: {:?}",
            write.err().unwrap()
        );

        let open = stor.open(&built.id);
        assert!(open.is_ok(), "opening blob err: {:?}", open.err().unwrap());

        let blob = open.unwrap();
        let res = SsTable::lookup(&blob, &Bytes::from("Fyodor"));
        assert!(res.is_ok(), "lookup err: {:?}", res.err().unwrap());
        let res = res.unwrap();
        assert!(res.is_some());
        let res = SsTable::lookup(&blob, &Bytes::from("Jesus"));
        assert!(res.is_ok(), "lookup err: {:?}", res.err().unwrap());
        let res = res.unwrap();
        assert!(res.is_none());
    }

    #[test]
    #[should_panic]
    fn test_build_full_should_panic() {
        let mt = create_shrinked_table();
        let _ = SsTable::build_full(mt);
    }

    #[test]
    fn test_decode() {
        let (mt, key, value) = create_full_memtable(SsTableSize::Is(4 * 1024));
        let built = SsTable::build_full(mt.clone());
        let mut encoded = built.encode();
        let decoded = SsTable::decode(&mut encoded);
        assert!(decoded.is_ok());
        let map = decoded.unwrap();
        assert!(map.contains_key(&key));
        let got = map.get(&key);
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(got, &value);

        let mt = create_shrinked_table();
        let built = SsTable::build(mt.clone());
        let mut encoded = built.encode();
        let decoded = SsTable::decode(&mut encoded);
        assert!(decoded.is_ok());
        let map = decoded.unwrap();
        assert!(map.contains_key(&Bytes::from("Fyodor")));
        let got = map.get(&Bytes::from("Fyodor"));
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(got, &Bytes::from("Dostoevsky"));
    }

    #[test]
    fn test_lookup() {
        let (mt, _, _) = create_full_memtable(SsTableSize::Is(8 * 1024));
        let built = SsTable::build_full(mt.clone());
        let encoded = built.encode();

        let stor = mem::new();
        let write = stor.write(&built.id, encoded.as_ref());
        assert!(
            write.is_ok(),
            "persisting a table err: {:?}",
            write.err().unwrap()
        );

        let open = stor.open(&built.id);
        assert!(open.is_ok(), "opening blob err: {:?}", open.err().unwrap());

        let blob = open.unwrap();

        for key in mt.keys() {
            let res = SsTable::lookup(&blob, &Bytes::from(key));
            assert!(res.is_ok(), "lookup err: {:?}", res.err().unwrap());
            let res = res.unwrap();
            assert!(res.is_some());
        }
    }

    #[traced_test]
    #[test]
    fn test_probe_bloom_and_lookup_index() {
        let (mt, key, _) = create_full_memtable(SsTableSize::Is(8 * 1024));
        let built = SsTable::build_full(mt.clone());
        let encoded = built.encode();

        let res = SsTable::probe_bloom(&encoded, &key);
        assert!(res.is_ok(), "probe bloom err: {:?}", res.err().unwrap());
        let res = res.unwrap();
        assert!(res.0);
        assert_eq!(res.1, 168);

        let res = SsTable::lookup_index(&encoded, 168, &key);
        assert!(res.is_ok(), "lookup index err: {:?}", res.err().unwrap());

        let res = res.unwrap();
        // DEBUG
        if res.is_none() {
            debug!("key to find: {:?}", key);

            // Memtable
            debug!("memtable keys: {:?}", mt.keys());

            // Index
            let index_len = 168;
            let mut index_data = vec![0; index_len];
            encoded
                .read_at(&mut index_data, bloom::ENCODED_LEN as u64)
                .unwrap();
            let index = TableIndex::decode(&index_data);
            debug!("index: {:?}", index);

            // Blocks
            let block_len = 4096;
            let mut block_1_data = vec![0; block_len];
            encoded
                .read_at(&mut block_1_data, (bloom::ENCODED_LEN + index_len) as u64)
                .unwrap();
            let block_1 = Block::decode(&block_1_data);
            let mut block_2_data = vec![0; block_len];
            encoded
                .read_at(
                    &mut block_2_data,
                    (bloom::ENCODED_LEN + index_len + block_len) as u64,
                )
                .unwrap();
            let block_2 = Block::decode(&block_2_data);
            debug!("blocks: {}, {}", block_1, block_2);
        }
        // END DEBUG

        assert!(
            res.is_some(),
            "key {:?} should be in index, but not found",
            key,
        );
    }

    #[test]
    fn test_probe_bloom() {
        let (mt, key, _) = create_full_memtable(SsTableSize::Is(8 * 1024));

        let built = SsTable::build_full(mt);
        let encoded = built.encode();

        let res = SsTable::probe_bloom(&encoded, &key);
        assert!(res.is_ok(), "probe bloom err: {:?}", res.err().unwrap());
        let res = res.unwrap();
        assert!(res.0);
        assert_eq!(res.1, 168);
    }

    fn make_test_index() -> TableIndex {
        let mut ti = TableIndex::new();
        ti.0.push(IndexEntry::new(
            1000,
            Bytes::from("1_block_start"),
            Bytes::from("1_block_end"),
        ));
        ti.0.push(IndexEntry::new(
            2000,
            Bytes::from("2_block_start"),
            Bytes::from("2_block_end"),
        ));
        ti.0.push(IndexEntry::new(
            3000,
            Bytes::from("3_block_start"),
            Bytes::from("3_block_end"),
        ));

        ti
    }

    #[test]
    fn test_index_encode() {
        let ti = make_test_index();
        let encoded = ti.encode();
        assert_eq!(encoded.len(), 104);

        let mut cloned = Cursor::new(encoded.clone());
        let len_encoded = cloned.get_u16();
        assert_eq!(len_encoded, 104);
        let blocks_count = cloned.get_u16();
        assert_eq!(blocks_count, ti.0.len() as u16);
    }

    #[test]
    fn test_index_decode() {
        let ti = make_test_index();
        let encoded = ti.encode();

        let decoded = TableIndex::decode(encoded.as_ref());
        assert_eq!(decoded.0.len(), ti.0.len());
        assert_eq!(decoded.0[0].offset, ti.0[0].offset);
        assert_eq!(decoded.0[2].offset, ti.0[2].offset);
    }
}
