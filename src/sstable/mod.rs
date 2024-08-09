pub mod block;
mod bloom;
mod builder;
mod compaction;
pub mod dispatcher;

use crate::sstable::bloom::BloomSerializable;
use anyhow::{bail, Result};
use block::Block;
use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};
use crc32fast;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

/*
SST layout schema.
----------------------------------------------------------------------------------------------
| Bloom Filter |             Block Index              |            Blocks Section            |
----------------------------------------------------------------------------------------------
| 11906 Bytes  | Entry #1 | Entry #2 | ... | Entry #N | Block #1 | Block #2 | ... | Block #N |
----------------------------------------------------------------------------------------------

Block index entry layout.
--------------------------------------------------------------------------
|                              Entry #1                            | ... |
--------------------------------------------------------------------------
| key_len (2B) | first_key | key_len (2B) | last_key | Offset (2B) | ... |
--------------------------------------------------------------------------

Individual block layout is given where Block is defined.
*/

pub struct SsTable {
    pub file: FileObject,
    pub block_index: BlockIndex,
    /// Where index begins.
    // pub index_offset: usize,
    id: String,
    first_key: Bytes,
    last_key: Bytes,
    pub bloom: Option<Bloom<Bytes>>,
    // max_ts: u64,
}

impl SsTable {
    pub fn open(id: String)
    pub fn open(id: usize, file: FileObject) -> Result<Self> {
        let len = file.size;
        let raw_bloom_offset = file.read(len - 4, 4)?; // TODO: Whats the real offset?
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?; // TODO HERE
        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(id: usize, file_size: u64, first_key: Bytes, last_key: Bytes) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    pub fn read_block(&self, block_idx: usize) -> Result<Block> {
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_len = offset_end - offset - 4;
        let block_data_with_chksum: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        let block_data = &block_data_with_chksum[..block_len];
        let checksum = (&block_data_with_chksum[block_len..]).get_u32();
        if checksum != crc32fast::hash(block_data) {
            bail!("block checksum mismatched");
        }

        Ok(Block::decode(block_data))
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

#[derive(Debug)]
struct BlockIndex {
    first_key: Bytes,
    last_key: Bytes,
    blocks: Vec<BlockIndexEntry>,
}

#[derive(Debug)]
struct BlockIndexEntry {
    /// Offset of this data block.
    pub offset: u32,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

impl BlockIndex {
    pub fn encode_block_index(&self, buf: &mut Vec<u8>) {
        let mut size = std::mem::size_of::<u32>(); // ???
        for block in block_index {
            // The size of offset
            size += std::mem::size_of::<u32>();
            // The size of key length
            size += std::mem::size_of::<u16>();
            // The size of actual key
            size += block.first_key.len();
            // The size of key length
            size += std::mem::size_of::<u16>();
            // The size of actual key
            size += block.last_key.len();
        }
        size += std::mem::size_of::<u32>();

        // Reserve the space to improve performance, especially when the size of incoming data is large.
        buf.reserve(size); // ???

        let original_len = buf.len();
        buf.put_u32(block_index.len() as u32);
        for meta in block_index {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.as_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.as_ref());
        }

        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(size, buf.len() - original_len);
    }

    pub fn decode_block_index(mut buf: &[u8]) -> Result<Vec<Self>> {
        let mut block_index = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            block_index.push(Self {
                offset,
                first_key,
                last_key,
            });
        }

        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }

        Ok(block_index)
    }
}

pub struct FileObject {
    file: File,
    size: u64,
}

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        self.file.read_exact_at(&mut data[..], offset)?;

        Ok(data)
    }

    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;

        Ok(FileObject {
            file: File::options().read(true).write(false).open(path)?,
            size: data.len() as u64,
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject { file, size })
    }
}
