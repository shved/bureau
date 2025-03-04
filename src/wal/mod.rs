pub mod fs_storage;
pub mod mem_storage;

use crate::WalStorage;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

pub const PAGE_SIZE: usize = 4 * 1024; // 4KB.

/// This WAL is very basic, no backward compatibility, no versioning. Just
/// records encoded with simple checksum. Records being written to buffer of
/// PAGE_SIZE until it can not accomodate the next record and then it is being
/// flushed to storage.
// TODO: Filling records with paddings to make it a page to persist should not be
// WALs concern but rather storage concern. FS storage should just cut off dangling
// zeroes and return payload so that WAL itself will not be tied to pages size.
#[derive(Debug, Clone)]
pub struct Wal<T: WalStorage> {
    buf: BytesMut,
    storage: T,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub encoded: Bytes,
    pub key: Bytes,
    pub value: Bytes,
}

impl<W: WalStorage> Wal<W> {
    /// If log has some records persisted, return them with the call so that engine can populate
    /// the records to the memtable as well.
    pub fn init(storage: W) -> io::Result<(Self, Option<Vec<Entry>>)> {
        let mut wal = Self {
            buf: BytesMut::with_capacity(PAGE_SIZE),
            storage,
        };

        let mut records: Vec<Entry> = Vec::new();
        if let Some(data) = wal.storage.persisted_data()? {
            for page in data.chunks(PAGE_SIZE) {
                if let Some(parsed_records) = parse_page(page)? {
                    records.extend(parsed_records);
                };
            }
        }

        let records = if records.is_empty() {
            None
        } else {
            Some(records)
        };

        Ok((wal, records))
    }

    /// Adds encoded frame to buffer. If new record will overflow the buffer its content will be
    /// sent to storage to free buffer for new records.
    pub fn append(&mut self, key: Bytes, value: Bytes) -> io::Result<()> {
        let entry = Entry::encode(key, value);

        if self.buf.len() + entry.encoded.len() > PAGE_SIZE {
            let mut page = self.buf.split_to(self.buf.len());
            self.buf.reserve(PAGE_SIZE - self.buf.len());

            if page.len() < PAGE_SIZE {
                let len_to_fill = PAGE_SIZE - page.len();
                page.reserve(len_to_fill);
                page.extend(std::iter::repeat(0).take(len_to_fill));
            }

            self.storage.append(Bytes::from(page))?;
        }

        self.buf.extend(entry.encoded);

        Ok(())
    }

    /// Checks if the buffer is not empty and flushes its content to storage.
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let mut page = self.buf.split_to(self.buf.len());
            self.buf.reserve(PAGE_SIZE - self.buf.len());

            let len_to_fill = PAGE_SIZE - page.len();
            page.reserve(len_to_fill);
            page.extend(std::iter::repeat(0).take(len_to_fill));
            self.storage.append(Bytes::from(page))?;
        }

        Ok(())
    }

    /// Flushes buffers to disk and calls the storage to rotate log.
    pub fn rotate(&mut self) -> io::Result<()> {
        self.flush()?;
        self.storage.rotate()
    }
}

/*
WAL entry schema.
-------------------------------------------------------------------------------
| Entry Length | Key Length | Key Data | Value Length | Value Data | Checksum |
-------------------------------------------------------------------------------
|     2B       |     2B     |   ...    |      2B      |    ...     |    4B    |
-------------------------------------------------------------------------------
*/
impl Entry {
    pub fn encode(key: Bytes, value: Bytes) -> Self {
        // TODO: Return error instead of panic.
        assert!(!key.is_empty());
        assert!(!value.is_empty());

        let mut data = BytesMut::new();
        // 2 bytes key len, then the key, 2 bytes value len, then the value, and 4 bytes for checksum.
        data.put_u16((2 + key.len() + 2 + value.len() + 4) as u16);
        data.put_u16(key.len() as u16);
        data.extend_from_slice(key.as_ref());
        data.put_u16(value.len() as u16);
        data.extend_from_slice(value.as_ref());
        let checksum = crc32fast::hash(data.as_ref());
        data.put_u32(checksum);
        let encoded = Bytes::from(data);

        Self {
            encoded,
            key,
            value,
        }
    }

    pub fn decode(buf: &mut io::Cursor<&[u8]>, entry_len: usize) -> io::Result<Self> {
        // TODO: The whole function is ugly.
        use bytes::Buf;

        if buf.remaining() == 0 || entry_len == 0 || buf.remaining() < entry_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "insufficient bytes for the entry",
            ));
        }

        let mut encoded = BytesMut::with_capacity(2 + entry_len);

        encoded.put_u16(entry_len as u16);

        let key_len = buf.get_u16() as usize;
        encoded.put_u16(key_len as u16);

        let mut key = vec![0; key_len];
        buf.copy_to_slice(&mut key);
        encoded.extend_from_slice(&key);

        let value_len = buf.get_u16() as usize;
        encoded.put_u16(value_len as u16);

        let mut value = vec![0; value_len];
        buf.copy_to_slice(&mut value);
        encoded.extend_from_slice(&value);

        let checksum_read = buf.get_u32();
        let calculated_checksum = crc32fast::hash(&encoded);
        if checksum_read != calculated_checksum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "wrong checksum"));
        }

        encoded.put_u32(checksum_read);

        Ok(Self {
            encoded: Bytes::from(encoded),
            key: Bytes::from(key),
            value: Bytes::from(value),
        })
    }
}

fn parse_page(page: &[u8]) -> io::Result<Option<Vec<Entry>>> {
    if page.len() != PAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input slice should be exactly page in size",
        ));
    };

    let mut records = Vec::new();
    let mut buf = io::Cursor::new(page);

    while buf.remaining() >= 2 {
        let entry_len = buf.get_u16();
        if entry_len == 0 {
            break;
        }

        let record = Entry::decode(&mut buf, entry_len as usize)?;
        records.push(record);
    }

    if records.is_empty() {
        return Ok(None);
    }

    Ok(Some(records))
}

#[cfg(test)]
mod tests {
    use super::mem_storage::{InitialState, MemStorage};
    use super::*;
    use std::collections::HashMap;

    impl Wal<MemStorage> {
        pub fn persisted_data(&self) -> HashMap<usize, Bytes> {
            self.storage.logs()
        }

        pub fn buf(&self) -> Bytes {
            Bytes::from(self.buf.clone())
        }
    }

    #[test]
    fn test_init() {
        let mem = MemStorage::init(InitialState::Blank).unwrap();
        let wal = Wal::init(mem);
        assert!(wal.is_ok());
        let (_, entries) = wal.unwrap();
        assert!(entries.is_none());

        let mut state = HashMap::new();
        let entries: Vec<Entry> = vec![
            Entry::encode(Bytes::from("Day after day"), Bytes::from("Alone on a hill")),
            Entry::encode(
                Bytes::from("The man with the foolish grin is keeping perfectly still"),
                Bytes::from("But nobody wants to know him"),
            ),
            Entry::encode(
                Bytes::from("They can see that he's just a fool"),
                Bytes::from("And he never gives an answer"),
            ),
        ];
        let mut encoded: BytesMut = entries.into_iter().fold(BytesMut::new(), |mut acc, b| {
            acc.extend_from_slice(&b.encoded);
            acc
        });

        state.insert(0, Bytes::default());
        state.insert(1, Bytes::from("lagom is the key"));
        state.insert(2, Bytes::from(encoded.clone()));

        let mem = MemStorage::init(InitialState::Is(state.clone())).unwrap();
        let wal = Wal::init(mem);
        assert!(wal.is_err()); // page must be 4096 bytes in size.

        let padding_len = 4096 - encoded.len();
        encoded.reserve(padding_len);
        encoded.extend(std::iter::repeat(0).take(padding_len));
        state.insert(2, Bytes::from(encoded.clone()));

        let mem = MemStorage::init(InitialState::Is(state.clone())).unwrap();
        let wal = Wal::init(mem);
        assert!(wal.is_ok());
        let (wal, entries) = wal.unwrap();
        assert!(entries.is_some());
        let entries = entries.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(wal.persisted_data(), state);
    }

    #[test]
    fn test_append() {
        let mem = MemStorage::init(InitialState::Blank).unwrap();
        let (mut wal, _) = Wal::init(mem).unwrap();
        let res = wal.append(Bytes::from("a"), Bytes::from("b"));
        assert!(res.is_ok());
        let data: &[u8] = &[0, 10, 0, 1, b'a', 0, 1, b'b'];
        let h = crc32fast::hash(data).to_be_bytes();
        assert_eq!(wal.buf(), Bytes::from_iter(data.iter().copied().chain(h)));
    }

    #[test]
    #[should_panic]
    fn test_append_panic() {
        let mem = MemStorage::init(InitialState::Blank).unwrap();
        let (mut wal, _) = Wal::init(mem).unwrap();
        let _ = wal.append(Bytes::from(""), Bytes::from(""));
    }

    #[test]
    fn test_flush() {
        let mem = MemStorage::init(InitialState::Blank).unwrap();
        let (mut wal, _) = Wal::init(mem).unwrap();
        let _ = wal.append(Bytes::from("a"), Bytes::from("b"));
        let res = wal.flush();
        assert!(res.is_ok());
        assert!(wal.buf().is_empty());
        let data = wal.storage.logs();
        let data = data.get(&0);
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data.len(), 4 * 1024);
    }

    #[test]
    fn test_rotate() {
        let mem = MemStorage::init(InitialState::Blank).unwrap();
        let (mut wal, _) = Wal::init(mem).unwrap();
        let _ = wal.append(Bytes::from("a"), Bytes::from("b"));
        let res = wal.rotate();
        assert!(res.is_ok());
        assert!(wal.buf().is_empty());
        let logs = wal.storage.logs();
        assert!(logs.contains_key(&1));
    }

    #[test]
    fn test_parse_page() {
        let page = generate_valid_page();
        let res = parse_page(page.as_ref());
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(res.len(), 9);

        let page = Bytes::default();
        let res = parse_page(page.as_ref());
        assert!(res.is_err());

        let raw: &[u8] = &[0; 5000];
        let page = Bytes::from(raw);
        let res = parse_page(page.as_ref());
        assert!(res.is_err());
    }

    #[test]
    #[should_panic]
    fn test_entry_encode_key_panic() {
        let _ = Entry::encode(Bytes::default(), Bytes::from("asdf"));
    }

    #[test]
    #[should_panic]
    fn test_entry_encode_value_panic() {
        let _ = Entry::encode(Bytes::from("asdf"), Bytes::default());
    }

    #[test]
    fn test_entry_encode() {
        let entry = Entry::encode(Bytes::from("asdf"), Bytes::from("test"));
        assert_eq!(entry.key, Bytes::from("asdf"));
        assert_eq!(entry.value, Bytes::from("test"));
        let encoded: &[u8] = &[
            0, 16, 0, 4, b'a', b's', b'd', b'f', 0, 4, b't', b'e', b's', b't',
        ];
        let h = crc32fast::hash(encoded).to_be_bytes();
        assert_eq!(
            entry.encoded,
            Bytes::from_iter(encoded.iter().copied().chain(h))
        );
    }

    #[test]
    fn test_entry_decode() {
        let data: &[u8] = &[
            0, 16, 0, 4, b'a', b's', b'd', b'f', 0, 4, b't', b'e', b's', b't',
        ];
        let h = crc32fast::hash(data).to_be_bytes();
        let expected_encoded = Bytes::from_iter(data.iter().copied().chain(h));
        let data = Bytes::from_iter(data.iter().copied().chain(h));
        let mut cursor = io::Cursor::new(data.as_ref());
        let len = cursor.get_u16() as usize;
        let entry = Entry::decode(&mut cursor, len);
        assert!(entry.is_ok());
        let entry = entry.unwrap();
        assert_eq!(entry.key, Bytes::from("asdf"));
        assert_eq!(entry.value, Bytes::from("test"));
        assert_eq!(entry.encoded, expected_encoded);
    }

    fn generate_valid_page() -> Bytes {
        let entries: Vec<Entry> = vec![
            Entry::encode(
                Bytes::from("Ave, Maria, grátia plena"),
                Bytes::from("Maria grátia plena"),
            ),
            Entry::encode(
                Bytes::from("Maria grátia plena"),
                Bytes::from("Ave, ave Dóminus"),
            ),
            Entry::encode(
                Bytes::from("Dóminus tecum"),
                Bytes::from("Benedícta tu in muliéribus"),
            ),
            Entry::encode(
                Bytes::from("Et benedíctus, benedíctus"),
                Bytes::from("Fructus fructus ventris tui, Iesus"),
            ),
            Entry::encode(
                Bytes::from("Ave, Maria"),
                Bytes::from("Ave Maria, Mater Dei"),
            ),
            Entry::encode(
                Bytes::from("Ora pro nobis peccatóribus"),
                Bytes::from("Ora, ora pro nobis"),
            ),
            Entry::encode(
                Bytes::from("Ora, ora pro nobis peccatóribus"),
                Bytes::from("Nunc et in hora mortis"),
            ),
            Entry::encode(
                Bytes::from("In hora mortis nostrae"),
                Bytes::from("In hora mortis mortis nostrae"),
            ),
            Entry::encode(
                Bytes::from("In hora mortis nostrae"),
                Bytes::from("Ave Maria"),
            ),
        ];

        let mut encoded: BytesMut = entries.into_iter().fold(BytesMut::new(), |mut acc, b| {
            acc.extend_from_slice(&b.encoded);
            acc
        });

        let padding_len = 4096 - encoded.len();
        encoded.reserve(padding_len);
        encoded.extend(std::iter::repeat(0).take(padding_len));

        Bytes::from(encoded)
    }
}
