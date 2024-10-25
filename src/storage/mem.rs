use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// A dummy persistance layer that puts blobs in memory. Exists for test.
#[derive(Clone, Debug)]
pub struct MemStorage {
    entries: Arc<Mutex<HashMap<Uuid, Vec<u8>>>>,
}

pub fn new() -> MemStorage {
    MemStorage {
        entries: Arc::new(Mutex::new(HashMap::new())),
    }
}

impl crate::Storage for MemStorage {
    type Entry = Vec<u8>;

    fn bootstrap(&self) -> io::Result<()> {
        Ok(())
    }

    fn list_entries(&self) -> io::Result<Vec<Uuid>> {
        let mut entries: Vec<Uuid> = self.entries.lock().unwrap().clone().into_keys().collect();
        entries.sort();
        entries.reverse();

        Ok(entries)
    }

    fn write(&self, table_id: &Uuid, data: &[u8]) -> io::Result<()> {
        self.entries
            .lock()
            .unwrap()
            .insert(*table_id, Vec::from(data));

        Ok(())
    }

    fn open(&self, table_id: &Uuid) -> io::Result<Self::Entry> {
        match self.entries.lock().unwrap().get(table_id) {
            Some(data) => Ok(data.clone()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("table id {} not found", table_id),
            )),
        }
    }
}

impl crate::StorageEntry for Vec<u8> {
    fn read_at(&self, data: &mut Vec<u8>, position: u64) -> io::Result<()> {
        let position = position as usize;

        if position >= self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("position {} exceeds data len {}", position, self.len()),
            ));
        }

        let read_len = std::cmp::min(data.capacity(), self.len() - position);

        data.clear();
        data.extend_from_slice(&self[position..position + read_len]);

        if data.len() < data.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "target vector is not filled up its capacity ({}/{})",
                    data.len(),
                    data.capacity()
                ),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Storage, StorageEntry};

    #[test]
    fn test_memstore() {
        let ids: Vec<Uuid> = vec![
            Uuid::parse_str("01923000-9809-722f-b567-64f172b54f56").unwrap(),
            Uuid::parse_str("01923000-4db5-71c9-8586-0554d2c9f956").unwrap(),
            Uuid::parse_str("01923000-d486-705e-b6fe-f1dcf9cb01ae").unwrap(),
            Uuid::parse_str("01922ffe-ff42-7a24-99af-69793801e519").unwrap(),
            Uuid::parse_str("01923000-1551-71d1-96b0-4063addc3fcd").unwrap(),
        ];

        let st = new();
        assert!(st.bootstrap().is_ok(), "bootstrapping the storage");

        for id in ids {
            assert!(
                st.write(&id, b"abcde").is_ok(),
                "writing an entry {} to storage",
                id
            );
        }

        let list = st.list_entries().unwrap();

        assert_eq!(list[0].to_string(), "01923000-d486-705e-b6fe-f1dcf9cb01ae");
        assert_eq!(list[4].to_string(), "01922ffe-ff42-7a24-99af-69793801e519");

        let entry_not_found =
            st.open(&Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap());
        assert!(entry_not_found.is_err());

        let entry = st
            .open(&Uuid::parse_str("01922ffe-ff42-7a24-99af-69793801e519").unwrap())
            .unwrap();

        let mut data = vec![0; 3];
        assert!(entry.read_at(&mut data, 0).is_ok());
        assert_eq!(
            data,
            b"abc".to_vec(),
            "target vec capacity is less then source vec lenght"
        );

        data = vec![0; 5];
        assert!(entry.read_at(&mut data, 6).is_err());
        assert!(entry.read_at(&mut data, 0).is_ok());
        assert_eq!(
            data,
            b"abcde".to_vec(),
            "target vec capacity equals to source vec length"
        );

        data = vec![0; 6];
        assert!(
            entry.read_at(&mut data, 0).is_err(),
            "target vec capacity exceeds source vec length"
        );
    }
}
