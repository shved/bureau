use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::io;

/// This implementation never returns errors. Its state is not persisted.
#[derive(Debug)]
pub struct MemStorage {
    pub logs: HashMap<usize, Bytes>,
    pub cur_key: usize,
}

pub enum InitialState {
    Blank,
    Is(HashMap<usize, Bytes>),
}

impl crate::WalStorage for MemStorage {
    type LogPath = InitialState;

    fn init(initial: InitialState) -> io::Result<Self> {
        let mut logs: HashMap<usize, Bytes> = HashMap::new();
        let mut cur_key = 0;

        match initial {
            InitialState::Is(initial_records) => {
                cur_key = *initial_records.keys().max().unwrap();
                logs = initial_records;
            }
            InitialState::Blank => {}
        }

        Ok(Self { logs, cur_key })
    }

    fn persisted_data(&mut self) -> io::Result<Option<Bytes>> {
        if let Some(latest_key) = self.logs.keys().max() {
            if let Some(value) = self.logs.get(latest_key) {
                if value.is_empty() {
                    return Ok(None);
                }

                return Ok(Some(value.clone()));
            }
        };

        Ok(None)
    }

    fn append(&mut self, page: bytes::Bytes) -> io::Result<()> {
        self.logs
            .entry(self.cur_key)
            .and_modify(|value| {
                let mut new_value = BytesMut::from(value.as_ref());
                new_value.extend_from_slice(&page);
                *value = new_value.freeze();
            })
            .or_insert(page);

        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.logs.remove(&self.cur_key);
        self.cur_key += 1;
        self.logs.insert(self.cur_key, Bytes::default());

        Ok(())
    }
}

impl MemStorage {
    pub fn logs(&self) -> HashMap<usize, Bytes> {
        self.logs.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalStorage;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[test]
    fn test_init() {
        let empty = MemStorage::init(InitialState::Blank);
        assert!(empty.is_ok());
        let mut empty = empty.unwrap();
        assert!(matches!(empty.persisted_data(), Ok(None)));
        assert_eq!(empty.cur_key, 0);

        let mut state: HashMap<usize, Bytes> = HashMap::new();
        let expected_data = Bytes::from("bubble gum");
        state.insert(1, Bytes::from("laobabao"));
        state.insert(2, expected_data.clone());

        let with_state = MemStorage::init(InitialState::Is(state.clone()));
        assert!(with_state.is_ok());
        let with_state = with_state.unwrap();
        assert_eq!(with_state.logs, state);
        assert_eq!(with_state.cur_key, 2);
    }

    #[test]
    fn test_persisted_data() {
        let mut stor = MemStorage {
            logs: HashMap::new(),
            cur_key: 0,
        };

        let data = stor.persisted_data();
        assert!(data.is_ok());
        let data = data.unwrap();
        assert!(data.is_none());

        let mut state = HashMap::new();
        state.insert(0, Bytes::from("data"));
        let mut stor = MemStorage {
            logs: state,
            cur_key: 0,
        };
        let data = stor.persisted_data();
        assert!(data.is_ok());
        let data = data.unwrap();
        assert!(data.is_some());
        let data = data.unwrap();
        assert_eq!(data, Bytes::from("data"));
    }

    #[test]
    fn test_append() {
        let mut data = HashMap::new();
        data.insert(2, Bytes::default());
        data.insert(5, Bytes::default());
        let mut stor = MemStorage {
            logs: data,
            cur_key: 5,
        };

        let res = stor.append(Bytes::from("hahaha"));
        assert!(res.is_ok());
        let saved = stor.persisted_data();
        assert!(saved.is_ok());
        let saved = saved.unwrap();
        assert!(saved.is_some());
        let saved = saved.unwrap();
        assert_eq!(saved, Bytes::from("hahaha"));
    }

    #[test]
    fn test_rotate() {
        let mut stor = MemStorage::init(InitialState::Blank).unwrap();
        stor.append(Bytes::from("good data")).unwrap();
        let res = stor.rotate();
        assert!(res.is_ok());
        let res = stor.persisted_data().unwrap();
        dbg!(&res);
        assert!(res.is_none());
        assert_eq!(stor.cur_key, 1);
    }
}
