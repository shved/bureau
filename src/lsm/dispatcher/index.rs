use std::fs;
use uuid::Uuid;

/// Index holding all the SSTables. Index is being updated by Dispatcher
/// in runtime and initialized from disk at the start of the database.
#[derive(Debug, Clone)]
pub struct Index {
    pub entries: Vec<Entry>,
}

/// An entry in the LSM index representing a single SSTable.
#[derive(Debug, Clone)]
pub struct Entry {
    pub id: Uuid,
}

/// Holds an ordered list of SSTables present on disk and ready for requests.
impl Index {
    /// Reads the data folder to look for data files and builds index.
    // TODO: Remove unwrap.
    pub fn init() -> std::result::Result<Self, anyhow::Error> {
        let paths = fs::read_dir(crate::lsm::DATA_PATH).expect("Failed to read data files");

        paths
            .into_iter()
            .map(|p| {
                let id = Uuid::parse_str(p?.file_name().into_string().unwrap().as_str())?;
                Ok(Entry { id })
            })
            .collect::<std::result::Result<Vec<Entry>, anyhow::Error>>()
            .map(|mut r| {
                r.sort_by_key(|e| e.id);
                Self { entries: r }
            })
    }

    pub fn prepend(&mut self, id: Uuid) {
        let old = self.entries.clone();
        self.entries = Vec::new();
        self.entries.push(Entry { id });
        self.entries.extend(old);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
