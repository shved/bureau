use std::cmp::Reverse;
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
    // TODO: Remove panic.
    pub fn init(data_path: String) -> std::result::Result<Self, anyhow::Error> {
        let paths = fs::read_dir(data_path).expect("Failed to read data files");

        // TODO: Better errors initializing index.
        paths
            .into_iter()
            .map(|p| {
                let id = Uuid::parse_str(p?.file_name().into_string().unwrap().as_str())?;
                Ok(Entry { id })
            })
            .collect::<std::result::Result<Vec<Entry>, anyhow::Error>>()
            .map(|mut r| {
                r.sort_by_key(|e| Reverse(e.id));
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
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn prepend() {
        let mut idx = Index {
            entries: vec![
                Entry {
                    id: Uuid::parse_str("01923000-1551-71d1-96b0-4063addc3fcd").unwrap(),
                },
                Entry {
                    id: Uuid::parse_str("01922ffe-ff42-7a24-99af-69793801e519").unwrap(),
                },
            ],
        };

        let to_prepend = "01923001-1551-71d1-96b0-4063addc3fcd";

        idx.prepend(Uuid::parse_str(to_prepend).unwrap());

        assert_eq!(idx.entries[0].id.to_string(), to_prepend)
    }

    #[test]
    fn init() {
        let data_dir = tempdir().expect("could not create a tempdir for test data");

        let ids: [&str; 5] = [
            "01923000-9809-722f-b567-64f172b54f56",
            "01923000-4db5-71c9-8586-0554d2c9f956",
            "01923000-d486-705e-b6fe-f1dcf9cb01ae",
            "01922ffe-ff42-7a24-99af-69793801e519",
            "01923000-1551-71d1-96b0-4063addc3fcd",
        ];

        for name in ids {
            let file_path = data_dir.path().join(name);
            File::create(file_path).unwrap_or_else(|_| panic!("could not create file {}", name));
        }

        match Index::init(data_dir.path().as_os_str().to_str().unwrap().to_string()) {
            Ok(index) => {
                assert_eq!(
                    index.entries[0].id.to_string(),
                    "01923000-d486-705e-b6fe-f1dcf9cb01ae"
                );
                assert_eq!(
                    index.entries[4].id.to_string(),
                    "01922ffe-ff42-7a24-99af-69793801e519"
                )
            }
            Err(e) => {
                panic!("could not init index: {}", e)
            }
        }
    }
}
