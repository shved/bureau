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
    // TODO: Consider renaming it to new.
    pub fn init(entries: &mut [Uuid]) -> Self {
        // Seem to be not necessary here but tables set will not be too huge and index only needs
        // to be initialized once the database starts so it's fine if we end up doing extra work.
        entries.sort();
        entries.reverse();

        Self {
            entries: entries
                .iter()
                .map(|table_id| Entry { id: *table_id })
                .collect(),
        }
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

    #[test]
    fn test_prepend() {
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
    fn test_init() {
        let mut ids: Vec<Uuid> = vec![
            Uuid::parse_str("01923000-9809-722f-b567-64f172b54f56").unwrap(),
            Uuid::parse_str("01923000-4db5-71c9-8586-0554d2c9f956").unwrap(),
            Uuid::parse_str("01923000-d486-705e-b6fe-f1dcf9cb01ae").unwrap(),
            Uuid::parse_str("01922ffe-ff42-7a24-99af-69793801e519").unwrap(),
            Uuid::parse_str("01923000-1551-71d1-96b0-4063addc3fcd").unwrap(),
        ];

        let index = Index::init(&mut ids);
        assert_eq!(
            index.entries[0].id.to_string(),
            "01923000-d486-705e-b6fe-f1dcf9cb01ae"
        );
        assert_eq!(
            index.entries[4].id.to_string(),
            "01922ffe-ff42-7a24-99af-69793801e519"
        );
    }
}
