mod cache;
pub mod mem;

use std::fs;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct FsStorage {
    data_path: PathBuf,
}

pub fn new(path: &str) -> FsStorage {
    let data_path = PathBuf::from(path);

    FsStorage { data_path }
}

impl crate::Storage for FsStorage {
    type Entry = fs::File;

    fn bootstrap(&self) -> io::Result<()> {
        if !self.data_path.exists() {
            return fs::create_dir(self.data_path.as_path());
        }

        Ok(())
    }

    fn list_entries(&self) -> io::Result<Vec<Uuid>> {
        let mut uuids: Vec<Uuid> = fs::read_dir(self.data_path.as_path())?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();

                if path.is_file() {
                    // Get the filename without extension
                    if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                        // Attempt to parse the filename as a UUID
                        if let Ok(uuid) = Uuid::parse_str(file_stem) {
                            return Some(uuid);
                        }
                    }
                }
                None
            })
            .collect();

        uuids.sort();
        uuids.reverse();

        Ok(uuids)
    }

    fn write(&self, table_id: &Uuid, data: &[u8]) -> io::Result<()> {
        fs::write(sstable_path(self.data_path.as_path(), table_id), data)?;
        fs::File::open(self.data_path.as_path())?.sync_all()
    }

    fn open(&self, table_id: &Uuid) -> io::Result<Self::Entry> {
        fs::File::options()
            .read(true)
            .write(false)
            .open(sstable_path(self.data_path.as_path(), table_id))
    }
}

impl crate::StorageEntry for fs::File {
    fn read_at(&self, data: &mut Vec<u8>, position: u64) -> io::Result<()> {
        self.read_exact_at(data, position)?;

        Ok(())
    }
}

fn sstable_path(data_path: &Path, table_id: &Uuid) -> PathBuf {
    data_path.join(table_id.to_string())
}
