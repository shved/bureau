// TODO: DO I EVEN NEED IT
use std::fs::File;
// use std::io::prelude::*;
// use std::io::SeekFrom;
use crate::Result;
use std::os::unix::fs::FileExt;
use std::path::Path;

#[derive(Debug)]
pub struct FileManager {
    file: File,
    size: u64,
}

impl FileManager {
    pub fn touch(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        Ok(FileManager { file, size: 0 })
    }

    pub fn write(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileManager(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileManager { file, size })
    }

    pub fn read_exact_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut data = vec![0; len];
        self.file.read_exact_at(&mut data, offset)?;

        Ok(data)
    }

    // pub fn read_from(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
    //     self.file.seek(SeekFrom::Start(offset))?;
    //     let mut data = vec![0; len];
    //     self.file.read_exact(&mut data)?;

    //     Ok(data)
    // }

    // pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
    //     std::fs::write(path, &data)?;
    //     File::open(path)?.sync_all()?;

    //     Ok(FileReader {
    //         file: File::options().read(true).write(false).open(path)?,
    //         size: data.len() as u64,
    //     })
    // }
}
