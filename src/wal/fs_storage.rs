use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const LOG_PATH: &str = "/var/log/bureau"; // TODO: Make configurable.

#[derive(Debug)]
pub struct FsStorage {
    log_path: PathBuf,
    cur_filename: String,
    cur_file: File,
}

pub enum LogPath {
    Default,
    Is(String),
}

impl crate::WalStorage for FsStorage {
    type LogPath = LogPath;

    fn init(path: LogPath) -> io::Result<Self> {
        let log_path = match path {
            LogPath::Default => PathBuf::from(LOG_PATH),
            LogPath::Is(path_str) => PathBuf::from(path_str),
        };

        if !log_path.exists() {
            fs::create_dir(log_path.as_path())?;
        }

        let cur_filename: String;
        let cur_file: File;
        let file_path: PathBuf;

        if let Ok(Some(found_filename)) = find_latest_wal_file(&log_path) {
            cur_filename = found_filename;
            let file_path = log_path.join(cur_filename.clone());
            cur_file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(&file_path)?;
        } else {
            cur_filename = new_file_name();
            file_path = log_path.join(cur_filename.clone());

            cur_file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&file_path)?;
        }

        Ok(FsStorage {
            log_path,
            cur_filename,
            cur_file,
        })
    }

    fn persisted_data(&mut self) -> io::Result<Option<Bytes>> {
        let mut data = Vec::new();
        self.cur_file.read_to_end(&mut data)?;

        if data.is_empty() {
            return Ok(None); // File is empty, no data yet.
        }

        Ok(Some(Bytes::from(data)))
    }

    fn append(&mut self, page: bytes::Bytes) -> io::Result<()> {
        assert!(
            page.len() == super::PAGE_SIZE,
            "data to write to disk should be exactly memory page in size (got {})",
            page.len()
        );

        self.cur_file.write_all(page.as_ref())?;
        self.cur_file.flush()?;

        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        let old_file_path = self.log_path.join(self.cur_filename.clone());

        let cur_filename = new_file_name();
        let file_path = self.log_path.join(cur_filename.clone());

        let cur_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        self.cur_filename = cur_filename;
        self.cur_file = cur_file;

        fs::remove_file(old_file_path)?;

        Ok(())
    }
}

fn new_file_name() -> String {
    format!(
        "{}.wal",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

fn extract_timestamp(filename: &str) -> Option<u64> {
    filename
        .split('.')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
}

fn find_latest_wal_file(dir: &PathBuf) -> io::Result<Option<String>> {
    let mut wal_files: Vec<(u64, String)> = fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension()? == "wal" {
                let filename = path.file_name()?.to_str()?.to_string();
                let timestamp = extract_timestamp(&filename)?;
                Some((timestamp, filename))
            } else {
                None
            }
        })
        .collect();

    // Sort files by timestamp in descending order.
    wal_files.sort_by(|a, b| b.0.cmp(&a.0));

    // Return the latest file (first in the sorted list).
    Ok(wal_files.into_iter().map(|(_, filename)| filename).next())
}
