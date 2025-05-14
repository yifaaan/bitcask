use std::fs::OpenOptions;
use std::path::Path;
use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use super::IOManager;
use crate::errors::Errors;
use crate::errors::Result;

use log::error;
use parking_lot::RwLock;

pub struct FileIo {
    fd: Arc<RwLock<File>>,
}

impl IOManager for FileIo {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        read_guard.read_at(buf, offset).map_err(|e| {
            error!("Failed to read from file: {}", e);
            Errors::ReadFromDataFileError
        })
    }
    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        use std::io::Write;
        write_guard.write(buf).map_err(|e| {
            error!("Failed to write to file: {}", e);
            Errors::WriteToDataFileError
        })
    }
    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        read_guard.sync_all().map_err(|e| {
            error!("Failed to sync file: {}", e);
            Errors::SyncFileError
        })
    }
    fn size(&self) -> u64 {
        let read_guard = self.fd.read();
        read_guard.metadata().unwrap().len()
    }
}

impl FileIo {
    pub fn new(file_path: &Path) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .append(true) // 只支持追加写入
            .open(file_path)
        {
            Ok(file) => Ok(Self {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => {
                error!("Failed to open file: {}", e);
                Err(Errors::OpenFileError)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let file_path = PathBuf::from("/tmp/a.data");
        let file_res = FileIo::new(&file_path);
        assert!(file_res.is_ok());
        let file = file_res.unwrap();
        let buf = b"hello, world";
        let write_res = file.write(buf);
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), buf.len());
        std::fs::remove_file(file_path).unwrap();
    }

    #[test]
    fn test_file_io_read() {
        let file_path = PathBuf::from("/tmp/b.data");
        let file_res = FileIo::new(&file_path);
        assert!(file_res.is_ok());
        let file = file_res.unwrap();

        let write_res = file.write(b"hello, world");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 12);

        let write_res = file.write(b"aabbcc");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 6);

        let mut buf = vec![0; 12];
        let read_res = file.read(&mut buf, 0);
        assert!(read_res.is_ok());
        assert_eq!(read_res.unwrap(), 12);
        assert_eq!(buf, b"hello, world");

        let mut buf = vec![0; 6];
        let read_res = file.read(&mut buf, 12);
        assert!(read_res.is_ok());
        assert_eq!(read_res.unwrap(), 6);
        assert_eq!(buf, b"aabbcc");

        std::fs::remove_file(file_path).unwrap();
    }

    #[test]
    fn test_file_io_sync() {
        let file_path = PathBuf::from("/tmp/c.data");
        let file_res = FileIo::new(&file_path);
        assert!(file_res.is_ok());
        let file = file_res.unwrap();

        let write_res = file.write(b"hello, world");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 12);

        let sync_res = file.sync();
        assert!(sync_res.is_ok());

        std::fs::remove_file(file_path).unwrap();
    }
}
