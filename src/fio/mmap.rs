#![allow(unused_variables)]
#![allow(dead_code)]

use std::{path::Path, sync::Arc};

use log::error;
use parking_lot::Mutex;

use super::IOManager;
use crate::errors::{Errors, Result};
pub struct MmapIO {
    map: Arc<Mutex<memmap2::Mmap>>,
}

impl MmapIO {
    pub fn new(file_path: &Path) -> Result<Self> {
        match std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(file_path)
        {
            Ok(f) => {
                let mmap = unsafe {
                    memmap2::MmapOptions::new()
                        .map(&f)
                        .expect("Failed to mmap file")
                };
                Ok(Self {
                    map: Arc::new(Mutex::new(mmap)),
                })
            }
            Err(e) => {
                error!("Failed to open file: {}", e);
                Err(Errors::OpenFileError)
            }
        }
    }
}

impl IOManager for MmapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mmap = self.map.lock();
        let end = offset + buf.len() as u64;
        if end > mmap.len() as u64 {
            return Err(Errors::ReadDataFileEof);
        }
        buf.copy_from_slice(&mmap[offset as usize..end as usize]);
        Ok(buf.len())
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn size(&self) -> u64 {
        let mmap = self.map.lock();
        mmap.len() as u64
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::fio::file_io::FileIo;

    use super::*;

    #[test]
    fn test_file_io_read() {
        let file_path = PathBuf::from("/tmp/mmap.data");
        let file_res = MmapIO::new(&file_path);
        assert!(file_res.is_ok());
        let file = file_res.unwrap();
        // 无数据
        let mut buf = vec![0; 6];
        let read_res = file.read(&mut buf, 0);
        assert!(read_res.is_err());

        // 写入数据
        let fio = FileIo::new(&file_path);
        assert!(fio.is_ok());
        let fio = fio.unwrap();
        let write_res = fio.write(b"hello, world");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 12);

        let write_res = fio.write(b"aabbcc");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 6);

        let file_res = MmapIO::new(&file_path);
        assert!(file_res.is_ok());
        let file = file_res.unwrap();
        let mut buf = vec![0; 12];
        let read_res = file.read(&mut buf, 0);
        assert!(read_res.is_ok());
        assert_eq!(read_res.unwrap(), 12);
        assert_eq!(buf, b"hello, world");

        std::fs::remove_file(file_path).unwrap();
    }
}
