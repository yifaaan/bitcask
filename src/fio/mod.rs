#![allow(dead_code)]

mod file_io;
mod mmap;
use std::path::Path;

use file_io::FileIo;
use mmap::MmapIO;

use crate::{errors::Result, options::IOType};

/// Abstract IOManager, for different file systems.
/// Only support file based storage at present.
pub trait IOManager: Send + Sync {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
    fn size(&self) -> u64;
}

/// Create a new IOManager
pub fn new_io_manager(file_path: &Path, io_type: IOType) -> Result<Box<dyn IOManager + 'static>> {
    match io_type {
        IOType::StandardFileIO => Ok(Box::new(FileIo::new(file_path)?)),
        IOType::MmapIO => Ok(Box::new(MmapIO::new(file_path)?)),
    }
}
