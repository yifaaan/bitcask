#![allow(dead_code)]

mod file_io;
use std::path::Path;

use file_io::FileIo;

use crate::errors::Result;

/// Abstract IOManager, for different file systems.
/// Only support file based storage at present.
pub trait IOManager: Send + Sync {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
    fn size(&self) -> u64;
}

/// Create a new IOManager
pub fn new_io_manager(file_path: &Path) -> Result<impl IOManager + 'static> {
    FileIo::new(file_path)
}
