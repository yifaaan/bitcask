#![allow(dead_code)]

mod file_io;
use crate::errors::Result;

/// Abstract IOManager, for different file systems.
/// Only support file based storage at present.
pub trait IOManager: Send + Sync {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
}
