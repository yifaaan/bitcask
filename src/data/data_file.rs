#![allow(dead_code)]
#![allow(unused_variables)]
use std::path::Path;
use std::sync::Arc;

use crate::errors::Result;
use crate::fio::IOManager;
use parking_lot::RwLock;

use super::log_record::LogRecord;

/// 数据文件
pub struct DataFile {
    /// 文件id
    file_id: Arc<RwLock<u32>>,
    /// 写偏移
    write_offset: Arc<RwLock<u64>>,
    /// io管理接口
    io_managner: Box<dyn IOManager>,
}

impl DataFile {
    pub fn new(dir_path: &Path, file_id: u32) -> Result<Self> {
        todo!()
    }
    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }
}
