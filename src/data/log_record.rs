#![allow(dead_code)]

/// record position in the log file for index
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LogRecordType {
    Normal = 1,
    Deleted = 2,
}
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    /// 将记录编码为字节流
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }
}
