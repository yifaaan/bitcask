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

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => LogRecordType::Normal,
            2 => LogRecordType::Deleted,
            _ => panic!("invalid log record type: {}", value),
        }
    }
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

    /// 计算CRC
    pub fn get_crc(&self) -> u32 {
        todo!()
    }
}

/// 从文件读取的一条记录，包含其大小
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

/// 最大日志记录头大小
pub fn max_log_record_header_size() -> usize {
    // 记录类型 + key长度 + value长度
    // length_delimiter_len:编码一个长度分隔符所需的字节数,长度分隔符是用来表示一个长度可变的字段的长度的，
    // 在编码变长消息之前，需要预先计算出需要多少空间来存储长度分隔符
    std::mem::size_of::<LogRecordType>() + prost::length_delimiter_len(u32::MAX as usize) * 2
}
