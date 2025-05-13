#![allow(dead_code)]

use bytes::{BufMut, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter, length_delimiter_len};

/// record position in the log file for index
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

impl LogRecordPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_length_delimiter(self.file_id as usize, &mut buf).expect("Failed to encode file id");
        encode_length_delimiter(self.offset as usize, &mut buf).expect("Failed to encode offset");
        buf.to_vec()
    }
}

pub(crate) fn decode_log_record_pos(buf: &[u8]) -> LogRecordPos {
    let mut buf = BytesMut::from(buf);
    let file_id = match decode_length_delimiter(&mut buf) {
        Ok(v) => v as u32,
        Err(e) => {
            panic!("Failed to decode file id: {}", e);
        }
    };
    let offset = match decode_length_delimiter(&mut buf) {
        Ok(v) => v as u64,
        Err(e) => {
            panic!("Failed to decode offset: {}", e);
        }
    };
    LogRecordPos { file_id, offset }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum LogRecordType {
    #[default]
    Normal = 1,
    Deleted = 2,
    TxnFinished = 3,
}

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => LogRecordType::Normal,
            2 => LogRecordType::Deleted,
            3 => LogRecordType::TxnFinished,
            _ => panic!("invalid log record type: {}", value),
        }
    }
}

#[derive(Default, Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    /// 将记录编码为字节流，并返回字节流和CRC
    ///
    // encode 对 LogRecord 进行编码，返回字节数组及长度
    //
    //	+-------------+--------------+-------------+--------------+-------------+-------------+
    //	|  type 类型   |    key size |   value size |      key    |      value   |  crc 校验值  |
    //	+-------------+-------------+--------------+--------------+-------------+-------------+
    //	    1字节        变长（最大5）   变长（最大5）        变长           变长           4字节
    pub fn encode(&self) -> Vec<u8> {
        self.encode_and_get_crc().0
    }

    /// 计算CRC
    pub fn get_crc(&self) -> u32 {
        self.encode_and_get_crc().1
    }

    /// 将记录编码为字节流，并返回字节流和CRC
    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::with_capacity(self.encoded_length());
        // 写入记录类型
        buf.put_u8(self.rec_type as u8);
        // 写入key长度
        encode_length_delimiter(self.key.len(), &mut buf).expect("Failed to encode key length");
        // 写入value长度
        encode_length_delimiter(self.value.len(), &mut buf).expect("Failed to encode value length");
        // 写入key
        buf.put(self.key.as_slice());
        // 写入value
        buf.put(self.value.as_slice());
        // 计算CRC
        use crc32fast::Hasher;
        let mut hasher = Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        // 写入CRC
        buf.put_u32(crc);
        (buf.to_vec(), crc)
    }

    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
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

/// 事务记录
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) position: LogRecordPos,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode() {
        let record = LogRecord {
            key: "hello".into(),
            value: "world".into(),
            rec_type: LogRecordType::Normal,
        };
        let encoded = record.encode();
        assert!(encoded.len() > 5);
        assert_eq!(561450126, record.get_crc());

        let record = LogRecord {
            key: "abc".into(),
            value: "123".into(),
            rec_type: LogRecordType::Normal,
        };
        let encoded = record.encode();
        assert!(encoded.len() > 5);
        assert_eq!(819267436, record.get_crc());
    }
}
