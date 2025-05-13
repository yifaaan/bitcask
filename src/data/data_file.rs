#![allow(dead_code)]
#![allow(unused_variables)]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::data::log_record::{LogRecord, max_log_record_header_size};
use crate::errors::{Errors, Result};
use crate::fio::{IOManager, new_io_manager};
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use super::log_record::{LogRecordPos, LogRecordType, ReadLogRecord};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub(crate) const HINT_FILE_NAME: &str = "hint-index";
pub(crate) const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";

/// 数据文件
pub struct DataFile {
    /// 文件id
    file_id: Arc<RwLock<u32>>,
    /// 写偏移
    write_offset: Arc<RwLock<u64>>,
    /// io管理接口
    io_manager: Box<dyn IOManager>,
}

impl DataFile {
    /// 打开或创建数据文件
    pub fn new(dir_path: &Path, file_id: u32) -> Result<Self> {
        let file_path = create_data_file_name(dir_path, file_id);
        let io_manager = new_io_manager(&file_path)?;
        Ok(Self {
            file_id: Arc::new(RwLock::new(file_id)),
            write_offset: Default::default(),
            io_manager: Box::new(io_manager),
        })
    }

    /// 设置写偏移
    pub fn set_write_offset(&self, offset: u64) {
        *self.write_offset.write() = offset;
    }

    /// 获取写偏移
    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    /// 同步数据文件
    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }

    /// 获取文件id
    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    /// 写入数据
    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // 更新写偏移
        *self.write_offset.write() += n_bytes as u64;
        Ok(n_bytes)
    }

    /// 从给定偏移处读取一条记录
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 读取header，此处读取的header_buf大小为max_log_record_header_size()
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;
        // 取出record type
        let record_type = header_buf.get_u8();
        // 取出key长度
        let key_len = decode_length_delimiter(&mut header_buf).unwrap();
        // 取出value长度
        let value_len = decode_length_delimiter(&mut header_buf).unwrap();
        // 如果key长度和value长度都为0，则表示读取到文件末尾
        if key_len == 0 && value_len == 0 {
            // 读取到文件末尾
            // 在数据库加载时，如果读取到文件末尾，会continue，读取下一个文件的内容
            return Err(Errors::ReadDataFileEof);
        }
        // 计算实际的header大小
        let actual_header_size =
            1 + length_delimiter_len(key_len) + length_delimiter_len(value_len);
        // 读取key，value，CRC
        let mut k_v_crc_buf = BytesMut::zeroed(key_len + value_len + 4);
        self.io_manager
            .read(&mut k_v_crc_buf, offset + actual_header_size as u64)?;
        // 构造log record
        let record = LogRecord {
            key: k_v_crc_buf.get(0..key_len).unwrap().to_vec(),
            value: k_v_crc_buf
                .get(key_len..k_v_crc_buf.len() - 4)
                .unwrap()
                .to_vec(),
            rec_type: record_type.into(),
        };
        // 读取CRC
        k_v_crc_buf.advance(key_len + value_len);
        let crc = k_v_crc_buf.get_u32();
        // 验证CRC
        if record.get_crc() != crc {
            return Err(Errors::InvalidLogRecordCrc);
        }
        Ok(ReadLogRecord {
            record,
            size: (actual_header_size + key_len + value_len + 4) as u64,
        })
    }

    /// 打开或创建hint索引文件
    pub fn new_hint_file(dir_path: &Path) -> Result<Self> {
        let file_name = dir_path.join(HINT_FILE_NAME);
        let io_manager = new_io_manager(&file_name)?;
        Ok(Self {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Default::default(),
            io_manager: Box::new(io_manager),
        })
    }

    /// 打开或创建标识merge完成的文件
    pub fn new_merge_finished_file(dir_path: &Path) -> Result<Self> {
        let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);
        let io_manager = new_io_manager(&file_name)?;
        Ok(Self {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Default::default(),
            io_manager: Box::new(io_manager),
        })
    }

    /// 写入hint索引记录
    pub fn write_hint_record(&self, key: Vec<u8>, record_pos: LogRecordPos) -> Result<()> {
        let hint_record = LogRecord {
            key,
            value: record_pos.encode(),
            rec_type: LogRecordType::Normal,
        };
        let encoded_record = hint_record.encode();
        self.write(&encoded_record)?;
        Ok(())
    }
}

pub(crate) fn create_data_file_name(dir_path: &Path, file_id: u32) -> PathBuf {
    let file_name = format!("{:09}{}", file_id, DATA_FILE_NAME_SUFFIX);
    dir_path.join(file_name)
}

#[cfg(test)]
mod tests {
    use crate::data::log_record::LogRecordType;

    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 0);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 0);
        assert_eq!(data_file.get_write_offset(), 0);
        let file_path = create_data_file_name(&dir_path, 0);
        println!("file_path: {}", file_path.display());
        std::fs::remove_file(file_path).unwrap();

        let data_file_res = DataFile::new(&dir_path, 12);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(data_file.get_file_id(), 12);
        assert_eq!(data_file.get_write_offset(), 0);
        let file_path = create_data_file_name(&dir_path, 12);
        std::fs::remove_file(file_path).unwrap();
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 0);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        let s = b"hello world";
        let write_res = data_file.write(s);
        assert!(write_res.is_ok());
        let write_size = write_res.unwrap();
        assert_eq!(write_size, s.len());
        assert_eq!(data_file.get_write_offset(), s.len() as u64);

        let write_res = data_file.write(s);
        assert!(write_res.is_ok());
        let write_size = write_res.unwrap();
        assert_eq!(write_size, s.len());
        assert_eq!(data_file.get_write_offset(), s.len() as u64 * 2);

        let s = b"aaabbccc";
        let write_res = data_file.write(s);
        assert!(write_res.is_ok());
        let write_size = write_res.unwrap();
        assert_eq!(write_size, s.len());
        assert_eq!(data_file.get_write_offset(), 30);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 111);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        let s = b"hello world";
        let write_res = data_file.write(s);
        assert!(write_res.is_ok());
        let write_size = write_res.unwrap();
        assert_eq!(write_size, s.len());
        assert_eq!(data_file.get_write_offset(), s.len() as u64);
        let sync_res = data_file.sync();
        assert!(sync_res.is_ok());
        let file_path = create_data_file_name(&dir_path, 111);
        println!("file_path: {}", file_path.display());
        std::fs::remove_file(file_path).unwrap();
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        let data_file_res = DataFile::new(&dir_path, 222);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        let record = LogRecord {
            key: "hello".into(),
            value: "world".into(),
            rec_type: LogRecordType::Normal,
        };
        let encoded = record.encode();
        data_file.write(&encoded).unwrap();
        let read_res = data_file.read_log_record(0);
        assert!(read_res.is_ok());
        let read_log_record = read_res.unwrap();
        assert_eq!(read_log_record.record.key, b"hello");
        assert_eq!(read_log_record.record.value, b"world");
        assert_eq!(read_log_record.record.rec_type, LogRecordType::Normal);
        println!("first record length: {:?}", read_log_record.size);

        let record = LogRecord {
            key: "abc".into(),
            value: "123".into(),
            rec_type: LogRecordType::Normal,
        };
        let encoded = record.encode();
        data_file.write(&encoded).unwrap();
        let read_res = data_file.read_log_record(read_log_record.size);
        assert!(read_res.is_ok());
        let read_log_record = read_res.unwrap();
        assert_eq!(read_log_record.record.key, b"abc");
        assert_eq!(read_log_record.record.value, b"123");
        assert_eq!(read_log_record.record.rec_type, LogRecordType::Normal);
        println!("second record length: {:?}", read_log_record.size);

        let file_path = create_data_file_name(&dir_path, 222);
        println!("file_path: {}", file_path.display());
        std::fs::remove_file(file_path).unwrap();
    }
}
