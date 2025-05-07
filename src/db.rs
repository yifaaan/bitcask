#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index::Indexer,
    options::Options,
};

pub struct Engine {
    /// 配置
    options: Arc<Options>,
    /// 活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    /// 旧数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 内存索引
    index: Box<dyn Indexer>,
}

impl Engine {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::Normal,
        };
        // 写入活跃数据文件
        let record_position = self.append_log_record(&mut record)?;
        // 更新内存索引
        if !self.index.put(key.to_vec(), record_position) {
            return Err(Errors::FailedToUpdateIndex);
        }
        Ok(())
    }
    /// 将记录追加写到活跃数据文件，返回写入到文件的起始位置
    fn append_log_record(&self, record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.as_path();
        let encoded_record = record.encode();
        let record_len = encoded_record.len();
        // 获取当前活跃数据文件
        let mut active_file = self.active_file.write();
        // 活跃数据文件大小如果超过阈值，需要创建新文件
        if active_file.get_write_offset() + record_len as u64 > self.options.data_file_size {
            // 持久化活跃数据文件
            active_file.sync()?;
            let current_file_id = active_file.get_file_id();
            let old_active_file = DataFile::new(dir_path, current_file_id)?;
            self.older_files
                .write()
                .insert(current_file_id, old_active_file);
            // 创建新的活跃数据文件
            let new_active_file = DataFile::new(dir_path, current_file_id + 1)?;
            *active_file = new_active_file;
        }
        // 写入记录
        let write_offset = active_file.get_write_offset();
        active_file.write(&encoded_record)?;
        // 根据配置项，决定是否立刻持久化活跃数据文件
        if self.options.sync_write {
            active_file.sync()?;
        }
        // 返回写入位置
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_offset,
        })
    }
}
