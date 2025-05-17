#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use bytes::{BufMut, Bytes, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::data::log_record::{LogRecord, LogRecordType};
use crate::db::Engine;
use crate::errors::{Errors, Result};
use crate::options::{IndexType, WriteBatchOptions};

const TX_FIN_KEY: &[u8] = b"txn-fin";
pub(crate) const NON_TRANSACTION_SEQ_NUMBER: usize = 0;

/// 批量写入，原子操作
pub struct WriteBatch<'a> {
    /// 待写入的记录
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    /// 选项
    options: WriteBatchOptions,
    /// 引擎
    engine: &'a Engine,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        if !self.sequence_number_file_exists
            && self.options.index_type == IndexType::BPlusTree
            && !self.is_first_load
        {
            return Err(Errors::UnableToUseWriteBatch);
        }
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            options,
            engine: self,
        })
    }
}

impl WriteBatch<'_> {
    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: crate::data::log_record::LogRecordType::Normal,
        };
        self.pending_writes.lock().insert(key.to_vec(), record);
        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut pending_writes = self.pending_writes.lock();
        // 索引中不存在，直接在pending_writes中删除
        if self.engine.index.get(key.to_vec()).is_none() {
            pending_writes.remove(key.as_ref());
            return Ok(());
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: vec![],
            rec_type: crate::data::log_record::LogRecordType::Deleted,
        };
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        if self.pending_writes.lock().is_empty() {
            return Ok(());
        }
        if self.pending_writes.lock().len() > self.options.max_batch_size {
            return Err(Errors::BatchSizeExceeded);
        }

        // 加锁，防止多个写入操作同时进行
        let batch_commit_lock = self.engine.batch_commit_mutex.lock();
        // 更新到下一个事务序列号
        let sequence_number = self
            .engine
            .sequence_number
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut pending_writes = self.pending_writes.lock();
        let mut positions = HashMap::new();
        for (key, record) in pending_writes.iter() {
            let mut record = LogRecord {
                key: get_record_sequence_number_with_key(key, sequence_number),
                value: record.value.clone(),
                rec_type: record.rec_type,
            };
            // 写入数据文件
            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(key.clone(), pos);
        }
        // 最后一条记录表示事务完成
        let mut finished_record = LogRecord {
            key: get_record_sequence_number_with_key(TX_FIN_KEY, sequence_number),
            value: vec![],
            rec_type: LogRecordType::TxnFinished,
        };
        self.engine.append_log_record(&mut finished_record)?;

        // 同步写入
        if self.options.sync_write {
            self.engine.sync()?;
        }

        // 写入index
        for (_, record) in pending_writes.drain() {
            match record.rec_type {
                LogRecordType::Normal => {
                    let pos = positions.get(&record.key).unwrap();
                    self.engine.index.put(record.key.clone(), *pos);
                }
                LogRecordType::Deleted => {
                    self.engine.index.delete(record.key);
                }
                LogRecordType::TxnFinished => {}
            }
        }
        Ok(())
    }
}

/// 获取带序列号的记录key
pub(crate) fn get_record_sequence_number_with_key(key: &[u8], sequence_number: usize) -> Vec<u8> {
    let mut key_seq_buf = BytesMut::new();
    encode_length_delimiter(sequence_number, &mut key_seq_buf).unwrap();
    key_seq_buf.extend_from_slice(key);
    key_seq_buf.to_vec()
}

/// 解析带序列号的记录key
pub(crate) fn parse_record_sequence_number_with_key(key: &[u8]) -> (usize, Vec<u8>) {
    let mut seq_key_buf = BytesMut::new();
    seq_key_buf.put(key);
    let seq_number = decode_length_delimiter(&mut seq_key_buf).unwrap();
    (seq_number, seq_key_buf.to_vec())
}

#[cfg(test)]
mod tests {
    use crate::{
        options::{IndexType, Options},
        util::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;

    #[test]
    fn test_write_batch() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_db_write_batch"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            bytes_per_sync: 100,
            index_type: IndexType::BTree,
            use_mmap: false,
        };
        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");
        let mut write_batch = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("Failed to create write batch");
        // 写入后未提交
        let put_res = write_batch.put("k1".into(), "v1".into());
        assert_eq!(put_res, Ok(()));
        let put_res = write_batch.put("k2".into(), "v2".into());
        assert_eq!(put_res, Ok(()));
        let get_res = engine.get("k1".into());
        assert!(get_res.is_err());

        // 提交
        let commit_res = write_batch.commit();
        assert_eq!(commit_res, Ok(()));
        let get_res = engine.get("k1".into());
        assert_eq!(get_res, Ok("v1".into()));

        assert_eq!(
            2,
            engine
                .sequence_number
                .load(std::sync::atomic::Ordering::SeqCst)
        );

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove engine dir");
    }

    #[test]
    fn test_write_batch_reopen() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_write_batch_reopen"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            bytes_per_sync: 1000000,
            index_type: IndexType::BTree,
            use_mmap: false,
        };
        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");
        let mut write_batch = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("Failed to create write batch");
        // 写入后未提交
        let put_res = write_batch.put("k1".into(), "v1".into());
        assert_eq!(put_res, Ok(()));
        let put_res = write_batch.put("k2".into(), "v2".into());
        assert_eq!(put_res, Ok(()));
        let get_res = engine.get("k1".into());
        assert_eq!(get_res, Err(Errors::KeyNotFound));

        // 提交
        let commit_res = write_batch.commit();
        assert_eq!(commit_res, Ok(()));
        let get_res = engine.get("k1".into());
        assert_eq!(get_res, Ok("v1".into()));

        engine.close().expect("Failed to close");

        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");
        assert_eq!(
            2,
            engine
                .sequence_number
                .load(std::sync::atomic::Ordering::SeqCst)
        );

        let mut write_batch = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("Failed to create write batch");
        let put_res = write_batch.put("k3".into(), "v3".into());
        assert_eq!(put_res, Ok(()));
        let put_res = write_batch.put("k4".into(), "v4".into());
        assert_eq!(put_res, Ok(()));
        let commit_res = write_batch.commit();
        assert_eq!(commit_res, Ok(()));
        write_batch.commit().expect("Failed to commit");

        assert_eq!(
            3,
            engine
                .sequence_number
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        let get_res = engine.get("k3".into());
        assert_eq!(get_res, Ok("v3".into()));

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove engine dir");
    }

    #[test]
    fn test_write_batch_shutdown() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_write_batch_shutdown"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            bytes_per_sync: 1000000,
            index_type: IndexType::BTree,
            use_mmap: true,
        };
        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");
        let mut write_batch = engine
            .new_write_batch(WriteBatchOptions {
                max_batch_size: 10000000,
                sync_write: false,
            })
            .expect("Failed to create write batch");

        for i in 0..1000000 {
            write_batch
                .put(get_test_key(i), get_test_value(i))
                .expect("Failed to put put");
        }

        write_batch.commit().expect("Failed to commit");

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove engine dir");
    }
}
