#![allow(dead_code)]

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, atomic::AtomicUsize},
};

use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};

use crate::{
    batch::{
        NON_TRANSACTION_SEQ_NUMBER, get_record_sequence_number_with_key,
        parse_record_sequence_number_with_key,
    },
    data::{
        data_file::{DATA_FILE_NAME_SUFFIX, DataFile},
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord},
    },
    errors::{Errors, Result},
    index::{Indexer, new_indexer},
    options::Options,
};

const INITIAL_DATA_FILE_ID: u32 = 0;

pub struct Engine {
    /// 配置
    options: Arc<Options>,
    /// 活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    /// 旧数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 内存索引
    pub(crate) index: Box<dyn Indexer>,
    /// 文件id,只用于启动时加载索引使用
    file_ids: Vec<u32>,
    /// 批量写入互斥锁
    pub(crate) batch_commit_mutex: Mutex<()>,
    /// 序列号
    pub(crate) sequence_number: Arc<AtomicUsize>,
}

impl Engine {
    pub fn open(opts: Options) -> Result<Self> {
        check_options(&opts)?;
        // 判断目录是否存在
        let dir_path = &opts.dir_path;
        if !dir_path.is_dir() {
            std::fs::create_dir_all(dir_path).map_err(|e| {
                warn!("Failed to create database dir: {}", e);
                Errors::FailedToCreateDatabaseDir
            })?;
        }
        let mut data_files = load_data_files(dir_path)?;
        // 新数据文件在开头
        data_files.reverse();
        let file_ids: Vec<_> = data_files.iter().map(|f| f.get_file_id()).rev().collect();
        // 将旧数据文件保存到older_files
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        // 最后一个是活跃数据文件
        let active_file = match data_files.pop() {
            Some(file) => file,
            None => DataFile::new(dir_path, INITIAL_DATA_FILE_ID)?,
        };
        let idx_type = opts.index_type;
        let mut engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: new_indexer(idx_type),
            file_ids,
            batch_commit_mutex: Mutex::new(()),
            sequence_number: Arc::new(AtomicUsize::new(0)),
        };
        // 读取数据文件来加载内存索引
        engine.load_index_from_data_files()?;
        Ok(engine)
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut record = LogRecord {
            // 事务序列号为0，表示非事务提交的记录
            key: get_record_sequence_number_with_key(&key, NON_TRANSACTION_SEQ_NUMBER),
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

    /// 获取指定key的value
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        // 从内存索引获取位置
        let Some(position) = self.index.get(key.to_vec()) else {
            return Err(Errors::KeyNotFound);
        };
        self.get_value_by_position(&position)
    }

    /// 获取指定位置的value
    pub(crate) fn get_value_by_position(&self, position: &LogRecordPos) -> Result<Bytes> {
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == position.file_id {
            true => active_file.read_log_record(position.offset)?.record,
            false => {
                let Some(data_file) = older_files.get(&position.file_id) else {
                    return Err(Errors::DataFileNotFound);
                };
                data_file.read_log_record(position.offset)?.record
            }
        };
        // 判断记录的类型
        if log_record.rec_type == LogRecordType::Deleted {
            return Err(Errors::KeyNotFound);
        }
        Ok(log_record.value.into())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        // 从内存索引查找对应数据，不存在时直接返回
        let Some(_) = self.index.get(key.to_vec()) else {
            return Err(Errors::KeyNotFound);
        };
        // 构造一条删除记录
        let mut record = LogRecord {
            key: get_record_sequence_number_with_key(&key, NON_TRANSACTION_SEQ_NUMBER),
            value: vec![],
            rec_type: LogRecordType::Deleted,
        };
        self.append_log_record(&mut record)?;
        // 从内存索引中删除
        if !self.index.delete(key.to_vec()) {
            return Err(Errors::FailedToUpdateIndex);
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    pub fn close(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    /// 将记录追加写到活跃数据文件，返回写入到文件的起始位置
    pub(crate) fn append_log_record(&self, record: &mut LogRecord) -> Result<LogRecordPos> {
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

    /// 从数据文件加载索引
    /// 1. 遍历数据文件，读取每条记录
    /// 2. 将记录写入索引
    /// 3. 如果是删除记录，则从索引中删除
    /// 4. 如果是正常记录，则将记录写入索引
    fn load_index_from_data_files(&mut self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let mut transaction_records: HashMap<usize, Vec<TransactionRecord>> = HashMap::new();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let read_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();

                        data_file.read_log_record(offset)
                    }
                };
                // 读取记录，和记录在data file中的大小
                // key: 事务序列号+key
                let (mut record, record_size) = match read_record_res {
                    Ok(v) => (v.record, v.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEof {
                            // 读取到文件末尾，退出循环,读取下一个文件
                            break;
                        }
                        return Err(e);
                    }
                };
                // 记录的位置信息
                let record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                let (seq_number, key) = parse_record_sequence_number_with_key(&record.key);
                if seq_number == NON_TRANSACTION_SEQ_NUMBER {
                    // 非事务提交的记录，更新索引
                    self.update_index(key, record.rec_type, record_pos)?;
                } else {
                    match record.rec_type {
                        LogRecordType::TxnFinished => {
                            // 事务结束记录，一次性更新该事务的所有记录的索引
                            let transaction_records =
                                transaction_records.remove(&seq_number).unwrap();
                            for txn_record in transaction_records {
                                self.update_index(
                                    txn_record.record.key,
                                    txn_record.record.rec_type,
                                    txn_record.position,
                                )?;
                            }
                        }
                        _ => {
                            // 去掉事务序列号
                            record.key = key;
                            // 根据事务序列号，插入对应的分组
                            transaction_records.entry(seq_number).or_default().push(
                                TransactionRecord {
                                    record,
                                    position: record_pos,
                                },
                            );
                        }
                    }
                }

                // 更新偏移量
                offset += record_size;
            }
            // 如果是最后一个文件，更新活跃数据文件的偏移量
            if i == self.file_ids.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }
        Ok(())
    }

    fn update_index(
        &self,
        key: Vec<u8>,
        rec_type: LogRecordType,
        record_pos: LogRecordPos,
    ) -> Result<()> {
        // 根据记录类型，更新索引
        if !match rec_type {
            LogRecordType::Normal => self.index.put(key, record_pos),
            LogRecordType::Deleted => self.index.delete(key),
            LogRecordType::TxnFinished => true,
        } {
            return Err(Errors::FailedToUpdateIndex);
        }
        Ok(())
    }
}

fn check_options(opts: &Options) -> Result<()> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Err(Errors::DirPathIsEmpty);
    }
    if opts.data_file_size == 0 {
        return Err(Errors::DataFileSizeIsTooSmall);
    }
    Ok(())
}

fn load_data_files(dir_path: &Path) -> Result<Vec<DataFile>> {
    let d_entries = std::fs::read_dir(dir_path).map_err(|_| Errors::FailedToReadDatabaseDir)?;
    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();

    for entry in d_entries {
        let entry = entry.map_err(|_| Errors::FailedToGetDirEntry)?;
        let file_name = entry.file_name();
        let file_name = file_name.to_str().unwrap();
        // 判断文件名是否以.data结尾
        if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
            // 0000.data
            let split_names: Vec<&str> = file_name.split(".").collect();
            let file_id = split_names[0].parse::<u32>()?;
            file_ids.push(file_id);
        }
    }

    file_ids.sort();

    // 打开数据文件
    for file_id in &file_ids {
        let data_file = DataFile::new(dir_path, *file_id)?;
        data_files.push(data_file);
    }
    Ok(data_files)
}

#[cfg(test)]
mod tests {
    use crate::{
        options::IndexType,
        util::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;
    #[test]
    fn test_db_put() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_db_put"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();

        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");

        // 单条写入
        engine
            .put(get_test_key(1), get_test_value(2))
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(1));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(2));

        // 重复写入key
        engine
            .put(get_test_key(2), get_test_value(2))
            .expect("Failed to put data");
        engine
            .put(get_test_key(2), get_test_value(22))
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(2));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(22));

        // key为空
        let put_res = engine.put(Bytes::new(), get_test_value(3));
        assert_eq!(put_res, Err(Errors::KeyIsEmpty));

        // value为空
        engine
            .put(get_test_key(3), Bytes::new())
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(3));
        assert!(get_res.is_ok());
        assert!(get_res.unwrap().is_empty());

        // 写入大量数据
        for i in 0..1000000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }

        // 重启数据库
        std::mem::drop(engine);
        let engine = Engine::open(engine_opts).expect("Failed to open engine");
        engine
            .put(get_test_key(33), get_test_value(33))
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(33));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(33));
        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_db_get() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_db_get"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();

        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");

        // 单条写入
        engine
            .put(get_test_key(1), get_test_value(2))
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(1));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(2));

        // 重复写入key
        engine
            .put(get_test_key(2), get_test_value(2))
            .expect("Failed to put data");
        engine
            .put(get_test_key(2), get_test_value(22))
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(2));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(22));

        // 获取不存在的key
        let get_res = engine.get(get_test_key(3));
        assert_eq!(get_res, Err(Errors::KeyNotFound));

        // 删除后获取
        engine
            .delete(get_test_key(2))
            .expect("Failed to delete data");
        let get_res = engine.get(get_test_key(2));
        assert_eq!(get_res, Err(Errors::KeyNotFound));

        // 写入大量数据
        for i in 10..1000000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }
        // 从旧数据文件获取
        let get_res = engine.get(get_test_key(100));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(100));
        // 从活跃数据文件获取
        let get_res = engine.get(get_test_key(999999));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(999999));

        // 重启数据库后，获取
        std::mem::drop(engine);
        let engine = Engine::open(engine_opts).expect("Failed to open engine");
        engine
            .put(get_test_key(33), get_test_value(33))
            .expect("Failed to put data");
        let get_res = engine.get(get_test_key(33));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(33));

        // 从旧数据文件获取
        let get_res = engine.get(get_test_key(100));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(100));
        // 从活跃数据文件获取
        let get_res = engine.get(get_test_key(999999));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(999999));

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_db_delete() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_db_delete"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();

        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");

        // 正常删除
        engine
            .put(get_test_key(1), get_test_value(2))
            .expect("Failed to put data");
        let del_res = engine.delete(get_test_key(1));
        assert!(del_res.is_ok());
        let get_res = engine.get(get_test_key(1));
        assert_eq!(get_res, Err(Errors::KeyNotFound));

        // 删除不存在的key
        let del_res = engine.delete(get_test_key(2));
        assert_eq!(del_res, Err(Errors::KeyNotFound));

        // 删除空key
        let del_res = engine.delete(Bytes::new());
        assert_eq!(del_res, Err(Errors::KeyIsEmpty));

        // 写入大量数据
        for i in 10..1000000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }

        // 重启数据库后，删除
        std::mem::drop(engine);
        let engine = Engine::open(engine_opts).expect("Failed to open engine");
        engine
            .delete(get_test_key(100))
            .expect("Failed to delete data");
        let get_res = engine.get(get_test_key(100));
        assert_eq!(get_res, Err(Errors::KeyNotFound));

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }
}
