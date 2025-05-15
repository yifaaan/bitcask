#![allow(dead_code)]

use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{Arc, atomic::AtomicUsize},
};

use bytes::Bytes;
use fs2::FileExt;
use log::{error, warn};
use parking_lot::{Mutex, RwLock};

use crate::{
    batch::{
        NON_TRANSACTION_SEQ_NUMBER, get_record_sequence_number_with_key,
        parse_record_sequence_number_with_key,
    },
    data::{
        data_file::{
            DATA_FILE_NAME_SUFFIX, DataFile, MERGE_FINISHED_FILE_NAME, SEQUENCE_NUMBER_FILE_NAME,
        },
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord},
    },
    errors::{Errors, Result},
    index::{Indexer, new_indexer},
    merge::load_merge_files,
    options::{IndexType, Options},
};

const INITIAL_DATA_FILE_ID: u32 = 0;
const SEQUENCE_NUMBER_KEY: &str = "sequence.number";
pub(crate) const FILE_LOCK_NAME: &str = "file-lock";

pub struct Engine {
    /// 配置
    pub(crate) options: Arc<Options>,
    /// 活跃数据文件
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    /// 旧数据文件
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 内存索引
    pub(crate) index: Box<dyn Indexer>,
    /// 文件id,只用于启动时加载索引使用
    file_ids: Vec<u32>,
    /// 批量写入互斥锁
    pub(crate) batch_commit_mutex: Mutex<()>,
    /// 序列号
    pub(crate) sequence_number: Arc<AtomicUsize>,
    /// 防止多个线程同时merge
    pub(crate) merge_lock: Mutex<()>,
    /// 事务序列号文件是否存在
    pub(crate) sequence_number_file_exists: bool,
    /// 是否是首次加载db
    pub(crate) is_first_load: bool,
    /// 文件锁,保证在db目录只打开一个db实例
    pub(crate) lock_file: File,
    /// 累计写入阈值
    pub(crate) bytes_write: Arc<AtomicUsize>,
}

impl Engine {
    pub fn open(opts: Options) -> Result<Self> {
        check_options(&opts)?;
        // 判断目录是否存在
        let dir_path = opts.dir_path.clone();
        let mut is_first_load = false;
        if !dir_path.is_dir() {
            // println!(
            //     "Database dir not found, creating dir: {}",
            //     dir_path.display()
            // );
            is_first_load = true;
            std::fs::create_dir_all(&dir_path).map_err(|e| {
                warn!("Failed to create database dir: {}", e);
                Errors::FailedToCreateDatabaseDir
            })?;
        }

        // 判断db目录是否正被使用中
        // 打开或创建文件锁
        let lock_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir_path.join(FILE_LOCK_NAME))
            .map_err(|e| {
                warn!("Failed to create file lock: {}", e);
                Errors::FailedToCreateFileLock
            })?;
        if lock_file.try_lock_exclusive().is_err() {
            return Err(Errors::DatabaseIsUsing);
        }

        // 空目录也认为是首次加载
        let entries = std::fs::read_dir(&dir_path).expect("Failed to read database dir");
        if entries.count() == 0 {
            is_first_load = true;
        }

        // 加载merge目录,删除已merge的数据文件，将已merge的数据文件移动到当前db
        load_merge_files(&dir_path)?;

        let mut data_files = load_data_files(&dir_path)?;
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
            None => DataFile::new(&dir_path, INITIAL_DATA_FILE_ID)?,
        };
        let idx_type = opts.index_type;
        let mut engine = Self {
            options: Arc::new(opts.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: new_indexer(idx_type, &dir_path),
            file_ids,
            batch_commit_mutex: Mutex::new(()),
            sequence_number: Arc::new(AtomicUsize::new(1)),
            merge_lock: Mutex::new(()),
            sequence_number_file_exists: false,
            is_first_load,
            lock_file,
            bytes_write: Default::default(),
        };

        // B+Tree索引，不需要从数据文件加载索引
        if opts.index_type != IndexType::BPlusTree {
            // 读取merge目录，从索引文件hint中，加载内存索引
            engine.load_index_from_hint_file()?;

            // 读取数据文件来加载内存索引
            let seq_number = engine.load_index_from_data_files()?;
            if seq_number > NON_TRANSACTION_SEQ_NUMBER {
                engine
                    .sequence_number
                    .store(seq_number + 1, std::sync::atomic::Ordering::SeqCst); // 更新到下一个事务序列号
            }
        }

        if opts.index_type == IndexType::BPlusTree {
            // 从sequence number文件中，加载事务序列号
            let (exists, seq_number) = engine.load_sequence_number_from_file();
            engine.sequence_number_file_exists = exists;
            engine
                .sequence_number
                .store(seq_number, std::sync::atomic::Ordering::SeqCst);
            // 设置活跃文件的写偏移
            let active_file = engine.active_file.write();
            active_file.set_write_offset(active_file.file_size());
        }
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
        if !self.options.dir_path.is_dir() {
            return Ok(());
        }
        // 写入事务序列号
        let sequence_number_file = DataFile::new_sequence_number_file(&self.options.dir_path)?;
        let record = LogRecord {
            key: SEQUENCE_NUMBER_KEY.as_bytes().to_vec(),
            value: self
                .sequence_number
                .load(std::sync::atomic::Ordering::SeqCst)
                .to_string()
                .into_bytes(),
            rec_type: LogRecordType::Normal,
        };
        sequence_number_file.sync()?;

        sequence_number_file.write(&record.encode())?;
        self.active_file.read().sync()?;
        fs2::FileExt::unlock(&self.lock_file).map_err(|e| {
            warn!("Failed to unlock file lock: {}", e);
            Errors::FailedToUnlockFileLock
        })?;
        Ok(())
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

        let previous = self
            .bytes_write
            .fetch_add(record_len, std::sync::atomic::Ordering::SeqCst);
        // 根据配置项，决定是否立刻持久化活跃数据文件
        let mut need_sync = self.options.sync_write;
        if !need_sync
            && self.options.bytes_per_sync > 0
            && previous + record_len >= self.options.bytes_per_sync
        {
            need_sync = true;
        }
        if need_sync {
            active_file.sync()?;
            // 累计值置为0
            self.bytes_write
                .store(0, std::sync::atomic::Ordering::SeqCst);
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
    fn load_index_from_data_files(&mut self) -> Result<usize> {
        // 最新的事务序列号
        let mut current_seq_number = NON_TRANSACTION_SEQ_NUMBER;
        if self.file_ids.is_empty() {
            return Ok(current_seq_number);
        }

        let mut unmerged_file_id = 0;
        let mut has_merge = false;
        let merge_finished_file_name = self.options.dir_path.join(MERGE_FINISHED_FILE_NAME);
        // 如果merge完成文件存在，则从不用从已被merge的文件中加载索引
        if merge_finished_file_name.is_file() {
            let merge_finished_file = DataFile::new_merge_finished_file(&self.options.dir_path)?;
            let read_log_record = merge_finished_file.read_log_record(0)?;
            unmerged_file_id = String::from_utf8(read_log_record.record.value)
                .unwrap()
                .parse::<u32>()?;
            has_merge = true;
        }

        let mut transaction_records: HashMap<usize, Vec<TransactionRecord>> = HashMap::new();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        for (i, file_id) in self.file_ids.iter().enumerate() {
            // 文件id小于unmerged_file_id，说明已经从hint索引文件中加载过索引，跳过
            if has_merge && *file_id < unmerged_file_id {
                continue;
            }
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
                            // 根据事务序列号，插入对应的分组,将其暂存到内存，知道读到对应的TxnFinished记录，才将该组记录插入索引
                            transaction_records.entry(seq_number).or_default().push(
                                TransactionRecord {
                                    record,
                                    position: record_pos,
                                },
                            );
                        }
                    }
                }
                current_seq_number = current_seq_number.max(seq_number);
                // 更新偏移量
                offset += record_size;
            }
            // 如果是最后一个文件，更新活跃数据文件的偏移量
            if i == self.file_ids.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }
        Ok(current_seq_number)
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

    fn load_sequence_number_from_file(&self) -> (bool, usize) {
        let file_name = self.options.dir_path.join(SEQUENCE_NUMBER_FILE_NAME);
        if !file_name.is_file() {
            return (false, 0);
        }
        let sequence_number_file = DataFile::new_sequence_number_file(&self.options.dir_path)
            .expect("Failed to create sequence number file");
        let record = match sequence_number_file.read_log_record(0) {
            Ok(v) => v.record,
            Err(e) => {
                error!("Failed to read sequence number file: {}", e);
                return (false, 0);
            }
        };
        let seq_number = String::from_utf8(record.value)
            .unwrap()
            .parse::<usize>()
            .unwrap();
        std::fs::remove_file(file_name).unwrap();
        (true, seq_number)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("Failed to close engine: {}", e);
        }
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
            bytes_per_sync: 1000000,
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
            bytes_per_sync: 1000000,
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
            bytes_per_sync: 1000000,
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

    #[test]
    fn test_db_sync() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_db_sync"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            bytes_per_sync: 1000000,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();

        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");
        engine
            .put(get_test_key(1), get_test_value(1))
            .expect("Failed to put data");
        engine.sync().expect("Failed to sync");
        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_db_file_lock() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_db_file_lock"),
            data_file_size: 8 * 1024 * 1024,
            sync_write: false,
            bytes_per_sync: 100,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();

        let engine = Engine::open(engine_opts.clone()).expect("Failed to open engine");
        let engine_res = Engine::open(engine_opts.clone());
        assert!(engine_res.is_err());
        // 关闭后，可以重新打开
        engine.close().expect("Failed to close engine");
        let engine_res = Engine::open(engine_opts.clone());
        assert!(engine_res.is_ok());

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }
}
