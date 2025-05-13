#![allow(dead_code)]
#![allow(unused_variables)]

use std::path::{Path, PathBuf};

use log::error;

use crate::{
    batch::{
        NON_TRANSACTION_SEQ_NUMBER, get_record_sequence_number_with_key,
        parse_record_sequence_number_with_key,
    },
    data::{
        data_file::{DataFile, HINT_FILE_NAME, MERGE_FINISHED_FILE_NAME, create_data_file_name},
        log_record::{LogRecord, LogRecordType, decode_log_record_pos},
    },
    db::Engine,
    errors::{Errors, Result},
    options::Options,
};

const MERGE_DIR_SUFFIX: &str = "merge";
const MERGE_FINISHED_KEY: &str = "merge.finished";

impl Engine {
    /// merge 数据目录，处理无效数据，并生成hint索引文件
    pub fn merge(&self) -> Result<()> {
        // 如果正在merge，直接返回，因为只允许单进程merge
        let lock = self.merge_lock.try_lock();
        if lock.is_none() {
            return Err(Errors::MergeInProgress);
        }

        let merge_dir = create_merge_dir(&self.options.dir_path);
        if merge_dir.is_dir() {
            std::fs::remove_dir_all(&merge_dir).map_err(|_| Errors::RemoveDirError)?;
        }
        std::fs::create_dir_all(&merge_dir).map_err(|_| Errors::FailedToCreateDatabaseDir)?;
        // 获取需要merge的数据文件
        let merge_files = self.ratate_merge_files()?;

        // 创建merge engine，依次打开每个数据文件并读取记录，构建hint索引文件
        let opts = Options {
            data_file_size: self.options.data_file_size,
            dir_path: merge_dir.clone(),
            index_type: self.options.index_type,
            ..Default::default()
        };
        let merge_engine = Engine::open(opts)?;

        // 创建hint索引文件,写入hint索引
        let hint_file = DataFile::new_hint_file(&merge_dir)?;
        for data_file in &merge_files {
            let mut offset = 0;
            loop {
                let (mut log_record, size) = match data_file.read_log_record(offset) {
                    Ok(v) => (v.record, v.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEof {
                            // 读取到文件末尾，退出循环,读取下一个文件
                            break;
                        }
                        return Err(e);
                    }
                };
                let (_, real_key) = parse_record_sequence_number_with_key(&log_record.key);
                if let Some(idx_pos) = self.index.get(real_key.clone()) {
                    // 如果索引位置对应的文件id和偏移量都匹配，则是有效记录
                    if idx_pos.file_id == data_file.get_file_id() && idx_pos.offset == offset {
                        // 去除key中事务id
                        log_record.key = get_record_sequence_number_with_key(
                            &real_key,
                            NON_TRANSACTION_SEQ_NUMBER,
                        );
                        // 写入数据文件
                        let record_pos = merge_engine.append_log_record(&mut log_record)?;
                        // 写入hint索引文件
                        hint_file.write_hint_record(real_key, record_pos)?;
                    }
                }
                offset += size;
            }
        }

        // 持久化merge engine
        merge_engine.sync()?;
        // 持久化hint索引文件
        hint_file.sync()?;

        // 原engine的当前活跃数据文件未merge
        let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
        // 创建标识merge完成的文件
        let merge_finished_file = DataFile::new_merge_finished_file(&merge_dir)?;
        let merge_finished_record = LogRecord {
            key: MERGE_FINISHED_KEY.as_bytes().to_vec(),
            value: non_merge_file_id.to_string().into_bytes(),
            rec_type: LogRecordType::Normal,
        };
        let encoded_record = merge_finished_record.encode();
        merge_finished_file.write(&encoded_record)?;
        // 持久化标识merge完成的文件
        merge_finished_file.sync()?;

        Ok(())
    }

    fn ratate_merge_files(&self) -> Result<Vec<DataFile>> {
        let mut merge_file_ids = self.older_files.write().keys().copied().collect::<Vec<_>>();
        let mut active_file = self.active_file.write();
        active_file.sync()?;
        let active_file_id = active_file.get_file_id();
        // 创建新的活跃数据文件，处理写入,将当前活跃数据文件转化为旧数据文件加入到merge列表
        let new_active_file = DataFile::new(&self.options.dir_path, active_file_id + 1)?;
        *active_file = new_active_file;
        let older_file = DataFile::new(&self.options.dir_path, active_file_id)?;
        self.older_files.write().insert(active_file_id, older_file);
        merge_file_ids.push(active_file_id);
        merge_file_ids.sort();
        let mut merge_files = Vec::new();
        for f_id in merge_file_ids {
            merge_files.push(DataFile::new(&self.options.dir_path, f_id)?);
        }
        Ok(merge_files)
    }

    pub fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);
        if !hint_file_name.is_file() {
            return Ok(());
        }
        let hint_file = DataFile::new_hint_file(&self.options.dir_path)?;
        let mut offset = 0;
        loop {
            let (record, size) = match hint_file.read_log_record(offset) {
                Ok(v) => (v.record, v.size),
                Err(e) => {
                    if e == Errors::ReadDataFileEof {
                        break;
                    }
                    return Err(e);
                }
            };
            // hint文件中存储的记录格式为：key+LogRecordPos
            let record_position = decode_log_record_pos(&record.value);
            self.index.put(record.key, record_position);
            offset += size;
        }

        Ok(())
    }
}

fn create_merge_dir(dir_path: &Path) -> PathBuf {
    let dir_str = dir_path.to_str().unwrap();
    format!("{}-{}", dir_str, MERGE_DIR_SUFFIX).into()
}

/// 加载merge目录，读取merge完成文件，删除已merge的数据文件，将已merge的数据文件移动到当前db
pub(crate) fn load_merge_files(dir_path: &Path) -> Result<()> {
    let merge_dir = create_merge_dir(dir_path);
    if !merge_dir.is_dir() {
        return Ok(());
    }
    let dentries = std::fs::read_dir(&merge_dir).map_err(|_| {
        error!("Failed to read merge dir: {}", merge_dir.display());
        Errors::FailedToReadDatabaseDir
    })?;

    let mut merge_finished = false;
    let mut merged_file_names = Vec::new();
    for dentry in dentries {
        let entry = dentry.map_err(|_| {
            error!("Failed to get dentry");
            Errors::FailedToGetDirEntry
        })?;
        let file_name_os = entry.file_name();
        let file_name = file_name_os.to_str().unwrap();
        if file_name.ends_with(MERGE_FINISHED_FILE_NAME) {
            merge_finished = true;
        } else {
            merged_file_names.push(file_name_os);
        }
    }

    // 如果merge未完成，则删除merge目录
    if !merge_finished {
        std::fs::remove_dir_all(&merge_dir).map_err(|_| {
            error!("Failed to remove merge dir: {}", merge_dir.display());
            Errors::RemoveDirError
        })?;
        return Ok(());
    }

    // 如果merge完成，则读取merge完成文件，其中存储未merge的文件id，小于该id的均被merge
    let merge_finished_file = DataFile::new_merge_finished_file(&merge_dir)?;
    let merge_finished_record = merge_finished_file.read_log_record(0)?;
    let unmerge_file_id = String::from_utf8(merge_finished_record.record.value.clone())
        .unwrap()
        .parse::<u32>()
        .unwrap();
    // 从当前db删除已被merge的数据文件
    for f_id in 0..unmerge_file_id {
        let file_name = create_data_file_name(dir_path, f_id);
        if file_name.is_file() {
            std::fs::remove_file(file_name).unwrap();
        }
    }
    // 将已merge的文件移动到当前db
    for file_name in merged_file_names {
        let src = merge_dir.join(&file_name);
        let dst = dir_path.join(&file_name);
        std::fs::rename(src, dst).unwrap();
    }
    // 删除merge目录
    std::fs::remove_dir_all(merge_dir).unwrap();
    Ok(())
}
