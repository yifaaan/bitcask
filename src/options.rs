#![allow(dead_code)]

use std::path::PathBuf;

const DEFAULT_DATA_FILE_SIZE_BYTES: u64 = 256 * 1024 * 1024; // 256MB

pub struct Options {
    /// 数据库目录
    pub(crate) dir_path: PathBuf,
    /// 数据文件大小
    pub(crate) data_file_size: u64,
    /// 是否立刻持久化
    pub(crate) sync_write: bool,
    /// 索引类型
    pub(crate) index_type: IndexType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: PathBuf::default(),
            data_file_size: DEFAULT_DATA_FILE_SIZE_BYTES,
            sync_write: false,
            index_type: IndexType::BTree,
        }
    }
}

#[derive(Copy, Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}

/// 迭代器选项
#[derive(Default, Clone)]
pub struct IteratorOptions {
    /// 是否逆序
    pub(crate) reverse: bool,
    /// 前缀
    pub(crate) prefix: Vec<u8>,
}
