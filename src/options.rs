#![allow(dead_code)]

use std::path::PathBuf;

const DEFAULT_DATA_FILE_SIZE_BYTES: u64 = 256 * 1024 * 1024; // 256MB

/// 数据库选项
#[derive(Clone, Debug)]
pub struct Options {
    /// 数据库目录
    pub(crate) dir_path: PathBuf,
    /// 数据文件大小
    pub(crate) data_file_size: u64,
    /// 是否立刻持久化
    pub(crate) sync_write: bool,
    /// 累计写入阈值后再持久化
    pub(crate) bytes_per_sync: usize,
    /// 索引类型
    pub(crate) index_type: IndexType,
    /// 是否使用mmap打开数据文件
    pub(crate) use_mmap: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: PathBuf::default(),
            data_file_size: DEFAULT_DATA_FILE_SIZE_BYTES,
            sync_write: false,
            bytes_per_sync: 0,
            index_type: IndexType::BPlusTree,
            use_mmap: true,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum IndexType {
    BTree,
    SkipList,
    BPlusTree,
}

/// 迭代器选项
#[derive(Default, Clone)]
pub struct IteratorOptions {
    /// 是否逆序
    pub(crate) reverse: bool,
    /// 前缀
    pub(crate) prefix: Vec<u8>,
}

/// 批量写入选项
#[derive(Clone, Copy)]
pub struct WriteBatchOptions {
    /// 批量写入的记录数
    pub(crate) max_batch_size: usize,
    /// 是否立刻持久化
    pub(crate) sync_write: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_size: 8192,
            sync_write: false,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum IOType {
    StandardFileIO,
    MmapIO,
}
