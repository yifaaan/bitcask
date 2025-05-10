#![allow(dead_code)]

use std::path::PathBuf;

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

#[derive(Copy, Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}
