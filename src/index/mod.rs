#![allow(dead_code)]

pub mod btree;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    errors::Result,
    options::{IndexType, IteratorOptions},
};

/// Abstract indexer, for different index types
pub trait Indexer: Send + Sync {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
    fn list_keys(&self) -> Result<Vec<Bytes>>;
}

pub fn new_indexer(idx_type: IndexType) -> Box<dyn Indexer> {
    match idx_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        IndexType::SkipList => todo!(),
    }
}

pub trait IndexIterator: Send + Sync {
    /// 重置迭代器，定位到起点
    fn rewind(&mut self);
    /// 定位到第一个大于（或小于）等于key的记录
    fn seek(&mut self, key: Vec<u8>);
    /// 获取下一个记录，如果迭代器已经到达末尾，则返回None
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
