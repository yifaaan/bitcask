#![allow(dead_code)]

pub mod btree;
use crate::{data::log_record::LogRecordPos, options::IndexType};

/// Abstract indexer, for different index types
pub trait Indexer: Send + Sync {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
}

pub fn new_indexer(idx_type: IndexType) -> Box<dyn Indexer> {
    match idx_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        IndexType::SkipList => todo!(),
    }
}
