#![allow(dead_code)]
#![allow(unused_variables)]
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{db::Engine, errors::Result, index::IndexIterator, options::IteratorOptions};

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, opts: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(opts))),
            engine: self,
        }
    }

    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        F: Fn(Bytes, Bytes) -> bool,
    {
        let mut iter = self.iter(IteratorOptions::default());
        while let Some((k, v)) = iter.next() {
            if !f(k, v) {
                break;
            }
        }
        Ok(())
    }
}

impl Iterator<'_> {
    /// 重置迭代器，定位到起点
    fn rewind(&mut self) {
        self.index_iter.write().rewind();
    }

    /// 定位到第一个大于（或小于）等于key的记录
    fn seek(&mut self, key: Vec<u8>) {
        self.index_iter.write().seek(key);
    }

    /// 获取下一个记录，如果迭代器已经到达末尾，则返回None
    fn next(&mut self) -> Option<(Bytes, Bytes)> {
        let mut write_guard = self.index_iter.write();
        if let Some((key, pos)) = write_guard.next() {
            let value = self
                .engine
                .get_value_by_position(pos)
                .expect("Failed to get value from data file");
            return Some((key.clone().into(), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        options::{IndexType, Options},
        util::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;

    #[test]
    fn test_iterator_seek() {
        let iter_opts = IteratorOptions::default();
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_iterator_seek"),
            data_file_size: 1024 * 1024,
            sync_write: true,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts).expect("Failed to open engine");

        // Empty
        let mut iter = engine.iter(iter_opts.clone());
        iter.seek("hello".into());
        assert!(iter.next().is_none());

        // 插入一些数据
        engine
            .put(get_test_key(0), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put(get_test_key(1), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put(get_test_key(2), get_test_value(2))
            .expect("Failed to put data");
        engine
            .put(get_test_key(3), get_test_value(3))
            .expect("Failed to put data");
        engine
            .put(get_test_key(4), get_test_value(4))
            .expect("Failed to put data");
        let mut iter = engine.iter(iter_opts.clone());
        iter.seek(get_test_key(0).to_vec());
        let result = iter.next();
        assert_eq!(result, Some((get_test_key(0), get_test_value(0))));
        let result = iter.next();
        assert_eq!(result, Some((get_test_key(1), get_test_value(1))));
        let result = iter.next();
        assert_eq!(result, Some((get_test_key(2), get_test_value(2))));
        let result = iter.next();
        assert_eq!(result, Some((get_test_key(3), get_test_value(3))));
        let result = iter.next();
        assert_eq!(result, Some((get_test_key(4), get_test_value(4))));

        engine
            .put("aaa".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaab".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaac".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("bbb".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("bbbb".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("bbbc".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("ccc".into(), get_test_value(2))
            .expect("Failed to put data");
        engine
            .put("ddd".into(), get_test_value(3))
            .expect("Failed to put data");
        engine
            .put("eee".into(), get_test_value(4))
            .expect("Failed to put data");
        engine
            .put("z".into(), get_test_value(5))
            .expect("Failed to put data");
        let mut iter = engine.iter(iter_opts.clone());
        iter.seek("aaa".into());
        while let Some((key, value)) = iter.next() {
            // println!(
            //     "key: {}, value: {}",
            //     String::from_utf8(key.to_vec()).unwrap(),
            //     String::from_utf8(value.to_vec()).unwrap()
            // );
        }

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_iterator_prefix() {
        let engine_opts = Options {
            dir_path: std::env::temp_dir().join("test_iterator_prefix"),
            data_file_size: 1024 * 1024,
            sync_write: true,
            index_type: IndexType::BTree,
        };
        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts).expect("Failed to open engine");

        engine
            .put("aaa".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaab".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaac".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("bbb".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("bbbb".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("bbbc".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("ccc".into(), get_test_value(2))
            .expect("Failed to put data");
        engine
            .put("ddd".into(), get_test_value(3))
            .expect("Failed to put data");
        engine
            .put("eee".into(), get_test_value(4))
            .expect("Failed to put data");
        engine
            .put("z".into(), get_test_value(5))
            .expect("Failed to put data");
        // prefix
        let opts = IteratorOptions {
            reverse: false,
            prefix: "aa".into(),
        };

        let mut iter = engine.iter(opts);
        iter.seek("aa".into());
        while let Some((key, value)) = iter.next() {
            // println!(
            //     "key: {}, value: {}",
            //     String::from_utf8(key.to_vec()).unwrap(),
            //     String::from_utf8(value.to_vec()).unwrap()
            // );
        }
        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_iterator_reverse() {
        let mut engine_opts = Options {
            dir_path: Default::default(),
            data_file_size: 1024 * 1024,
            sync_write: true,
            index_type: IndexType::BTree,
        };
        engine_opts.dir_path = std::env::temp_dir().join("test_iterator_reverse");

        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts).expect("Failed to open engine");

        engine
            .put("aaa".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaab".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaac".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("bbb".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("bbbb".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("bbbc".into(), get_test_value(1))
            .expect("Failed to put data");
        engine
            .put("ccc".into(), get_test_value(2))
            .expect("Failed to put data");
        engine
            .put("ddd".into(), get_test_value(3))
            .expect("Failed to put data");
        engine
            .put("eee".into(), get_test_value(4))
            .expect("Failed to put data");
        engine
            .put("z".into(), get_test_value(5))
            .expect("Failed to put data");

        // 逆序迭代
        let opts = IteratorOptions {
            reverse: true,
            ..Default::default()
        };
        let mut iter = engine.iter(opts);
        iter.seek("z".into());
        while let Some((key, value)) = iter.next() {
            // println!(
            //     "key: {}, value: {}",
            //     String::from_utf8(key.to_vec()).unwrap(),
            //     String::from_utf8(value.to_vec()).unwrap()
            // );
        }

        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_list_keys() {
        let mut engine_opts = Options {
            dir_path: Default::default(),
            data_file_size: 1024 * 1024,
            sync_write: true,
            index_type: IndexType::BTree,
        };
        engine_opts.dir_path = std::env::temp_dir().join("test_iterator_list_keys");

        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts).expect("Failed to open engine");

        assert!(engine.list_keys().unwrap().is_empty());

        engine
            .put("aaa".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaab".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaac".into(), get_test_value(0))
            .expect("Failed to put data");
        assert!(engine.list_keys().unwrap().len() == 3);
        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }

    #[test]
    fn test_fold() {
        let mut engine_opts = Options {
            dir_path: Default::default(),
            data_file_size: 1024 * 1024,
            sync_write: true,
            index_type: IndexType::BTree,
        };
        engine_opts.dir_path = std::env::temp_dir().join("test_iterator_fold");

        let engine_dir = engine_opts.dir_path.clone();
        let engine = Engine::open(engine_opts).expect("Failed to open engine");

        engine
            .put("aaa".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaab".into(), get_test_value(0))
            .expect("Failed to put data");
        engine
            .put("aaac".into(), get_test_value(0))
            .expect("Failed to put data");

        engine
            .fold(|k, v| {
                println!("key:{:?}, value:{:?}", k, v);
                true
            })
            .unwrap();
        std::fs::remove_dir_all(engine_dir).expect("Failed to remove test directory");
    }
}
