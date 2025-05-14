use std::{path::Path, sync::Arc};

use bytes::Bytes;
use jammdb::DB;

use crate::{
    data::log_record::{LogRecordPos, decode_log_record_pos},
    errors::Result,
    options::IteratorOptions,
};

use super::{IndexIterator, Indexer};

const BPTREE_INDEX_FILE_NAME: &str = "bptree-index";
const BPTREE_INDEX_BUCKET_NAME: &str = "bitcask-index";
pub struct BPlusTree {
    tree: Arc<DB>,
}

impl BPlusTree {
    pub fn new(dir_path: &Path) -> Self {
        let tree_path = dir_path.join(BPTREE_INDEX_FILE_NAME);
        let tree = DB::open(tree_path).expect("Failed to open bptree index file");
        let tx = tree
            .tx(true)
            .expect("Failed to create bptree index transaction");
        tx.get_or_create_bucket(BPTREE_INDEX_BUCKET_NAME)
            .expect("Failed to create bptree index bucket");
        tx.commit()
            .expect("Failed to commit bptree index transaction");
        Self {
            tree: Arc::new(tree),
        }
    }
}

impl Indexer for BPlusTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let tx = self
            .tree
            .tx(true)
            .expect("Failed to create bptree index transaction");
        let bucket = tx
            .get_or_create_bucket(BPTREE_INDEX_BUCKET_NAME)
            .expect("Failed to get bptree index bucket");
        bucket
            .put(key, pos.encode())
            .expect("Failed to put bptree index");
        tx.commit()
            .expect("Failed to commit bptree index transaction");
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tx = self
            .tree
            .tx(false)
            .expect("Failed to create bptree index transaction");
        let bucket = tx
            .get_bucket(BPTREE_INDEX_BUCKET_NAME)
            .expect("Failed to get bptree index bucket");
        bucket
            .get_kv(&key)
            .map(|kv| decode_log_record_pos(kv.value()))
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let tx = self
            .tree
            .tx(true)
            .expect("Failed to create bptree index transaction");
        let bucket = tx
            .get_or_create_bucket(BPTREE_INDEX_BUCKET_NAME)
            .expect("Failed to get bptree index bucket");
        if let Err(e) = bucket.delete(key) {
            if e == jammdb::Error::KeyValueMissing {
                return false;
            }
        }
        tx.commit()
            .expect("Failed to commit bptree index transaction");
        true
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let tx = self
            .tree
            .tx(false)
            .expect("Failed to create bptree index transaction");
        let bucket = tx
            .get_bucket(BPTREE_INDEX_BUCKET_NAME)
            .expect("Failed to get bptree index bucket");

        let mut items = bucket
            .kv_pairs()
            .map(|kv| (kv.key().to_vec(), decode_log_record_pos(kv.value())))
            .collect::<Vec<_>>();
        if options.reverse {
            items.reverse();
        }
        Box::new(BPlusTreeIterator {
            items,
            idx: 0,
            options,
        })
    }

    fn list_keys(&self) -> Result<Vec<Bytes>> {
        let tx = self
            .tree
            .tx(false)
            .expect("Failed to create bptree index transaction");
        let bucket = tx
            .get_bucket(BPTREE_INDEX_BUCKET_NAME)
            .expect("Failed to get bptree index bucket");
        Ok(bucket
            .kv_pairs()
            .map(|kv| kv.key().to_vec().into())
            .collect())
    }
}

pub struct BPlusTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    idx: usize,
    options: IteratorOptions,
}

impl IndexIterator for BPlusTreeIterator {
    fn rewind(&mut self) {
        self.idx = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.idx = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(search_idx) => search_idx,
            Err(insert_idx) => insert_idx,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.idx >= self.items.len() {
            return None;
        } else {
            while let Some(item) = self.items.get(self.idx) {
                self.idx += 1;
                if self.options.prefix.is_empty() || item.0.starts_with(&self.options.prefix) {
                    return Some((&item.0, &item.1));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bptree_put() {
        let dir_path = std::env::temp_dir().join("test_bptree_put");
        std::fs::create_dir_all(&dir_path).expect("Failed to create test directory");
        let bpt = BPlusTree::new(&dir_path);
        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        );
        bpt.put(
            "world".into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        );
        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        );
        std::fs::remove_dir_all(&dir_path).expect("Failed to remove test directory");
    }

    #[test]
    fn test_bptree_get() {
        let dir_path = std::env::temp_dir().join("test_bptree_get");
        std::fs::create_dir_all(&dir_path).expect("Failed to create test directory");
        let bpt = BPlusTree::new(&dir_path);

        let empty_get_res = bpt.get("hello".into());
        assert_eq!(empty_get_res, None);

        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        );
        bpt.put(
            "world".into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        );
        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        );

        let get_res = bpt.get("hello".into());
        assert_eq!(
            get_res,
            Some(LogRecordPos {
                file_id: 3,
                offset: 3,
            })
        );

        std::fs::remove_dir_all(&dir_path).expect("Failed to remove test directory");
    }

    #[test]
    fn test_bptree_delete() {
        let dir_path = std::env::temp_dir().join("test_bptree_delete");
        std::fs::create_dir_all(&dir_path).expect("Failed to create test directory");
        let bpt = BPlusTree::new(&dir_path);

        let empty_delete_res = bpt.delete("hello".into());
        assert!(!empty_delete_res);

        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        );
        bpt.put(
            "world".into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        );
        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        );

        let delete_res = bpt.delete("hello".into());
        assert!(delete_res);

        let get_res = bpt.get("hello".into());
        assert_eq!(get_res, None);

        std::fs::remove_dir_all(&dir_path).expect("Failed to remove test directory");
    }

    #[test]
    fn test_bptree_list_keys() {
        let dir_path = std::env::temp_dir().join("test_bptree_list_keys");
        std::fs::create_dir_all(&dir_path).expect("Failed to create test directory");
        let bpt = BPlusTree::new(&dir_path);

        let empty_list_keys_res = bpt.list_keys();
        assert_eq!(empty_list_keys_res, Ok(vec![]));

        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        );
        bpt.put(
            "world".into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        );
        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        );

        let list_keys_res = bpt.list_keys();
        assert_eq!(list_keys_res, Ok(vec!["hello".into(), "world".into()]));

        std::fs::remove_dir_all(&dir_path).expect("Failed to remove test directory");
    }

    #[test]
    fn test_bptree_iterator() {
        let dir_path = std::env::temp_dir().join("test_bptree_iterator");
        std::fs::create_dir_all(&dir_path).expect("Failed to create test directory");
        let bpt = BPlusTree::new(&dir_path);

        bpt.put(
            "hello".into(),
            LogRecordPos {
                file_id: 1,
                offset: 1,
            },
        );
        bpt.put(
            "world".into(),
            LogRecordPos {
                file_id: 2,
                offset: 2,
            },
        );
        bpt.put(
            "abc".into(),
            LogRecordPos {
                file_id: 3,
                offset: 3,
            },
        );

        let mut iter = bpt.iterator(IteratorOptions::default());
        assert_eq!(
            iter.next(),
            Some((
                &"abc".as_bytes().to_vec(),
                &LogRecordPos {
                    file_id: 3,
                    offset: 3,
                }
            ))
        );
        assert_eq!(
            iter.next(),
            Some((
                &"hello".as_bytes().to_vec(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 1,
                }
            ))
        );
        assert_eq!(
            iter.next(),
            Some((
                &"world".as_bytes().to_vec(),
                &LogRecordPos {
                    file_id: 2,
                    offset: 2,
                }
            ))
        );
        assert_eq!(iter.next(), None);

        std::fs::remove_dir_all(&dir_path).expect("Failed to remove test directory");
    }
}
