use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use crate::data::log_record::LogRecordPos;

use super::Indexer;

/// Btree Indexer
#[derive(Default)]
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }
    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key).is_some()
    }
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        assert!(bt.put(
            "".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        ),);
        assert!(bt.put(
            "bbc".as_bytes().into(),
            LogRecordPos {
                file_id: 11,
                offset: 11,
            },
        ),);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        bt.put(
            "".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "bbc".as_bytes().into(),
            LogRecordPos {
                file_id: 11,
                offset: 11,
            },
        );
        assert_eq!(
            bt.get("".as_bytes().into()),
            Some(LogRecordPos {
                file_id: 1,
                offset: 10
            })
        );
        assert_eq!(
            bt.get("bbc".as_bytes().into()),
            Some(LogRecordPos {
                file_id: 11,
                offset: 11,
            })
        );
    }

    #[test]
    fn test_btree_delete() {
        let bt = BTree::new();
        bt.put(
            "".as_bytes().into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "bbc".as_bytes().into(),
            LogRecordPos {
                file_id: 11,
                offset: 11,
            },
        );
        assert!(bt.delete("".as_bytes().into()));
        assert_eq!(bt.get("".as_bytes().into()), None);
        assert_eq!(
            bt.get("bbc".as_bytes().into()),
            Some(LogRecordPos {
                file_id: 11,
                offset: 11,
            })
        );
    }
}
