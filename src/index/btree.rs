use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use crate::{data::log_record::LogRecordPos, options::IteratorOptions};

use super::{IndexIterator, Indexer};

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

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = read_guard
            .iter()
            .map(|(k, p)| (k.clone(), *p))
            .collect::<Vec<_>>();
        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            idx: 0,
            options,
        })
    }
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Default::default(),
        }
    }
}

pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    idx: usize,
    options: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
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

    #[test]
    fn test_btree_iterator() {
        let bt = BTree::new();

        // 空 iterator
        let mut iter = bt.iterator(IteratorOptions::default());
        iter.seek("a".into());
        assert!(iter.next().is_none());

        // 一条记录
        bt.put(
            "a".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = bt.iterator(IteratorOptions::default());
        iter.seek("a".into());
        assert_eq!(
            iter.next(),
            Some((
                &"a".into(),
                &LogRecordPos {
                    file_id: 1,
                    offset: 10,
                }
            ))
        );

        // 多个记录
        bt.put(
            "aa".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "ab".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "ac".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aaa".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aac".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "b".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "by".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iter = bt.iterator(IteratorOptions::default());
        while let Some((k, _)) = iter.next() {
            // println!("{}", String::from_utf8(k.clone()).unwrap());
            assert!(!k.is_empty());
        }
        iter.rewind();
        iter.seek("aac".into());
        while let Some((k, _)) = iter.next() {
            // println!("{}", String::from_utf8(k.clone()).unwrap());
            assert!(!k.is_empty());
        }

        // reverse iterator
        let mut iter = bt.iterator(IteratorOptions {
            reverse: true,
            ..Default::default()
        });
        while let Some((k, _)) = iter.next() {
            // println!("{}", String::from_utf8(k.clone()).unwrap());
            assert!(!k.is_empty());
        }
        // prefix iterator
        let mut iter = bt.iterator(IteratorOptions {
            prefix: "b".into(),
            ..Default::default()
        });
        while let Some((k, _)) = iter.next() {
            println!("{}", String::from_utf8(k.clone()).unwrap());
            assert!(k.starts_with(b"b"));
        }
    }
}
