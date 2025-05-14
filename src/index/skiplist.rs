use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::{data::log_record::LogRecordPos, options::IteratorOptions};

use super::Indexer;

pub struct SkipList {
    skip_list: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            skip_list: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        self.skip_list.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        self.skip_list.get(&key).map(|entry| *entry.value())
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        self.skip_list.remove(&key).is_some()
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        Ok(self
            .skip_list
            .iter()
            .map(|entry| entry.key().clone().into())
            .collect())
    }

    fn iterator(&self, options: crate::options::IteratorOptions) -> Box<dyn super::IndexIterator> {
        let mut items = self
            .skip_list
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect::<Vec<_>>();
        if options.reverse {
            items.reverse();
        }
        Box::new(SkipListIterator {
            items,
            idx: 0,
            options,
        })
    }
}

pub struct SkipListIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    idx: usize,
    options: IteratorOptions,
}

impl super::IndexIterator for SkipListIterator {
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
    fn test_skiplist_iterator() {
        let skl = SkipList::new();

        // 空 iterator
        let mut iter = skl.iterator(IteratorOptions::default());
        iter.seek("a".into());
        assert!(iter.next().is_none());

        // 一条记录
        skl.put(
            "a".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = skl.iterator(IteratorOptions::default());
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
        skl.put(
            "aa".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        skl.put(
            "ab".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        skl.put(
            "ac".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        skl.put(
            "aaa".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        skl.put(
            "aac".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        skl.put(
            "b".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        skl.put(
            "by".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iter = skl.iterator(IteratorOptions::default());
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

        skl.delete("aaa".into());
        // reverse iterator
        let mut iter = skl.iterator(IteratorOptions {
            reverse: true,
            ..Default::default()
        });
        while let Some((k, _)) = iter.next() {
            // println!("{}", String::from_utf8(k.clone()).unwrap());
            assert!(!k.is_empty());
        }
        // prefix iterator
        let mut iter = skl.iterator(IteratorOptions {
            prefix: "b".into(),
            ..Default::default()
        });
        while let Some((k, _)) = iter.next() {
            // println!("{}", String::from_utf8(k.clone()).unwrap());
            assert!(k.starts_with(b"b"));
        }
    }
}
