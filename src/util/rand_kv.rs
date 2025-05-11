#![allow(dead_code)]

use bytes::Bytes;

pub fn get_test_key(i: u32) -> Bytes {
    format!("bitcask_test_key_{}", i).into()
}

pub fn get_test_value(i: u32) -> Bytes {
    format!("bitcask_test_value_{}", i).into()
}
