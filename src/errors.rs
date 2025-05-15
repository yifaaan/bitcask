#![allow(dead_code)]

use thiserror::Error;
pub type Result<T> = std::result::Result<T, Errors>;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug, Eq, PartialEq)]
pub enum Errors {
    #[error("Failed to read from data file")]
    ReadFromDataFileError,

    #[error("Failed to write to data file")]
    WriteToDataFileError,

    #[error("Failed to sync file")]
    SyncFileError,

    #[error("Failed to open file")]
    OpenFileError,

    #[error("Key is empty")]
    KeyIsEmpty,

    #[error("Failed to update index")]
    FailedToUpdateIndex,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Data file not found")]
    DataFileNotFound,

    #[error("Dir path is empty")]
    DirPathIsEmpty,

    #[error("Data file size is too small")]
    DataFileSizeIsTooSmall,

    #[error("Failed to create database dir")]
    FailedToCreateDatabaseDir,

    #[error("Failed to read database dir")]
    FailedToReadDatabaseDir,

    #[error("Failed to get dir entry")]
    FailedToGetDirEntry,

    #[error("Failed to parse file id")]
    FailedToParseFileId(#[from] std::num::ParseIntError),

    #[error("Read data file eof")]
    ReadDataFileEof,

    #[error("Invalid log record crc")]
    InvalidLogRecordCrc,

    #[error("Batch size exceeded")]
    BatchSizeExceeded,

    #[error("Merge in progress, try again later")]
    MergeInProgress,

    #[error("Failed to remove dir")]
    RemoveDirError,

    #[error("Unable to use write batch")]
    UnableToUseWriteBatch,

    #[error("Failed to create file lock")]
    FailedToCreateFileLock,

    #[error("Database is using")]
    DatabaseIsUsing,

    #[error("Failed to unlock file lock")]
    FailedToUnlockFileLock,
}
