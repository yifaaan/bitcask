#![allow(dead_code)]

use thiserror::Error;
pub type Result<T> = std::result::Result<T, Errors>;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum Errors {
    #[error("Failed to read from data file")]
    ReadFromDataFileError,

    #[error("Failed to write to data file")]
    WriteToDataFileError,

    #[error("Failed to sync file")]
    SyncFileError,

    #[error("Failed to open file")]
    OpenFileError,
}
