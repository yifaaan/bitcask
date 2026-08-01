package bitcask

import "errors"

var (
	ErrKeyIsEmpty            = errors.New("key is empty")
	ErrIndexUpdateFailed     = errors.New("failed to update index")
	ErrKeyNotFound           = errors.New("key is not found")
	ErrDataFileNotFound      = errors.New("data file not found")
	ErrDataFileNameCorrupted = errors.New("data file name corrupted")
	ErrExceedMaxBatchNum     = errors.New("exceed the max batch num")
)
