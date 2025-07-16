package bitcask

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdataFailed = errors.New("failed to update index")
)
