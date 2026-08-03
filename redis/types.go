package redis

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/yifaaan/bitcask"
)

// RedisDataStruct redis 服务
type RedisDataStruct struct {
	db *bitcask.DB
}

func NewRedisDatastruct(options bitcask.Options) (*RedisDataStruct, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStruct{db: db}, nil
}

type redisDataType = byte

const (
	STRING redisDataType = iota
	HASH
	SET
	LIST
	ZSET
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE operation against a key holding the wrong kind of value")
)

// ------------------- String ---------------------

func (rds *RedisDataStruct) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value: type | expire | payload(value)
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = STRING
	var idx = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	idx += binary.PutVarint(buf[idx:], expire)
	res := make([]byte, idx+len(value))
	copy(res[:idx], buf[:idx])
	copy(res[idx:], value)

	return rds.db.Put(key, res)
}

func (rds *RedisDataStruct) Get(key []byte) ([]byte, error) {
	res, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := res[0]
	if dataType != STRING {
		return nil, ErrWrongTypeOperation
	}

	var idx = 1
	expire, n := binary.Varint(res[idx:])
	idx += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return res[idx:], nil
}
