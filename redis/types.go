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

// ------------------- Hash ---------------------

// HSet field不存在时才会返回 true
func (rds *RedisDataStruct) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, HASH)
	if err != nil {
		return false, err
	}

	// 构造 HASH 的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	k := hk.encode()

	// 查找是否存在
	var exist = true
	if _, err := rds.db.Get(k); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	// write batch 将元数据的更新和数据的更新组成一个事务
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在时需要更新元数据的size字段
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(k, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStruct) HGet(key, field []byte) ([]byte, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, HASH)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// 构造 HASH 的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStruct) HDel(key, field []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, HASH)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造 HASH 的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	k := hk.encode()

	// 查看是否存在
	var exist = true
	if _, err := rds.db.Get(k); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(k)
		if err := wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// findMetadata 查找元数据，不存在或过期时创建
func (rds *RedisDataStruct) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist bool
	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		exist = true
		meta = decodeMetadata(metaBuf)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		// 判断过期
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
	}
	if dataType == LIST {
		meta.head = initialListMark
		meta.tail = initialListMark
	}
	return meta, nil
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.field))

	var idx = 0
	copy(buf[idx:idx+len(hk.key)], hk.key)
	idx += len(hk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(hk.version))
	idx += 8

	copy(buf[idx:idx+len(hk.field)], hk.field)

	return buf
}
