package redis

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/yifaaan/bitcask"
	"github.com/yifaaan/bitcask/utils"
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
		if dataType == LIST {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
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

// ------------------- Set ---------------------

func (rds *RedisDataStruct) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, SET)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err := rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *RedisDataStruct) SIsMember(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, SET)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStruct) SRem(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, SET)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+8+len(sk.member)+4)

	var idx = 0
	copy(buf[idx:idx+len(sk.key)], sk.key)
	idx += len(sk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(sk.version))
	idx += 8

	copy(buf[idx:idx+len(sk.member)], sk.member)
	idx += len(sk.member)

	binary.LittleEndian.PutUint32(buf[idx:], uint32(len(sk.member)))

	return buf
}

// ------------------- List ---------------------

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	var idx = 0
	copy(buf[idx:idx+len(lk.key)], lk.key)
	idx += len(lk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(lk.version))
	idx += 8

	binary.LittleEndian.PutUint64(buf[idx:idx+8], lk.index)
	return buf
}

func (rds *RedisDataStruct) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStruct) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStruct) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, LIST)
	if err != nil {
		return 0, err
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStruct) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStruct) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStruct) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, LIST)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	if err := rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

// ------------------- ZSet ---------------------

func (rds *RedisDataStruct) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, ZSET)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 是否已存在
	val, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist {
		if score == utils.Float64FromBytes(val) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.Float64FromBytes(val),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}

	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStruct) ZScore(key []byte, score float64, member []byte) (float64, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, ZSET)
	if err != nil {
		return -1, err
	}

	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	val, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}
	return utils.Float64FromBytes(val), nil
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+8+len(zk.member))

	var idx = 0
	copy(buf[idx:idx+len(zk.key)], zk.key)
	idx += len(zk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(zk.version))
	idx += 8

	copy(buf[idx:idx+len(zk.member)], zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+8+len(zk.member)+len(scoreBuf)+4)

	var idx = 0
	copy(buf[idx:idx+len(zk.key)], zk.key)
	idx += len(zk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(zk.version))
	idx += 8

	copy(buf[idx:idx+len(scoreBuf)], scoreBuf)
	idx += len(scoreBuf)

	copy(buf[idx:idx+len(zk.member)], zk.member)
	idx += len(zk.member)

	binary.LittleEndian.PutUint32(buf[idx:idx+4], uint32(len(zk.member)))
	return buf
}
