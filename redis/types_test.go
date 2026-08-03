package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yifaaan/bitcask"
)

func openTestRedisDataStruct(t *testing.T) (*RedisDataStruct, bitcask.Options) {
	t.Helper()

	options := bitcask.DefaultOptions
	options.DirPath = t.TempDir()
	options.SyncWrite = true

	rds, err := NewRedisDatastruct(options)
	require.NoError(t, err)
	return rds, options
}

func closeTestRedisDataStruct(t *testing.T, rds *RedisDataStruct) {
	t.Helper()

	if rds == nil || rds.db == nil {
		return
	}
	if err := rds.db.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	rds.db = nil
}

func putRawRedisValue(t *testing.T, rds *RedisDataStruct, key []byte, dataType redisDataType, payload []byte) {
	t.Helper()

	encoded := make([]byte, 2+len(payload))
	encoded[0] = dataType
	encoded[1] = 0
	copy(encoded[2:], payload)
	require.NoError(t, rds.db.Put(key, encoded))
}

func TestRedisMetadataEncodeDecode(t *testing.T) {
	want := &metadata{
		dataType: HASH,
		expire:   0,
		version:  123456789,
		size:     2,
	}

	got := decodeMetadata(want.encode())
	require.NotNil(t, got)
	assert.Equal(t, want.dataType, got.dataType)
	assert.Equal(t, want.expire, got.expire)
	assert.Equal(t, want.version, got.version)
	assert.Equal(t, want.size, got.size)
}

func TestRedisListMetadataEncodeDecode(t *testing.T) {
	want := &metadata{
		dataType: LIST,
		expire:   0,
		version:  123456789,
		size:     2,
		head:     initialListMark - 3,
		tail:     initialListMark + 2,
	}

	got := decodeMetadata(want.encode())
	require.NotNil(t, got)
	assert.Equal(t, want.dataType, got.dataType)
	assert.Equal(t, want.expire, got.expire)
	assert.Equal(t, want.version, got.version)
	assert.Equal(t, want.size, got.size)
	assert.Equal(t, want.head, got.head)
	assert.Equal(t, want.tail, got.tail)
}

func TestRedisDataStructSetAndGet(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	want := []byte("bitcask")
	require.NoError(t, rds.Set([]byte("name"), 0, want))

	got, err := rds.Get([]byte("name"))
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestRedisDataStructGetHonorsTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		want    []byte
		wantNil bool
	}{
		{name: "no expiration", ttl: 0, want: []byte("persistent")},
		{name: "future expiration", ttl: time.Hour, want: []byte("available")},
		{name: "expired", ttl: -time.Second, want: []byte("expired"), wantNil: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rds, _ := openTestRedisDataStruct(t)
			defer closeTestRedisDataStruct(t, rds)

			key := []byte("key:" + tt.name)
			require.NoError(t, rds.Set(key, tt.ttl, tt.want))

			got, err := rds.Get(key)
			require.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRedisDataStructSetNilValueDoesNotCreateKey(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("nil-value")
	require.NoError(t, rds.Set(key, 0, nil))

	_, err := rds.Get(key)
	assert.ErrorIs(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructGetRejectsWrongType(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	putRawRedisValue(t, rds, []byte("hash"), HASH, []byte("field-value"))

	got, err := rds.Get([]byte("hash"))
	assert.Nil(t, got)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)
}

func TestRedisDataStructHashSetAndGet(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("user:1")
	field := []byte("name")

	isNew, err := rds.HSet(key, field, []byte("Alice"))
	require.NoError(t, err)
	assert.True(t, isNew)
	rawMetadata, err := rds.db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), decodeMetadata(rawMetadata).size)

	got, err := rds.HGet(key, field)
	require.NoError(t, err)
	assert.Equal(t, []byte("Alice"), got)

	dataType, err := rds.Type(key)
	require.NoError(t, err)
	assert.Equal(t, HASH, dataType)
}

func TestRedisDataStructHashSetUpdatesExistingField(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("user:1")
	field := []byte("name")

	isNew, err := rds.HSet(key, field, []byte("Alice"))
	require.NoError(t, err)
	assert.True(t, isNew)

	isNew, err = rds.HSet(key, field, []byte("Bob"))
	require.NoError(t, err)
	assert.False(t, isNew)

	got, err := rds.HGet(key, field)
	require.NoError(t, err)
	assert.Equal(t, []byte("Bob"), got)
}

func TestRedisDataStructHashFieldsAreIsolated(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	firstKey := []byte("user:1")
	secondKey := []byte("user:2")
	field := []byte("name")

	firstNew, err := rds.HSet(firstKey, field, []byte("Alice"))
	require.NoError(t, err)
	assert.True(t, firstNew)

	secondNew, err := rds.HSet(secondKey, field, []byte("Bob"))
	require.NoError(t, err)
	assert.True(t, secondNew)

	firstValue, err := rds.HGet(firstKey, field)
	require.NoError(t, err)
	secondValue, err := rds.HGet(secondKey, field)
	require.NoError(t, err)
	assert.Equal(t, []byte("Alice"), firstValue)
	assert.Equal(t, []byte("Bob"), secondValue)
}

func TestRedisDataStructHashDel(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("user:1")
	nameField := []byte("name")
	ageField := []byte("age")
	_, err := rds.HSet(key, nameField, []byte("Alice"))
	require.NoError(t, err)
	_, err = rds.HSet(key, ageField, []byte("18"))
	require.NoError(t, err)

	deleted, err := rds.HDel(key, nameField)
	require.NoError(t, err)
	assert.True(t, deleted)

	_, err = rds.HGet(key, nameField)
	assert.ErrorIs(t, err, bitcask.ErrKeyNotFound)
	remainingValue, err := rds.HGet(key, ageField)
	require.NoError(t, err)
	assert.Equal(t, []byte("18"), remainingValue)

	deleted, err = rds.HDel(key, nameField)
	require.NoError(t, err)
	assert.False(t, deleted)

	deleted, err = rds.HDel(key, ageField)
	require.NoError(t, err)
	assert.True(t, deleted)
}

func TestRedisDataStructHashRejectsWrongType(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("string-key")
	require.NoError(t, rds.Set(key, 0, []byte("value")))

	_, err := rds.HSet(key, []byte("field"), []byte("value"))
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	_, err = rds.HGet(key, []byte("field"))
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	_, err = rds.HDel(key, []byte("field"))
	assert.ErrorIs(t, err, ErrWrongTypeOperation)
}

func TestRedisDataStructHashReopenPreservesFields(t *testing.T) {
	rds, options := openTestRedisDataStruct(t)
	key := []byte("user:1")
	_, err := rds.HSet(key, []byte("name"), []byte("Alice"))
	require.NoError(t, err)
	_, err = rds.HSet(key, []byte("role"), []byte("admin"))
	require.NoError(t, err)
	closeTestRedisDataStruct(t, rds)

	reopened, err := NewRedisDatastruct(options)
	require.NoError(t, err)
	defer closeTestRedisDataStruct(t, reopened)

	name, err := reopened.HGet(key, []byte("name"))
	require.NoError(t, err)
	role, err := reopened.HGet(key, []byte("role"))
	require.NoError(t, err)
	assert.Equal(t, []byte("Alice"), name)
	assert.Equal(t, []byte("admin"), role)
}

func TestRedisDataStructSAddAndIsMember(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("tags")
	member := []byte("go")

	isNew, err := rds.SAdd(key, member)
	require.NoError(t, err)
	assert.True(t, isNew)

	isMember, err := rds.SIsMember(key, member)
	require.NoError(t, err)
	assert.True(t, isMember)

	rawMetadata, err := rds.db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), decodeMetadata(rawMetadata).size)

	dataType, err := rds.Type(key)
	require.NoError(t, err)
	assert.Equal(t, SET, dataType)
}

func TestRedisDataStructSAddIsIdempotent(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("tags")
	member := []byte("go")

	isNew, err := rds.SAdd(key, member)
	require.NoError(t, err)
	assert.True(t, isNew)

	isNew, err = rds.SAdd(key, member)
	require.NoError(t, err)
	assert.False(t, isNew)

	isMember, err := rds.SIsMember(key, member)
	require.NoError(t, err)
	assert.True(t, isMember)

	rawMetadata, err := rds.db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), decodeMetadata(rawMetadata).size)
}

func TestRedisDataStructSAddMembersAreIsolated(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	firstKey := []byte("user:1:tags")
	secondKey := []byte("user:2:tags")
	commonMember := []byte("go")
	firstOnlyMember := []byte("storage")

	firstNew, err := rds.SAdd(firstKey, commonMember)
	require.NoError(t, err)
	assert.True(t, firstNew)
	firstNew, err = rds.SAdd(firstKey, firstOnlyMember)
	require.NoError(t, err)
	assert.True(t, firstNew)

	secondNew, err := rds.SAdd(secondKey, commonMember)
	require.NoError(t, err)
	assert.True(t, secondNew)

	firstCommon, err := rds.SIsMember(firstKey, commonMember)
	require.NoError(t, err)
	assert.True(t, firstCommon)
	firstOnly, err := rds.SIsMember(firstKey, firstOnlyMember)
	require.NoError(t, err)
	assert.True(t, firstOnly)
	secondOnly, err := rds.SIsMember(secondKey, firstOnlyMember)
	require.NoError(t, err)
	assert.False(t, secondOnly)

	secondCommon, err := rds.SIsMember(secondKey, commonMember)
	require.NoError(t, err)
	assert.True(t, secondCommon)
}

func TestRedisDataStructSRem(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("tags")
	members := [][]byte{[]byte("go"), []byte("storage"), []byte("database")}
	for _, member := range members {
		isNew, err := rds.SAdd(key, member)
		require.NoError(t, err)
		assert.True(t, isNew)
	}

	deleted, err := rds.SRem(key, members[1])
	require.NoError(t, err)
	assert.True(t, deleted)

	isMember, err := rds.SIsMember(key, members[1])
	require.NoError(t, err)
	assert.False(t, isMember)
	isMember, err = rds.SIsMember(key, members[0])
	require.NoError(t, err)
	assert.True(t, isMember)

	deleted, err = rds.SRem(key, members[1])
	require.NoError(t, err)
	assert.False(t, deleted)
	deleted, err = rds.SRem(key, []byte("missing"))
	require.NoError(t, err)
	assert.False(t, deleted)

	rawMetadata, err := rds.db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), decodeMetadata(rawMetadata).size)

	deleted, err = rds.SRem(key, members[0])
	require.NoError(t, err)
	assert.True(t, deleted)
	deleted, err = rds.SRem(key, members[2])
	require.NoError(t, err)
	assert.True(t, deleted)

	rawMetadata, err = rds.db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), decodeMetadata(rawMetadata).size)
}

func TestRedisDataStructSetRejectsWrongType(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("string-key")
	require.NoError(t, rds.Set(key, 0, []byte("value")))

	isNew, err := rds.SAdd(key, []byte("member"))
	assert.False(t, isNew)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	isMember, err := rds.SIsMember(key, []byte("member"))
	assert.False(t, isMember)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	deleted, err := rds.SRem(key, []byte("member"))
	assert.False(t, deleted)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)
}

func TestRedisDataStructSetReopenPreservesMembers(t *testing.T) {
	rds, options := openTestRedisDataStruct(t)
	key := []byte("tags")
	members := [][]byte{[]byte("go"), []byte("storage")}
	for _, member := range members {
		isNew, err := rds.SAdd(key, member)
		require.NoError(t, err)
		assert.True(t, isNew)
	}
	closeTestRedisDataStruct(t, rds)

	reopened, err := NewRedisDatastruct(options)
	require.NoError(t, err)
	defer closeTestRedisDataStruct(t, reopened)

	dataType, err := reopened.Type(key)
	require.NoError(t, err)
	assert.Equal(t, SET, dataType)

	for _, member := range members {
		isMember, err := reopened.SIsMember(key, member)
		require.NoError(t, err)
		assert.True(t, isMember)
	}
}

func TestRedisDataStructListPushAndPop(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("queue")

	length, err := rds.RPush(key, []byte("middle"))
	require.NoError(t, err)
	assert.Equal(t, uint32(1), length)

	length, err = rds.LPush(key, []byte("first"))
	require.NoError(t, err)
	assert.Equal(t, uint32(2), length)

	length, err = rds.RPush(key, []byte("last"))
	require.NoError(t, err)
	assert.Equal(t, uint32(3), length)

	dataType, err := rds.Type(key)
	require.NoError(t, err)
	assert.Equal(t, LIST, dataType)

	got, err := rds.LPop(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), got)

	got, err = rds.RPop(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("last"), got)

	got, err = rds.LPop(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("middle"), got)

	got, err = rds.LPop(key)
	require.NoError(t, err)
	assert.Nil(t, got)
	got, err = rds.RPop(key)
	require.NoError(t, err)
	assert.Nil(t, got)

	rawMetadata, err := rds.db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), decodeMetadata(rawMetadata).size)
}

func TestRedisDataStructListMissingKeyIsEmpty(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("missing-list")

	got, err := rds.LPop(key)
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = rds.RPop(key)
	require.NoError(t, err)
	assert.Nil(t, got)

	_, err = rds.Type(key)
	assert.ErrorIs(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructListRejectsWrongType(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("string-key")
	require.NoError(t, rds.Set(key, 0, []byte("value")))

	length, err := rds.LPush(key, []byte("element"))
	assert.Zero(t, length)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	length, err = rds.RPush(key, []byte("element"))
	assert.Zero(t, length)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	got, err := rds.LPop(key)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)

	got, err = rds.RPop(key)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, ErrWrongTypeOperation)
}

func TestRedisDataStructListReopenPreservesElements(t *testing.T) {
	rds, options := openTestRedisDataStruct(t)
	key := []byte("queue")

	_, err := rds.RPush(key, []byte("middle"))
	require.NoError(t, err)
	_, err = rds.LPush(key, []byte("first"))
	require.NoError(t, err)
	_, err = rds.RPush(key, []byte("last"))
	require.NoError(t, err)
	closeTestRedisDataStruct(t, rds)

	reopened, err := NewRedisDatastruct(options)
	require.NoError(t, err)
	defer closeTestRedisDataStruct(t, reopened)

	dataType, err := reopened.Type(key)
	require.NoError(t, err)
	assert.Equal(t, LIST, dataType)

	got, err := reopened.LPop(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), got)
	got, err = reopened.RPop(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("last"), got)
	got, err = reopened.LPop(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("middle"), got)
}

func TestRedisDataStructDel(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	key := []byte("name")
	require.NoError(t, rds.Set(key, 0, []byte("bitcask")))
	require.NoError(t, rds.Del(key))

	_, err := rds.Get(key)
	assert.ErrorIs(t, err, bitcask.ErrKeyNotFound)
	assert.NoError(t, rds.Del(key))
}

func TestRedisDataStructType(t *testing.T) {
	rds, _ := openTestRedisDataStruct(t)
	defer closeTestRedisDataStruct(t, rds)

	require.NoError(t, rds.Set([]byte("string"), 0, []byte("value")))
	got, err := rds.Type([]byte("string"))
	require.NoError(t, err)
	assert.Equal(t, STRING, got)

	putRawRedisValue(t, rds, []byte("hash"), HASH, []byte("field-value"))
	got, err = rds.Type([]byte("hash"))
	require.NoError(t, err)
	assert.Equal(t, HASH, got)

	_, err = rds.Type([]byte("missing"))
	assert.ErrorIs(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructReopenPreservesValue(t *testing.T) {
	rds, options := openTestRedisDataStruct(t)
	require.NoError(t, rds.Set([]byte("name"), time.Hour, []byte("bitcask")))
	closeTestRedisDataStruct(t, rds)

	reopened, err := NewRedisDatastruct(options)
	require.NoError(t, err)
	defer closeTestRedisDataStruct(t, reopened)

	got, err := reopened.Get([]byte("name"))
	require.NoError(t, err)
	assert.Equal(t, []byte("bitcask"), got)
}
