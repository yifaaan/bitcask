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
