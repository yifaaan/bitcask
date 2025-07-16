package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeRecord(t *testing.T) {
	// 测试正常情况
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("value1"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	// t.Log(res1, n1)

	// value为空
	rec2 := &LogRecord{
		Key:   []byte("key2"),
		Value: []byte{},
		Type:  LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	// t.Log(res2, n2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// delete类型
	rec3 := &LogRecord{
		Key:   []byte("key3"),
		Value: []byte("value3"),
		Type:  LogRecordDelete,
	}
	res3, n3 := EncodeLogRecord(rec3)
	t.Log(res3, n3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecord(t *testing.T) {
	header, n := decodeLogRecordHeader([]byte{115, 112, 45, 146, 0, 8, 12})
	// t.Log(header, n)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), n)
	assert.Equal(t, uint32(2452451443), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(6), header.valueSize)

	header, n = decodeLogRecordHeader([]byte{229, 183, 46, 229, 0, 8, 0})
	// t.Log(header, n)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), n)
	assert.Equal(t, uint32(3845044197), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)

	header, n = decodeLogRecordHeader([]byte{243, 217, 42, 54, 1, 8, 12})
	// t.Log(header, n)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), n)
	assert.Equal(t, uint32(908777971), header.crc)
	assert.Equal(t, LogRecordDelete, header.recordType)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(6), header.valueSize)

}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("value1"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{115, 112, 45, 146, 0, 8, 12}
	crc := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2452451443), crc)

	rec2 := &LogRecord{
		Key:   []byte("key2"),
		Value: []byte{},
		Type:  LogRecordNormal,
	}
	headerBuf2 := []byte{229, 183, 46, 229, 0, 8, 0}
	crc = getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(3845044197), crc)

	rec3 := &LogRecord{
		Key:   []byte("key3"),
		Value: []byte("value3"),
		Type:  LogRecordDelete,
	}
	headerBuf3 := []byte{243, 217, 42, 54, 1, 8, 12}
	crc = getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(908777971), crc)
}
