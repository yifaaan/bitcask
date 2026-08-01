package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// normal
	r1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("1"),
		Type:  LOG_RECORD_NORMAL,
	}

	buf1, n1 := EncodeLogRecord(r1)
	assert.NotNil(t, buf1)
	assert.Greater(t, n1, int64(5))
	t.Log(buf1, n1)

	// value is empty
	r2 := &LogRecord{
		Key:  []byte("name"),
		Type: LOG_RECORD_NORMAL,
	}
	buf2, n2 := EncodeLogRecord(r2)
	assert.NotNil(t, buf2)
	assert.Greater(t, n2, int64(5))
	t.Log(buf2, n2)

	// deleted
	r3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("111"),
		Type:  LOG_RECORD_DELETED,
	}

	buf3, n3 := EncodeLogRecord(r3)
	assert.NotNil(t, buf3)
	assert.Greater(t, n3, int64(5))
	t.Log(buf3, n3)
}

func TestDecodeLogRecord(t *testing.T) {
	buf1 := []byte{228, 174, 198, 183, 0, 8, 2, 110, 97, 109, 101, 49}
	header, size := decodeLogRecordHeader(buf1)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(1), header.valueSize)
	assert.Equal(t, LOG_RECORD_NORMAL, header.recordType)
	assert.NotNil(t, header)
	assert.Equal(t, uint32(7), size)

	buf2 := []byte{9, 252, 88, 14, 0, 8, 0, 110, 97, 109, 101}
	header, size = decodeLogRecordHeader(buf2)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(0), header.valueSize)
	assert.Equal(t, LOG_RECORD_NORMAL, header.recordType)
	assert.NotNil(t, header)
	assert.Equal(t, uint32(7), size)

	buf3 := []byte{114, 127, 174, 182, 1, 8, 6, 110, 97, 109, 101, 49, 49, 49}
	header, size = decodeLogRecordHeader(buf3)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(3), header.valueSize)
	assert.Equal(t, LOG_RECORD_DELETED, header.recordType)
	assert.NotNil(t, header)
	assert.Equal(t, uint32(7), size)
}
