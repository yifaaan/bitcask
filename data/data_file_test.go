package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/fio"
)

func TestOpenDataFile(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_test_data_file_open_*")
	defer os.RemoveAll(dir)

	t.Log(dir)
	df1, err := OpenDataFile(dir, 1, fio.STANDARD_FIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := OpenDataFile(dir, 2, fio.STANDARD_FIO)
	assert.Nil(t, err)
	assert.NotNil(t, df2)
}

func TestDataFile_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_test_data_file_write_*")
	defer os.RemoveAll(dir)

	t.Log(dir)
	df1, err := OpenDataFile(dir, 1, fio.STANDARD_FIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	err = df1.Write([]byte("111"))
	assert.Nil(t, err)

	err = df1.Write([]byte("222"))
	assert.Nil(t, err)

	err = df1.Write([]byte("333"))
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_test_data_file_readlogrecord_*")
	defer os.RemoveAll(dir)

	t.Log(dir)
	df1, err := OpenDataFile(dir, 1, fio.STANDARD_FIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	r1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("val1"),
	}
	buf1, s1 := EncodeLogRecord(r1)
	err = df1.Write(buf1)
	assert.Nil(t, err)

	res1, len1, err := df1.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.NotNil(t, res1)
	assert.Equal(t, r1, res1)
	assert.Equal(t, s1, len1)

	r2 := &LogRecord{
		Key:   []byte("key2"),
		Value: []byte("val2"),
	}
	buf2, s2 := EncodeLogRecord(r2)
	err = df1.Write(buf2)
	assert.Nil(t, err)

	res2, len2, err := df1.ReadLogRecord(s2)
	assert.Nil(t, err)
	assert.NotNil(t, res2)
	assert.Equal(t, r2, res2)
	assert.Equal(t, s2, len2)

	r3 := &LogRecord{
		Key:   []byte("key3"),
		Value: []byte("val3"),
	}
	buf3, s3 := EncodeLogRecord(r3)
	err = df1.Write(buf3)
	assert.Nil(t, err)

	res3, len3, err := df1.ReadLogRecord(s1 + s2)
	assert.Nil(t, err)
	assert.NotNil(t, res3)
	assert.Equal(t, r3, res3)
	assert.Equal(t, s3, len3)
}
