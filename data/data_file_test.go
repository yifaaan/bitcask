package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/fio"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("world"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 333, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("value1"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)
	// t.Log(rec1)

	readRec1, readSize, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, size1, readSize)
	assert.Equal(t, rec1.Key, readRec1.Key)
	assert.Equal(t, rec1.Value, readRec1.Value)
	assert.Equal(t, rec1.Type, readRec1.Type)

	// 有两条 LogRecord
	rec2 := &LogRecord{
		Key:   []byte("key2"),
		Value: []byte("value2"),
		Type:  LogRecordNormal,
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)
	// t.Log(rec2)

	readRec2, readSize, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, size2, readSize)
	assert.Equal(t, rec2.Key, readRec2.Key)
	assert.Equal(t, rec2.Value, readRec2.Value)
	assert.Equal(t, rec2.Type, readRec2.Type)

	rec3 := &LogRecord{
		Key:   []byte("key3"),
		Value: []byte{},
		Type:  LogRecordNormal,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	// t.Log(rec3)

	readRec3, readSize, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, size3, readSize)
	assert.Equal(t, rec3.Key, readRec3.Key)
	assert.Equal(t, rec3.Value, readRec3.Value)
	assert.Equal(t, rec3.Type, readRec3.Type)

}
