package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_test_data_file_open_*")
	defer os.RemoveAll(dir)

	t.Log(dir)
	df1, err := OpenDataFile(dir, 1)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := OpenDataFile(dir, 2)
	assert.Nil(t, err)
	assert.NotNil(t, df2)
}

func TestDataFile_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_test_data_file_write_*")
	defer os.RemoveAll(dir)

	t.Log(dir)
	df1, err := OpenDataFile(dir, 1)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	err = df1.Write([]byte("111"))
	assert.Nil(t, err)

	err = df1.Write([]byte("222"))
	assert.Nil(t, err)

	err = df1.Write([]byte("333"))
	assert.Nil(t, err)
}
