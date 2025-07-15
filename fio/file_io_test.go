package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(name string) {
	if err := os.Remove(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	fio, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	fio, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	fio, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("key-a"), b)

	_, err = fio.Read(b, int64(n))
	assert.Nil(t, err)
	assert.Equal(t, []byte("key-b"), b)
}

func TestFileIo_Sync(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	fio, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIo_Close(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	fio, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
