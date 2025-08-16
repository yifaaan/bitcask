package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMMapIOManager(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	// Ensure file exists before mmap opening
	fioW, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fioW)
	_ = fioW.Close()

	fio, err := NewMMapIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestMMap_Read(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	// Prepare file content using FileIO (mmap is read-only)
	fioW, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fioW)

	_, err = fioW.Write([]byte("key-a"))
	assert.Nil(t, err)
	_, err = fioW.Write([]byte("key-b"))
	assert.Nil(t, err)
	_ = fioW.Close()

	fio, err := NewMMapIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("key-a"), b)

	_, err = fio.Read(b, int64(n))
	assert.Nil(t, err)
	assert.Equal(t, []byte("key-b"), b)
}

func TestMMap_Close(t *testing.T) {
	name := filepath.Join("/tmp", "a.data")
	defer destoryFile(name)

	// Ensure file exists before mmap opening
	fioW, err := NewFileIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fioW)
	_ = fioW.Close()

	fio, err := NewMMapIOManager(name)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
