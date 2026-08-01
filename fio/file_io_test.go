package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIO(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_fio_test_new_*")
	defer os.RemoveAll(dir)
	fio, err := NewFileIO(filepath.Join(dir, "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_fio_test_write_*")
	defer os.RemoveAll(dir)
	fio, err := NewFileIO(filepath.Join(dir, "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("123456"))
	assert.Nil(t, err)
	assert.Equal(t, 6, n)

	n, err = fio.Write([]byte("444"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
}

func TestFileIO_Read(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask_fio_test_read_*")
	defer os.RemoveAll(dir)
	fio, err := NewFileIO(filepath.Join(dir, "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("123456"))
	assert.Nil(t, err)
	assert.Equal(t, 6, n)

	n, err = fio.Write([]byte("444"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	buf := make([]byte, 9)
	_, err = fio.Read(buf[0:6], 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("123456"), buf[0:6])

	_, err = fio.Read(buf[6:], 6)
	assert.Nil(t, err)
	assert.Equal(t, []byte("444"), buf[6:])
}
