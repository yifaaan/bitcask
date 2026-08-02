package fio

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMMap_ReadAndSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.data")
	content := []byte("memory-mapped content")
	require.NoError(t, os.WriteFile(path, content, 0o644))

	reader, err := NewMMap(path)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	size, err := reader.Size()
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)

	buf := make([]byte, len("mapped content"))
	n, err := reader.Read(buf, int64(len("memory-")))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, []byte("mapped content"), buf)

	buf = make([]byte, len(content))
	n, err = reader.Read(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.Equal(t, content, buf)
}

func TestMMap_ReadErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.data")
	require.NoError(t, os.WriteFile(path, []byte("content"), 0o644))

	reader, err := NewMMap(path)
	require.NoError(t, err)

	_, err = reader.Read(make([]byte, 1), -1)
	require.Error(t, err)

	_, err = reader.Read(make([]byte, 1), int64(len("content"))+1)
	require.Error(t, err)

	_, err = reader.Read(make([]byte, len("content")+1), 0)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, reader.Close())
}

func TestMMap_Close(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.data")
	require.NoError(t, os.WriteFile(path, []byte("content"), 0o644))

	reader, err := NewMMap(path)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())

	_, err = reader.Read(make([]byte, 1), 0)
	require.Error(t, err)
}

func TestNewMMap_Errors(t *testing.T) {
	_, err := NewMMap(filepath.Join(t.TempDir(), "missing.data"))
	require.Error(t, err)
}
