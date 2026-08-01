package fio

import (
	"os"
)

var _ IOManager = (*FileIO)(nil)

// FileIO 标准文件 IO
type FileIO struct {
	fd *os.File
}

func NewFileIO(name string) (*FileIO, error) {
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, DATA_FILE_PERM)
	if err != nil {
		return nil, err
	}
	return &FileIO{
		fd,
	}, nil
}

func (f *FileIO) Read(b []byte, off int64) (int, error) {
	return f.fd.ReadAt(b, off)
}

func (f *FileIO) Write(b []byte) (int, error) {
	return f.fd.Write(b)
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}

func (f *FileIO) Size() (int64, error) {
	st, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
