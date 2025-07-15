package fio

import "os"

// 文件IO实现
type FileIO struct {
	fd *os.File
}

func NewFileIOManager(name string) (*FileIO, error) {
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// 从文件的指定位置读取数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// 写入字节数组
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// 持久化到磁盘
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// 关闭
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
