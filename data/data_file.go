package data

import "github.com/yifaaan/bitcask/fio"

const DataFileNameSuffix = ".data"

// 数据文件结构体
type DataFile struct {
	FileId uint32
	// 写偏移
	WriteOff int64
	// io 读写管理
	IOManager fio.IOManager
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// 根据偏移读取一条记录,返回记录和它在文件中的大小
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(b []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
