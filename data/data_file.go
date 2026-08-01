package data

import "github.com/yifaaan/bitcask/fio"

const DATA_FILE_NAME_SUFFIX = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// ReadLogRecord 从数据文件 off 处读取一条 record，返回其在文件中的长度
func (df *DataFile) ReadLogRecord(off int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
