package data

import "errors"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

var (
	ErrInvalidCRC = errors.New("invalid log record crc")
)

// 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	// file id，存储到了哪个文件
	Fid uint32
	// file offset，存储到了文件的哪个位置
	Offset int64
}

// 写入到数据文件的一条记录，数据文件中的数据是追加写入的
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 日志记录头
type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// 将记录编码成字节数组
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 将字节数组解码成日志记录头,返回日志记录头和它在字节数组中的大小
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}
