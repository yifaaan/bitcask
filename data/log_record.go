package data

import "encoding/binary"

type LogRecordType = byte

const (
	LOG_RECORD_NORMAL LogRecordType = iota
	LOG_RECORD_DELETED
)

// 编码后的 log record 的 header 的最大长度
// crc type keySize valSize
//	4    1
const MAX_LOG_RECORD_HEADER_SIZE = 4 + 1 + binary.MaxVarintLen32*2

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	// file id
	Fid uint32
	// file offset
	Offset int64
}

// LogRecord 写入到数据文件的记录，追加写入
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// EncodeLogRecord 对 LogRecord 编码，返回编码后的字节数组和长度
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}

type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// decodeLogRecordHeader 从 buf(max size) 解码 header 结构，并返回其真正的长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, uint32) {
	return nil, 0
}

// getLogRecordCRC 计算编码后的 crc，headerBuf是 type+keySize+valueSize 的字节数组
func getLogRecordCRC(lr *LogRecord, headerBuf []byte) uint32 {
	return 0
}
