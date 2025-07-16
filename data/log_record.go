package data

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

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
// 编码规则：
// +----------+----------+----------+----------+----------+----------+
// | crc      | type     | keySize  | valueSize| key      | value    |
// +----------+----------+----------+----------+----------+----------+
// | 4 bytes  | 1 byte   | varint   | varint   | keySize  | valueSize|
// +----------+----------+----------+----------+----------+----------+
// 4 + 1 + varint + varint = 15 bytes（最大）
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = record.Type
	var idx = 5
	idx += binary.PutVarint(header[idx:], int64(len(record.Key)))
	idx += binary.PutVarint(header[idx:], int64(len(record.Value)))

	size := idx + len(record.Key) + len(record.Value)
	encBytes := make([]byte, size)
	// 将header拷贝到encBytes
	copy(encBytes[:idx], header[:idx])
	// 将key拷贝到encBytes
	copy(encBytes[idx:], record.Key)
	// 将value拷贝到encBytes
	copy(encBytes[idx+len(record.Key):], record.Value)

	// 计算crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	// println("header length: ", idx, "crc: ", crc)
	return encBytes, int64(size)
}

// 将字节数组解码成日志记录头,返回日志记录头和它在文件中的大小
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{crc: binary.LittleEndian.Uint32(buf[:4]), recordType: buf[4]}

	var idx = 5
	// 读取keySize
	keySize, n := binary.Varint(buf[idx:])
	idx += n
	header.keySize = uint32(keySize)
	// 读取valueSize
	valueSize, n := binary.Varint(buf[idx:])
	idx += n
	header.valueSize = uint32(valueSize)
	return header, int64(idx)
}
