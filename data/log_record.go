package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LOG_RECORD_NORMAL LogRecordType = iota
	LOG_RECORD_DELETED
	LOG_RECORD_TXN_FINISH
)

// 编码后的 log record 的 header 的最大长度
// crc type keySize valSize
//
//	4    1
const MAX_LOG_RECORD_HEADER_SIZE = 4 + 1 + binary.MaxVarintLen32*2

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	// file id
	Fid uint32
	// file offset
	Offset int64
	// 数据在磁盘上的大小
	Size uint32
}

// LogRecord 写入到数据文件的记录，追加写入
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// EncodeLogRecord 对 LogRecord 编码，将其转换成最终写入到文件中的一条记录，返回编码后的字节数组和长度
//
// +----------+------+----------+------------+---------+--------+
// |   crc    | type | key size | value size |  key    | value  |
// +----------+------+----------+------------+---------+--------+
//
//	4         1    var(max 5)  var(max 5)     var       var
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	header := make([]byte, MAX_LOG_RECORD_HEADER_SIZE)

	header[4] = lr.Type
	var idx = 5
	idx += binary.PutVarint(header[idx:], int64(len(lr.Key)))
	idx += binary.PutVarint(header[idx:], int64(len(lr.Value)))

	// 编码之后一条完整记录的长度
	var size = idx + len(lr.Key) + len(lr.Value)
	buf := make([]byte, size)

	copy(buf[:idx], header[:idx])
	copy(buf[idx:], lr.Key)
	copy(buf[idx+len(lr.Key):], lr.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)

	return buf, int64(size)
}

type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// decodeLogRecordHeader 从 buf(max size) 解码 header 结构，并返回其真正的长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, uint32) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var idx = 5
	keySize, n := binary.Varint(buf[idx:])
	header.keySize = uint32(keySize)
	idx += n
	valueSize, n := binary.Varint(buf[idx:])
	header.valueSize = uint32(valueSize)
	idx += n

	return header, uint32(idx)
}

// getLogRecordCRC 计算编码后的 crc，headerBuf是 type+keySize+valueSize 的字节数组
func getLogRecordCRC(lr *LogRecord, headerBuf []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(headerBuf)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecordPos 对 pos 编码，用于写入 hint 索引文件
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var idx = 0
	idx += binary.PutVarint(buf[idx:], int64(pos.Fid))
	idx += binary.PutVarint(buf[idx:], int64(pos.Offset))
	idx += binary.PutVarint(buf[idx:], int64(pos.Size))
	return buf[:idx]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var idx = 0
	fileId, n := binary.Varint(buf[idx:])
	idx += n
	offset, n := binary.Varint(buf[idx:])
	idx += n
	size, _ := binary.Varint(buf[idx:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}
