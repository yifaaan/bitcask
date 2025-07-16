package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
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

// 将记录编码成字节数组
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
