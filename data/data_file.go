package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/yifaaan/bitcask/fio"
)

const (
	DATA_FILE_NAME_SUFFIX    = ".data"
	HINT_FILE_NAME           = "hint-index"
	MERGE_FINISHED_FILE_NAME = "merge-finished"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value")
)

// DataFile 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fid uint32, ioType fio.FileIOType) (*DataFile, error) {
	name := filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+DATA_FILE_NAME_SUFFIX)
	return newDataFile(name, fid, ioType)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	name := filepath.Join(dirPath, HINT_FILE_NAME)
	return newDataFile(name, 0, fio.STANDARD_FIO)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	name := filepath.Join(dirPath, MERGE_FINISHED_FILE_NAME)
	return newDataFile(name, 0, fio.STANDARD_FIO)
}

func newDataFile(name string, fid uint32, ioType fio.FileIOType) (*DataFile, error) {
	iom, err := fio.NewIOManager(name, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fid,
		WriteOff:  0,
		IOManager: iom,
	}, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// WriteHintRecord 写入一条 hint 索引记录
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	lr := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	buf, _ := EncodeLogRecord(lr)
	return df.Write(buf)
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// ReadLogRecord 从数据文件 off 处读取一条 record，返回其在文件中的长度
func (df *DataFile) ReadLogRecord(off int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 从 off 处不足以读取 MAX_LOG_RECORD_HEADER_SIZE，则需要读到文件末尾
	var headSizeToRead int64 = MAX_LOG_RECORD_HEADER_SIZE
	if off+MAX_LOG_RECORD_HEADER_SIZE > fileSize {
		headSizeToRead = fileSize - off
	}

	// header
	headerBuf, err := df.readNBytes(headSizeToRead, off)
	if err != nil {
		return nil, 0, err
	}

	header, headSize := decodeLogRecordHeader(headerBuf)

	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := header.keySize, header.valueSize
	// 编码后的 record 所占文件大小
	recordSize := headSize + keySize + valueSize

	lr := &LogRecord{Type: header.recordType}
	// key value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(int64(keySize)+int64(valueSize), off+int64(headSize))
		if err != nil {
			return nil, 0, err
		}
		lr.Key = kvBuf[:keySize]
		lr.Value = kvBuf[keySize:]
	}

	// 检验 crc
	if header.crc != getLogRecordCRC(lr, headerBuf[crc32.Size:headSize]) {
		return nil, 0, ErrInvalidCRC
	}
	return lr, int64(recordSize), nil
}

// readNBytes 从 off 偏移处读取 n 个字节
func (df *DataFile) readNBytes(n int64, off int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IOManager.Read(buf, off)
	return buf, err
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}
	name := filepath.Join(dirPath, fmt.Sprintf("%09d", df.FileId)+DATA_FILE_NAME_SUFFIX)
	iom, err := fio.NewIOManager(name, ioType)
	if err != nil {
		return err
	}
	df.IOManager = iom
	return nil
}
