package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/yifaaan/bitcask/fio"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
	// crc:4, type:1, keySize: 5, valueSize: 5
	maxLogRecordHeaderSize = 4 + 1 + 2*binary.MaxVarintLen32
)

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
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, DataFileNameSuffix))
	return newDataFile(fileName, fileId)
}

// 打开hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// 打开标识merge完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

// 打开标事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	iomanager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{FileId: fileId, WriteOff: 0, IOManager: iomanager}, nil
}

// 根据偏移读取一条记录,返回记录和它在文件中的大小
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	// 如果读取的最大header大小超过了文件长度，则只需读到文件末尾
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// 读header, headerBuf是最大可能的header大小
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize
	// 读取key，value数据
	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 检验数据有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{Key: key, Value: EncodeLogRecordPos(pos)}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *DataFile) Write(b []byte) error {
	n, err := df.IOManager.Write(b)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// 读取n个字节到b中，从offset偏移开始
func (df *DataFile) readNBytes(n, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}
