package bitcask

import (
	"sync"

	"github.com/yifaaan/bitcask/data"
	"github.com/yifaaan/bitcask/index"
)

// 存储引擎实例
type DB struct {
	options Options
	mu      *sync.RWMutex
	// 当前活跃数据文件，用于写入
	activeFile *data.DataFile
	// 旧数据文件，用于读取
	olderFiles map[uint32]*data.DataFile
	// 内存索引
	index index.Indexer
}

// 写入key/value数据，key不能为空
func (db *DB) Put(key, value []byte) error {
	// key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 向当前活跃数据文件追加写入一条记录
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 写入之后，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdataFailed
	}
	return nil
}

// 向当前活跃数据文件追加写入一条记录
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前的活跃数据文件是否存在
	// 不存在的话需要创建
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入编码后的记录
	// 首先编码
	encRecrod, size := data.EncodeLogRecord(logRecord)
	// 如果写入的记录导致活跃数据文件的大小超出阈值，则关闭活跃数据文件，打开一个新的
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化该活跃数据文件到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将该活跃数据文件转化为旧数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecrod); err != nil {
		return nil, err
	}

	// 根据配置决定是否立刻持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置一个新的活跃数据文件
// 访问此方法前必须持有锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		// id 递增
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
