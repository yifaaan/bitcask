package bitcask

import (
	"sync"

	"github.com/yifaaan/bitcask/data"
	"github.com/yifaaan/bitcask/index"
)

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	options    Options
	index      index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造 LogRecord
	lr := &data.LogRecord{Key: key, Value: value, Type: data.LOG_RECORD_NORMAL}

	// 写入数据文件
	pos, err := db.appendLogRecord(lr)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecord 将记录写入数据文件，返回对应的位置索引
func (db *DB) appendLogRecord(lr *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断是否初始化活跃数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 编码
	encRecord, len := data.EncodeLogRecord(lr)
	// 写入数据之后如果超过阈值 需要打开新的活跃文件
	if db.activeFile.WriteOff+len > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	return &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}, nil
}

// setActiveDataFile 设置新的活跃数据文件
// 调用时需要加锁
func (db *DB) setActiveDataFile() error {
	var initialId uint32 = 0
	if db.activeFile != nil {
		initialId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引取位置信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取数据
	lr, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if lr.Type == data.LOG_RECORD_DELETED {
		return nil, ErrKeyNotFound
	}
	return lr.Value, nil
}
