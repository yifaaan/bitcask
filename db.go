package bitcask

import (
	"errors"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
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
	fids       []int // 只在启动时加载索引时使用
}

func Open(options Options) (*DB, error) {
	if err := checkOptions(&options); err != nil {
		return nil, err
	}

	// 数据目录不存在时需要创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		mu:         &sync.RWMutex{},
		options:    options,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件,初始化 db 的每个数据文件指针
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 构建内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func checkOptions(opts *Options) error {
	if opts.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if opts.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

// loadDataFiles 从磁盘加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntires, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fids []int
	for _, entry := range dirEntires {
		if strings.HasSuffix(entry.Name(), data.DATA_FILE_NAME_SUFFIX) {
			ss := strings.Split(entry.Name(), ".")
			fid, err := strconv.Atoi(ss[0])
			if err != nil {
				return ErrDataFileNameCorrupted
			}
			fids = append(fids, fid)
		}
	}
	slices.Sort(fids)
	db.fids = fids
	for i, id := range fids {
		df, err := data.OpenDataFile(db.options.DirPath, uint32(id))
		if err != nil {
			return err
		}
		if i == len(fids)-1 {
			// 当前的活跃数据文件 id 最大
			db.activeFile = df
		} else {
			db.olderFiles[uint32(id)] = df
		}
	}
	return nil
}

// loadIndexFromDataFiles 读取每个数据文件，构建内存索引
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fids) == 0 {
		return nil
	}

	for i, fid := range db.fids {
		var df *data.DataFile
		if fid == int(db.activeFile.FileId) {
			df = db.activeFile
		} else {
			df = db.olderFiles[uint32(fid)]
		}

		var off int64 = 0
		for {
			lr, len, err := df.ReadLogRecord(off)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			pos := &data.LogRecordPos{
				Fid:    uint32(fid),
				Offset: off,
			}
			var ok bool
			if lr.Type == data.LOG_RECORD_DELETED {
				ok = db.index.Delete(lr.Key)
			} else {
				ok = db.index.Put(lr.Key, pos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}

			off += len
		}

		if i == len(db.fids)-1 {
			// 更新活跃数据文件的写偏移
			db.activeFile.WriteOff = off
		}
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	lr := &data.LogRecord{Key: key, Type: data.LOG_RECORD_DELETED}
	_, err := db.appendLogRecord(lr)
	if err != nil {
		return err
	}

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
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
	lr, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if lr.Type == data.LOG_RECORD_DELETED {
		return nil, ErrKeyNotFound
	}
	return lr.Value, nil
}
