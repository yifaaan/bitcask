package bitcask

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gofrs/flock"
	"github.com/yifaaan/bitcask/data"
	"github.com/yifaaan/bitcask/fio"
	"github.com/yifaaan/bitcask/index"
	"github.com/yifaaan/bitcask/utils"
)

const (
	FILE_LOCK_NAME = "flock"
)

type DB struct {
	mu          *sync.RWMutex
	activeFile  *data.DataFile
	olderFiles  map[uint32]*data.DataFile
	options     Options
	index       index.Indexer
	fids        []int         // 只在启动时加载索引时使用
	seqNo       atomic.Uint64 // 事务序列号，全局递增
	isMerging   bool
	fileLock    *flock.Flock
	bytesWrite  uint  // 累计写入了多少字节
	reclaimSize int64 // 标识有多少无效数据
}

type Stat struct {
	KeyNum          uint
	DataFileNum     uint
	ReclaimableSize int64 // 可回收数据量
	DisSize         int64 // 数据目录所占磁盘空间
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

	// 判断当前目录是否正在被另一个db使用
	fileLock := flock.New(filepath.Join(options.DirPath, FILE_LOCK_NAME))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}
	db := &DB{
		mu:         &sync.RWMutex{},
		options:    options,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
		fileLock:   fileLock,
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件,初始化 db 的每个数据文件指针
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 构建内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	// 重置为标准文件 io 类型
	if db.options.MMapAtStart {
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.STANDARD_FIO); err != nil {
		return err
	}

	for _, df := range db.olderFiles {
		if err := df.SetIOManager(db.options.DirPath, fio.STANDARD_FIO); err != nil {
			return err
		}
	}
	return nil
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
		ioType := fio.STANDARD_FIO
		if db.options.MMapAtStart {
			ioType = fio.MMAP
		}
		df, err := data.OpenDataFile(db.options.DirPath, uint32(id), ioType)
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

	// merge 过的已经从 hint 文件加载索引了
	hasMerge, nonMergeFid := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MERGE_FINISHED_FILE_NAME)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		nonMergeFid, err = db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LOG_RECORD_DELETED {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存同一个事务的记录
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64
	for i, fid := range db.fids {
		if hasMerge && fid < int(nonMergeFid) {
			continue
		}

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
				Size:   uint32(len),
			}

			// 解析seqNo和实际的key
			realKey, seqNo := parseLogRecordKeyWithSeq(lr.Key)

			if seqNo == NON_TRANSACTION_SEQ_NO {
				// 改记录属于非事务提交，直接更新索引
				updateIndex(realKey, lr.Type, pos)
			} else {
				// 事务提交，先暂存
				// 如果找到标识某事务结束的记录，将该事务的所有记录更新到内存索引
				if lr.Type == data.LOG_RECORD_TXN_FINISH {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					lr.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: lr,
						Pos:    pos,
					})
				}
			}

			// 更新最新的事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			off += len
		}

		if i == len(db.fids)-1 {
			// 更新活跃数据文件的写偏移
			db.activeFile.WriteOff = off
		}
	}

	db.seqNo.Store(currentSeqNo)

	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	lr := &data.LogRecord{Key: logRecordKeyWithSeq(key, NON_TRANSACTION_SEQ_NO), Type: data.LOG_RECORD_DELETED}
	pos, err := db.appendLogRecordWithLock(lr)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造 LogRecord
	lr := &data.LogRecord{Key: logRecordKeyWithSeq(key, NON_TRANSACTION_SEQ_NO), Value: value, Type: data.LOG_RECORD_NORMAL}

	// 写入数据文件
	pos, err := db.appendLogRecordWithLock(lr)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		// 更新无效数据的大小
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(lr *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(lr)
}

// appendLogRecord 将记录写入数据文件，返回对应的位置索引
func (db *DB) appendLogRecord(lr *data.LogRecord) (*data.LogRecordPos, error) {
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

	db.bytesWrite += uint(len)

	var needSync = db.options.SyncWrite
	if !needSync && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.bytesWrite = 0
	}

	return &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(len)}, nil
}

// setActiveDataFile 设置新的活跃数据文件
// 调用时需要加锁
func (db *DB) setActiveDataFile() error {
	var initialId uint32 = 0
	if db.activeFile != nil {
		initialId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialId, fio.STANDARD_FIO)
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

	return db.getValueByPosition(pos)
}

// 根据索引信息获取对应的 value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
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

// ListKeys 获取所有 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, 0, db.index.Size())
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys = append(keys, iterator.Key())
	}
	return keys
}

func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic("failed to unlock the directory")
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, f := range db.olderFiles {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// Backup copies the database directory while blocking concurrent database writes.
func (db *DB) Backup(dstDir string, excludeDataFiles []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile != nil {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
	}

	exclusions := make([]string, 0, len(excludeDataFiles)+1)
	exclusions = append(exclusions, excludeDataFiles...)
	exclusions = append(exclusions, FILE_LOCK_NAME)
	return utils.Backup(db.options.DirPath, dstDir, exclusions)
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	s := uint(len(db.olderFiles))
	if db.activeFile != nil {
		s++
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		return nil
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     s,
		ReclaimableSize: db.reclaimSize,
		DisSize:         dirSize,
	}
}
