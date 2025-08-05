package bitcask

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	// 只用于加载索引时
	fileIds []int
	// 事务序列号
	seqNo uint64
	// 是否正在合并
	isMerging bool
}

// 打开存储引擎实例
func Open(options Options) (*DB, error) {
	// 检验配置
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，不存在需要创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化DB结构
	db := &DB{
		options:    options,
		mu:         &sync.RWMutex{},
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从hint文件加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 根据数据文件构造内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// 写入key/value数据，key不能为空
func (db *DB) Put(key, value []byte) error {
	// key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, noTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 向当前活跃数据文件追加写入一条记录
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 写入之后，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdataFailed
	}
	return nil
}

// 根据key读value
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引获取索引信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(pos)
}

// 根据位置信息获取对应的 value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// 根据文件id找到数据文件
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
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	// 数据已经被删除
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// 删除key对应的value
func (db *DB) Delete(key []byte) error {
	// 判断key有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 检查内存索引key是否存在,不存在之间返回就行
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造一条删除记录
	logRecord := &data.LogRecord{Key: logRecordKeyWithSeqNo(key, noTransactionSeqNo), Type: data.LogRecordDelete}
	// 写入到数据文件
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中删除key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdataFailed
	}
	return nil
}

// 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭活跃数据文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 持久化
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭活跃数据文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}

	// 关闭旧数据文件
	for _, file := range db.olderFiles {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// 获取所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, 0, db.index.Size())
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys = append(keys, iterator.Key())
	}
	return keys
}

// 对所有数据执行指定操作，函数返回false时退出
func (db *DB) Fold(f func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return nil
		}
		if !f(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 向当前活跃数据文件追加写入一条记录
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 向当前活跃数据文件追加写入一条记录，不加锁
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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

// 从磁盘加载数据文件
func (db *DB) loadDataFiles() error {
	// 读取目录
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 找到所有以.data结尾的数据文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id排序，从小到大依次加载数据文件
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历打开每个数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 如果是活跃数据文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件加载索引
// 有事务seqNo时，先暂存相同seqNo的记录，直到读到各自的txn_fin记录，再更新索引
func (db *DB) loadIndexFromDataFiles() error {
	// 空数据库
	if len(db.fileIds) == 0 {
		return nil
	}

	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		nonMergeFileId, err = db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
	}

	updataIndex := func(pos *data.LogRecordPos, key []byte, t data.LogRecordType) bool {
		var ok bool
		// 如果记录是删除的，从index删除即可
		if t == data.LogRecordDelete {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("update index failed at loadIndexFromDataFiles")
		}
		return ok
	}

	// 当前最大的事务序列号
	var maxTxnSeqNo uint64 = noTransactionSeqNo

	for i, fid := range db.fileIds {
		fileId := uint32(fid)
		// 已经从hint文件加载过了，跳过
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 暂存事务提交的记录+位置，后面读到txnfin后，批量更新
		// 这里面的key不包含seqNo
		transactionRecords := make(map[uint64][]*data.TransactionRecord)
		// 处理每一个数据文件
		var offset int64
		for {
			// 读取一条数据记录
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建一条索引
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			seqNo, realKey := parseLogRecordKey(logRecord.Key)
			// 非事务提交的记录
			if seqNo == noTransactionSeqNo {
				updataIndex(logRecordPos, realKey, logRecord.Type)
			} else {
				// seqNo对应事务结束的记录
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, tsRecord := range transactionRecords[seqNo] {
						updataIndex(tsRecord.Pos, tsRecord.Record.Key, tsRecord.Record.Type)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 暂存同一个事务的记录
					logRecord.Key = realKey
					transactionRecord := &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					}
					transactionRecords[seqNo] = append(transactionRecords[seqNo], transactionRecord)
				}
			}

			// 更新当前最大的事务序列号
			if seqNo > maxTxnSeqNo {
				maxTxnSeqNo = seqNo
			}

			// 更新下一个记录的读取位置
			offset += size
		}

		// 如果是当前活跃数据文件，需要更新这个文件的写偏移
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	// 更新事务序列号
	db.seqNo = maxTxnSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
