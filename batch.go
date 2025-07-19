package bitcask

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/yifaaan/bitcask/data"
)

// 非事务的记录，seqNo为0
const noTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn_fin")

// 原子批量写数据，额外添加最后一个记录表示事务完成
type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	options       WriteBatchOptions
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:            &sync.Mutex{},
		db:            db,
		options:       opts,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) >= int(wb.options.MaxBatchNum) {
		return ErrExceedMaxBatchNum
	}

	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if pos := wb.db.index.Get(key); pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDelete}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交事务，写入批量数据
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) >= int(wb.options.MaxBatchNum) {
		return ErrExceedMaxBatchNum
	}

	// 保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	// 获取最新事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	logRecordPositons := make(map[string]*data.LogRecordPos, len(wb.pendingWrites))
	// 写入数据
	for _, record := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeqNo(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		logRecordPositons[string(record.Key)] = pos
	}

	// 写标识事务完成的记录
	finishRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishRecord); err != nil {
		return err
	}

	// 根据配置是否写持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := logRecordPositons[string(record.Key)]
		if record.Type == data.LogRecordDelete {
			wb.db.index.Delete([]byte(record.Key))
		}
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put([]byte(record.Key), pos)
		}
	}

	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key + seqNo
func logRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	// seqNo使用变长编码
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析key，返回seqNo和key
func parseLogRecordKey(key []byte) (uint64, []byte) {
	seqNo, n := binary.Uvarint(key)
	return seqNo, key[n:]
}
