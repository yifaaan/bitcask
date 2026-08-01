package bitcask

import (
	"encoding/binary"
	"sync"

	"github.com/yifaaan/bitcask/data"
)

// 非事务提交的序列号
const NON_TRANSACTION_SEQ_NO uint64 = 0

var txnFinishKey = []byte("txn-finish")

// WriteBatch 批量原子写数据
type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
	options       WriteBatchOptions
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
		options:       opts,
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	lr := &data.LogRecord{Key: key, Value: value, Type: data.LOG_RECORD_NORMAL}
	wb.pendingWrites[string(key)] = lr
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	pos := wb.db.index.Get(key)
	if pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	lr := &data.LogRecord{Key: key, Type: data.LOG_RECORD_DELETED}
	wb.pendingWrites[string(key)] = lr
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > int(wb.options.MaxBatchNum) {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := wb.db.seqNo.Add(1)

	positons := make(map[string]*data.LogRecordPos)
	for _, lr := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(lr.Key, seqNo),
			Value: lr.Value,
			Type:  lr.Type,
		})
		if err != nil {
			return err
		}
		positons[string(lr.Key)] = pos
	}

	// 写入标识事务结束的记录
	finRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinishKey, seqNo),
		Type: data.LOG_RECORD_TXN_FINISH,
	}
	if _, err := wb.db.appendLogRecord(finRecord); err != nil {
		return err
	}

	if wb.options.SyncWrite && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, lr := range wb.pendingWrites {
		pos := positons[string(lr.Key)]
		if lr.Type == data.LOG_RECORD_NORMAL {
			wb.db.index.Put(lr.Key, pos)
		}
		if lr.Type == data.LOG_RECORD_DELETED {
			wb.db.index.Delete(lr.Key)
		}
	}

	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// logRecordKeyWithSeq seqNo+key 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	buf := make([]byte, n+len(key))
	copy(buf[:n], seq[:n])
	copy(buf[n:], key)

	return buf
}

func parseLogRecordKeyWithSeq(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
