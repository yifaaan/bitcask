package bitcask

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/yifaaan/bitcask/data"
	"github.com/yifaaan/bitcask/utils"
)

const (
	MERGE_DIR_NAME     = "-merge"
	MERGE_FINISHED_KEY = "merge.finished"
)

var availableDiskSizeFn = utils.AvailableDiskSize

// Merge 清理无效数据，生成 hint 索引文件
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsInPrograss
	}

	// 查看可 merge 的数据量是否达到阈值
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(dirSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	// 查看剩余空间能否容纳 merge 之后的数据量
	availableDiskSize, err := availableDiskSizeFn()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(dirSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 打开新的活跃文件处理用户请求，然后merge所有旧文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}
	mergeFinFid := db.activeFile.FileId

	// 所有待 merge 的文件
	mergeFiles := make([]*data.DataFile, 0, len(db.olderFiles))
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	slices.SortFunc(mergeFiles, func(a, b *data.DataFile) int {
		return cmp.Compare(a.FileId, b.FileId)
	})

	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 在 merge 目录打开新的空 DB
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrite = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	for _, df := range mergeFiles {
		var off int64 = 0

		for {
			lr, len, err := df.ReadLogRecord(off)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析出真实的 key
			realKey, _ := parseLogRecordKeyWithSeq(lr.Key)
			// 从 db 获取该记录的位置, 如果有效就重写
			pos := db.index.Get(realKey)
			if pos != nil && pos.Fid == df.FileId && pos.Offset == off {
				// 清除旧的事务序列号
				lr.Key = logRecordKeyWithSeq(realKey, NON_TRANSACTION_SEQ_NO)
				newPos, err := mergeDB.appendLogRecord(lr)
				if err != nil {
					return err
				}

				// 将 pos 写入 hint 文件（使用原始 key，不带事务序列号前缀）
				if err := hintFile.WriteHintRecord(realKey, newPos); err != nil {
					return err
				}
			}
			off += len
		}
	}
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 创建标识 merge 完成的文件，写入 fid 最小的没有参与 merge 的文件(小于该fid的都merge过了)
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(MERGE_FINISHED_KEY),
		Value: []byte(strconv.Itoa(int(mergeFinFid))),
	}
	buf, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinFile.Write(buf); err != nil {
		return err
	}
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	// 关闭 merge 过程中打开的临时文件，释放文件句柄
	if err := hintFile.Close(); err != nil {
		return err
	}
	if err := mergeFinFile.Close(); err != nil {
		return err
	}
	if err := mergeDB.Close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergePath() string {
	parent := filepath.Dir(filepath.Clean(db.options.DirPath))
	base := filepath.Base(db.options.DirPath)
	return filepath.Join(parent, base+MERGE_DIR_NAME)
}

// loadMergeFiles 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntires, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 找标识 merge 完成的文件
	var mergeFileNames []string
	var mergeFinished bool
	for _, entry := range dirEntires {
		if entry.Name() == FILE_LOCK_NAME {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
		if entry.Name() == data.MERGE_FINISHED_FILE_NAME {
			mergeFinished = true
		}
	}
	if !mergeFinished {
		return nil
	}
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	// 删除已经 merge 的数据文件(id 比 nonMergeFileId 小)
	var fid uint32 = 0
	for ; fid < nonMergeFileId; fid++ {
		name := filepath.Join(db.options.DirPath, fmt.Sprintf("%09d", fid)+data.DATA_FILE_NAME_SUFFIX)
		if _, err := os.Stat(name); err == nil {
			if err := os.Remove(name); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录
	// 先关闭所有旧数据文件句柄，以便 Windows 上能重命名
	for _, f := range db.olderFiles {
		_ = f.Close()
	}
	db.olderFiles = make(map[uint32]*data.DataFile)

	for _, name := range mergeFileNames {
		srcPath := filepath.Join(mergePath, name)
		destPath := filepath.Join(db.options.DirPath, name)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(mergePath string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}
	defer mergeFinFile.Close()

	lr, _, err := mergeFinFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	fid, err := strconv.Atoi(string(lr.Value))
	if err != nil {
		return 0, err
	}
	return uint32(fid), nil
}

// loadIndexFromHintFile 从 hint 索引文件加载索引
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HINT_FILE_NAME)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	defer hintFile.Close()

	var off int64 = 0
	for {
		lr, len, err := hintFile.ReadLogRecord(off)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码 pos
		pos := data.DecodeLogRecordPos(lr.Value)
		db.index.Put(lr.Key, pos)
		off += len
	}
	return nil
}
