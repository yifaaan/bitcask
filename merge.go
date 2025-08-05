package bitcask

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/yifaaan/bitcask/data"
)

const mergeDirSuffix = "-merge"
const mergeFinishedKey = "merge-finished"

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	// 如果正在merge，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsRunning
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃数据文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 将当前活跃文件转化为旧数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃数据文件用来处理后续用户的写入
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 记录最近没有参与merge的文件id
	nonMergeFileId := db.activeFile.FileId
	// 提取当前所有旧的数据文件来merge
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// 释放锁,因为后续用户的写入不会影响所有需要merge的文件
	db.mu.Unlock()
	// id从小到大排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果merge目录存在，则删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 创建merge目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开新的临时bitcask实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开hint文件,存储索引信息
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 去掉seqNo，得到真正的key
			_, realKey := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 和内存索引中的位置比较
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 重写有效数据（没必要重写seqNo）
				logRecord.Key = logRecordKeyWithSeqNo(realKey, noTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将位置索引信息写入hint文件(key-pos)
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// 持久化hint文件
	if err := hintFile.Sync(); err != nil {
		return err
	}
	// 持久化临时bitcask实例
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	// 打开标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{Key: []byte(mergeFinishedKey), Value: []byte(strconv.Itoa(int(nonMergeFileId)))}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// /tmp/bitcask
// /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	// 获取父目录
	dir := filepath.Dir(path.Clean(db.options.DirPath))
	// 获取文件名
	base := filepath.Base(db.options.DirPath)
	// 拼接路径
	return filepath.Join(dir, base+mergeDirSuffix)
}

// 加载merge目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		return nil
	}
	defer func() {
		os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找merge-finished文件，判断merge是否完成
	var mergeFileNames []string
	var mergeFinished bool
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := filepath.Join(db.options.DirPath, fmt.Sprintf("%09d%s", fileId, data.DataFileNameSuffix))
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(mergePath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	var offset int64 = 0
	for {
		record, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)
		offset += size
	}
	return nil
}
