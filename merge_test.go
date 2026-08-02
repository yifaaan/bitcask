package bitcask

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/yifaaan/bitcask/data"
)

// mergeTestDB 打开一个数据文件较小、会频繁轮转的测试库
func mergeTestDB(t *testing.T) (*DB, Options) {
	t.Helper()

	opts := testOptions(t) // DataFileSize=64, SyncWrite=true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db, opts
}

// countDataFiles 统计数据目录里 .data 文件的个数
func countDataFiles(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), data.DATA_FILE_NAME_SUFFIX) {
			n++
		}
	}
	return n
}

func TestDBMergeRejectsWhenReclaimRatioUnreached(t *testing.T) {
	db, _ := mergeTestDB(t)
	defer closeTestDB(t, db)

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	db.options.DataFileMergeRatio = 1

	err := db.Merge()
	if !errors.Is(err, ErrMergeRatioUnreached) {
		t.Fatalf("Merge() error = %v, want %v", err, ErrMergeRatioUnreached)
	}
	if db.isMerging {
		t.Fatal("database should not remain in merging state after ratio rejection")
	}
}

func TestDBMergeRejectsWhenDiskSpaceIsInsufficient(t *testing.T) {
	db, _ := mergeTestDB(t)
	defer closeTestDB(t, db)

	if err := db.Put([]byte("key"), []byte("first")); err != nil {
		t.Fatalf("first Put() error = %v", err)
	}
	if err := db.Put([]byte("key"), []byte("second")); err != nil {
		t.Fatalf("second Put() error = %v", err)
	}
	db.options.DataFileMergeRatio = 0

	originalAvailableDiskSizeFn := availableDiskSizeFn
	availableDiskSizeFn = func() (uint64, error) {
		return 0, nil
	}
	defer func() {
		availableDiskSizeFn = originalAvailableDiskSizeFn
	}()

	err := db.Merge()
	if !errors.Is(err, ErrNoEnoughSpaceForMerge) {
		t.Fatalf("Merge() error = %v, want %v", err, ErrNoEnoughSpaceForMerge)
	}
	if db.isMerging {
		t.Fatal("database should not remain in merging state after disk-space rejection")
	}
}

func TestDBMergeCleansStaleRecords(t *testing.T) {
	db, opts := mergeTestDB(t)

	// 反复覆盖同一个 key，产生大量过期记录并触发文件轮转
	const rounds = 50
	for i := range rounds {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
		// 每个 key 覆盖若干次，制造 stale 记录
		for j := 0; j < 10; j++ {
			_ = db.Put([]byte(key), []byte(value))
		}
	}

	before := countDataFiles(t, opts.DirPath)
	if before == 0 {
		t.Fatal("expected at least one data file before merge")
	}
	for i := range rounds {
		requireValue(t, db, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// merge 后数据仍然可读
	for i := range rounds {
		requireValue(t, db, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}
	closeTestDB(t, db)

	// merge 把有效记录压缩进 merge 目录，旧数据文件要等 reopen 时由 loadMergeFiles
	// 删除/替换。因此磁盘实际瘦身发生在重新打开之后：比较 reopen 前后的数据文件数量。
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after merge error = %v", err)
	}
	defer closeTestDB(t, reopened)

	after := countDataFiles(t, opts.DirPath)
	if after == 0 {
		t.Fatal("expected at least one data file after merge + reopen")
	}
	if after >= before {
		t.Fatalf("merge should reduce data-file count: before=%d after=%d", before, after)
	}
	t.Logf("data files reduced: %d -> %d", before, after)

	// 重新打开后数据仍然正确
	for i := range rounds {
		requireValue(t, reopened, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}
}

func TestDBMergeDropDeletedKeys(t *testing.T) {
	db, _ := mergeTestDB(t)
	defer closeTestDB(t, db)
	db.options.DataFileMergeRatio = 0

	// 写入几个 key，然后删除其中一些
	for _, key := range []string{"keep-a", "keep-b", "gone-a", "gone-b"} {
		if err := db.Put([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}
	if err := db.Delete([]byte("gone-a")); err != nil {
		t.Fatalf("Delete(%q) error = %v", "gone-a", err)
	}
	if err := db.Delete([]byte("gone-b")); err != nil {
		t.Fatalf("Delete(%q) error = %v", "gone-b", err)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// 保留的 key 仍在
	requireValue(t, db, "keep-a", "v")
	requireValue(t, db, "keep-b", "v")
	// 已删除的 key 消失且不报错
	for _, key := range []string{"gone-a", "gone-b"} {
		if _, err := db.Get([]byte(key)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Get(%q) after merge error = %v, want %v", key, err, ErrKeyNotFound)
		}
	}
}

func TestDBMergeReopenLoadsHintFile(t *testing.T) {
	opts := testOptions(t)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	db.options.DataFileMergeRatio = 0

	// 制造多个数据文件 + 覆盖记录
	// 注意：每个 key 的第 2 次 Put 会把数据写入"新轮转出的活跃文件"。merge 只处理
	// olderFiles，所以这些 key 的最新值可能落在 merge 活跃文件里，不会进 hint 文件 ——
	// 这正是本测试要覆盖的"merge 与较新数据共存"场景。
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := db.Put([]byte(key), []byte("v1")); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
		_ = db.Put([]byte(key), []byte("v2"))
	}
	if err := db.Delete([]byte("key-0")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	mergePath := db.getMergePath()
	closeTestDB(t, db)

	// 重新打开前，merge 目录仍存在（尚未被 loadMergeFiles 清理）
	if _, err := os.Stat(mergePath); err != nil {
		t.Fatalf("merge directory %q should exist before reopen, but does not", mergePath)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after merge error = %v", err)
	}
	defer closeTestDB(t, db2)

	// loadMergeFiles 应在 reopen 时清理 merge 目录
	if _, err := os.Stat(mergePath); err == nil {
		t.Fatalf("merge directory %q should be cleaned up after reopen, but still exists", mergePath)
	}

	// 从 hint 文件 + 未 merge 的活跃文件共同恢复出所有数据
	for i := 1; i < 30; i++ {
		requireValue(t, db2, fmt.Sprintf("key-%d", i), "v2")
	}
	if _, err := db2.Get([]byte("key-0")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(key-0) after reopen error = %v, want %v", err, ErrKeyNotFound)
	}
}

func TestDBMergeConcurrentCallRejected(t *testing.T) {
	db, _ := mergeTestDB(t)
	defer closeTestDB(t, db)
	db.options.DataFileMergeRatio = 0

	// 写入一些数据确保有可 merge 的内容
	for i := range 20 {
		key := fmt.Sprintf("key-%d", i)
		_ = db.Put([]byte(key), []byte("v"))
		for j := 0; j < 5; j++ {
			_ = db.Put([]byte(key), []byte("v"))
		}
	}

	var firstErr, secondErr error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		firstErr = db.Merge()
	}()
	// 并行发起第二个 merge（与第一个可能有竞态，允许返回 ErrMergeIsInPrograss）
	go func() {
		defer wg.Done()
		secondErr = db.Merge()
	}()

	wg.Wait()

	// 两个合并中至少一个成功，且出错的那个必须是 ErrMergeIsInPrograss
	success, rejected := 0, 0
	for _, err := range []error{firstErr, secondErr} {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrMergeIsInPrograss):
			rejected++
		default:
			t.Fatalf("unexpected merge error = %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly one successful merge, got success=%d rejected=%d", success, rejected)
	}
}

func TestDBMergeEmptyDatabase(t *testing.T) {
	db, _ := mergeTestDB(t)
	defer closeTestDB(t, db)
	// 无活跃文件时不报错
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge() on empty database error = %v, want nil", err)
	}
}

// TestDBMergeDataIntegrity verifies merge does not change user-visible data after reopen
func TestDBMergeDataIntegrity(t *testing.T) {
	db, opts := mergeTestDB(t)
	db.options.DataFileMergeRatio = 0

	// 使用不同长度的 key 和 value，覆盖索引重建 + hint 文件加载
	expected := make(map[string]string)
	for i := range 60 {
		key := fmt.Sprintf("k%d", i)
		value := strings.Repeat("x", i)
		expected[key] = value
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	closeTestDB(t, db)

	// 重新打开验证完整数据
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen database error = %v", err)
	}
	defer closeTestDB(t, reopened)

	for key, value := range expected {
		requireValue(t, reopened, key, value)
	}
}
