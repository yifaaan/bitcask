package benchmark

import (
	"strconv"
	"testing"

	bitcask "github.com/yifaaan/bitcask"
)

const (
	benchmarkKeyCount  = 1024
	benchmarkValueSize = 128
	benchmarkBatchSize = 100
)

func openBenchmarkDB(b *testing.B) *bitcask.DB {
	b.Helper()

	options := bitcask.DefaultOptions
	options.DirPath = b.TempDir()

	db, err := bitcask.Open(options)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Errorf("Close() error = %v", err)
		}
	})
	return db
}

func benchmarkKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte("key-" + strconv.Itoa(i))
	}
	return keys
}

func benchmarkValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = 'v'
	}
	return value
}

func populateBenchmarkDB(b *testing.B, db *bitcask.DB, keys [][]byte, value []byte) {
	b.Helper()

	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put(%q) error = %v", key, err)
		}
	}
}

func BenchmarkDBPut(b *testing.B) {
	db := openBenchmarkDB(b)
	keys := benchmarkKeys(benchmarkKeyCount)
	value := benchmarkValue(benchmarkValueSize)

	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()

	keyIndex := 0
	for b.Loop() {
		if err := db.Put(keys[keyIndex], value); err != nil {
			b.Fatalf("Put() error = %v", err)
		}
		keyIndex++
		if keyIndex == len(keys) {
			keyIndex = 0
		}
	}
}

func BenchmarkDBPutOverwrite(b *testing.B) {
	db := openBenchmarkDB(b)
	key := []byte("overwrite-key")
	value := benchmarkValue(benchmarkValueSize)

	if err := db.Put(key, value); err != nil {
		b.Fatalf("initial Put() error = %v", err)
	}

	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put() error = %v", err)
		}
	}
}

func BenchmarkDBGet(b *testing.B) {
	db := openBenchmarkDB(b)
	keys := benchmarkKeys(benchmarkKeyCount)
	value := benchmarkValue(benchmarkValueSize)
	populateBenchmarkDB(b, db, keys, value)

	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()

	keyIndex := 0
	for b.Loop() {
		got, err := db.Get(keys[keyIndex])
		if err != nil {
			b.Fatalf("Get() error = %v", err)
		}
		if len(got) != len(value) {
			b.Fatalf("Get() returned %d bytes, want %d", len(got), len(value))
		}
		keyIndex++
		if keyIndex == len(keys) {
			keyIndex = 0
		}
	}
}

func BenchmarkDBWriteBatchCommit(b *testing.B) {
	db := openBenchmarkDB(b)
	keys := benchmarkKeys(benchmarkBatchSize)
	value := benchmarkValue(benchmarkValueSize)

	b.SetBytes(int64(benchmarkBatchSize * (len(keys[0]) + len(value))))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		batch := db.NewWriteBatch(bitcask.WriteBatchOptions{
			MaxBatchNum: benchmarkBatchSize,
		})
		for _, key := range keys {
			if err := batch.Put(key, value); err != nil {
				b.Fatalf("batch.Put(%q) error = %v", key, err)
			}
		}
		if err := batch.Commit(); err != nil {
			b.Fatalf("batch.Commit() error = %v", err)
		}
	}
}

func BenchmarkDBListKeys(b *testing.B) {
	db := openBenchmarkDB(b)
	keys := benchmarkKeys(benchmarkKeyCount)
	value := benchmarkValue(benchmarkValueSize)
	populateBenchmarkDB(b, db, keys, value)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		got := db.ListKeys()
		if len(got) != len(keys) {
			b.Fatalf("ListKeys() returned %d keys, want %d", len(got), len(keys))
		}
	}
}

func BenchmarkDBFold(b *testing.B) {
	db := openBenchmarkDB(b)
	keys := benchmarkKeys(benchmarkKeyCount)
	value := benchmarkValue(benchmarkValueSize)
	populateBenchmarkDB(b, db, keys, value)

	b.SetBytes(int64(len(keys) * (len(keys[0]) + len(value))))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		count := 0
		err := db.Fold(func(_, got []byte) bool {
			if len(got) != len(value) {
				b.Fatalf("Fold() returned %d bytes, want %d", len(got), len(value))
			}
			count++
			return true
		})
		if err != nil {
			b.Fatalf("Fold() error = %v", err)
		}
		if count != len(keys) {
			b.Fatalf("Fold() visited %d keys, want %d", count, len(keys))
		}
	}
}
