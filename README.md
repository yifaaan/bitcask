# Bitcask

> 一个使用 Go 编写的嵌入式、追加写键值存储。
>
> 数据顺序写入磁盘，内存索引只保存每个 key 的最新位置，适合学习日志结构存储、崩溃恢复和索引实现。

## 目录

- [特性](#特性)
- [快速开始](#快速开始)
- [核心 API](#核心-api)
- [Merge 与恢复](#merge-与恢复)
- [实现概览](#实现概览)
- [项目结构](#项目结构)
- [测试与开发](#测试与开发)
- [更新日志](#更新日志)

## 特性

| 状态 | 能力 | 说明 |
| --- | --- | --- |
| [x] | 追加写存储 | 数据以 log record 形式顺序追加，避免随机更新数据文件。 |
| [x] | 数据恢复 | 启动时扫描数据文件并重建内存索引，支持最新值恢复。 |
| [x] | B-tree / ART | 可通过 `Options.IndexType` 选择 B-tree 或 ART 内存索引。 |
| [x] | mmap 启动加载 | 已有数据文件启动时可使用 mmap 读取，索引恢复完成后切换回标准文件 IO。 |
| [x] | 文件锁 | 同一个数据目录同时只允许一个 `DB` 实例打开。 |
| [x] | WriteBatch | 支持 Put/Delete 批量原子提交和重启恢复。 |
| [x] | Merge | 清理失效记录、生成 hint 索引，并在下次打开数据库时应用结果。 |
| [x] | Merge 前置检查 | 检查回收比例和可用磁盘空间，避免低收益或空间不足的合并。 |
| [x] | 遍历 API | 支持 `ListKeys`、`Fold`、正逆序迭代、前缀过滤和 `Seek`。 |
| [x] | 运行统计 | `DB.Stat` 提供 key 数、数据文件数、可回收大小和目录大小。 |
| [x] | 跨平台磁盘检查 | 当前为 Linux 和 Windows 提供可用磁盘空间实现。 |

项目仍在持续开发中，当前定位是一个结构清晰、可测试的嵌入式存储实验项目，暂不提供 CLI、服务端或 Redis 协议兼容层。

## 快速开始

```go
package main

import (
	"fmt"
	"log"

	bitcask "github.com/yifaaan/bitcask"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "./data"

	db, err := bitcask.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("name"), []byte("bitcask")); err != nil {
		log.Fatal(err)
	}

	value, err := db.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("name=%s\n", value)
}
```

运行示例：

```powershell
go run .\examples\basic_operation.go
```

## 核心 API

### 基础读写

```go
if err := db.Put([]byte("user:1"), []byte("Alice")); err != nil {
	log.Fatal(err)
}

value, err := db.Get([]byte("user:1"))
if err != nil {
	log.Fatal(err)
}
fmt.Println(string(value))

if err := db.Delete([]byte("user:1")); err != nil {
	log.Fatal(err)
}
```

空 key 会返回 `ErrKeyIsEmpty`，读取不存在的 key 会返回 `ErrKeyNotFound`。

### WriteBatch

`WriteBatch` 将多条 Put/Delete 记录写入同一个事务序列，并以结束记录标识提交完成。恢复时，未看到结束记录的批次会被丢弃。

```go
batch := db.NewWriteBatch(bitcask.WriteBatchOptions{
	MaxBatchNum: 100,
	SyncWrite:   true,
})

if err := batch.Put([]byte("user:1"), []byte("Alice")); err != nil {
	log.Fatal(err)
}
if err := batch.Put([]byte("user:2"), []byte("Bob")); err != nil {
	log.Fatal(err)
}
if err := batch.Delete([]byte("user:old")); err != nil {
	log.Fatal(err)
}
if err := batch.Commit(); err != nil {
	log.Fatal(err)
}
```

### 遍历

`ListKeys` 返回当前索引中的 key，结果按字典序排列：

```go
for _, key := range db.ListKeys() {
	fmt.Println(string(key))
}
```

`Fold` 按正序读取 key 和 value。回调在数据库读锁期间执行，不要在回调中调用 `Put`、`Delete` 或 `Close`：

```go
err := db.Fold(func(key, value []byte) bool {
	fmt.Printf("%s = %s\n", key, value)
	return true
})
if err != nil {
	log.Fatal(err)
}
```

`NewIterator` 支持正序、逆序、前缀过滤和定位：

```go
it := db.NewIterator(bitcask.IteratorOptions{
	Prefix:  []byte("user:"),
	Reverse: false,
})
defer it.Close()

it.Rewind()
for it.Valid() {
	value, err := it.Value()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s = %s\n", it.Key(), value)
	it.Next()
}
```

正序迭代的 `Seek` 定位到第一个大于等于目标 key 的位置；逆序迭代定位到当前方向下第一个不大于目标 key 的位置。

### 统计信息

```go
stat := db.Stat()
if stat != nil {
	fmt.Printf(
		"keys=%d files=%d reclaimable=%dB dir=%dB\n",
		stat.KeyNum,
		stat.DataFileNum,
		stat.ReclaimableSize,
		stat.DisSize,
	)
}
```

字段含义：

- `KeyNum`：当前内存索引中的 key 数量。
- `DataFileNum`：当前打开的数据文件数量。
- `ReclaimableSize`：覆盖或删除后可由 Merge 回收的记录大小。
- `DisSize`：数据目录当前占用的字节数。

## 配置

默认配置定义在 `DefaultOptions`：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `DirPath` | `os.TempDir()` | 数据目录。 |
| `DataFileSize` | `256 MiB` | 单个数据文件达到该大小后轮换。 |
| `SyncWrite` | `false` | 是否在每次普通写入后同步数据。 |
| `IndexType` | `index.BTREE` | 内存索引类型，也可以设置为 `index.ART`。 |
| `BytesPerSync` | `0` | 累计写入达到该字节数后触发同步检查。 |
| `MMapAtStart` | `true` | 启动恢复阶段是否使用 mmap 读取已有数据文件。 |
| `DataFileMergeRatio` | `0.5` | 允许 Merge 的最低失效数据比例。 |

### 选择 ART 索引

```go
opts := bitcask.DefaultOptions
opts.DirPath = "./data"
opts.IndexType = index.ART

db, err := bitcask.Open(opts)
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

### mmap 启动加载

当 `MMapAtStart` 为 `true` 时，打开已有数据库的流程是：

```text
open data files -> mmap read -> load hint/index -> rebuild index -> standard FileIO
```

mmap 只用于启动阶段的只读加载。`Open` 返回前，数据文件会切换回标准 `FileIO`，因此后续写入、同步和关闭仍使用普通文件 IO。设置 `MMapAtStart = false` 可以关闭这项优化。

## Merge 与恢复

`DB.Merge` 会扫描旧数据文件，只保留当前索引仍指向的有效记录，并生成 hint 索引。为了控制收益和磁盘占用，Merge 开始前会检查：

1. `reclaimSize / dirSize >= DataFileMergeRatio`。
2. 当前可用磁盘空间大于预计保留数据大小。

检查失败时分别返回：

- `ErrMergeRatioUnreached`：失效数据比例没有达到阈值。
- `ErrNoEnoughSpaceForMerge`：可用空间不足以容纳合并后的数据。
- `ErrMergeIsInPrograss`：已有另一个 Merge 正在执行。

Merge 的结果先写入数据目录旁的临时目录。完成标记写入后，下次 `Open` 会：

1. 检测 `merge-finished`。
2. 删除已经参与 Merge 的旧数据文件。
3. 将新数据文件和 `hint-index` 移回正式数据目录。
4. 使用 hint 索引和未参与 Merge 的活跃文件恢复数据。
5. 清理临时 Merge 目录。

典型目录布局：

```text
./data/
|-- 000000000.data
|-- 000000001.data
|-- hint-index
|-- merge-finished
`-- flock

./data-merge/
|-- 000000000.data
|-- hint-index
`-- merge-finished
```

磁盘空间检测目前提供 Linux 和 Windows 实现，使用当前工作目录所在文件系统的可用空间进行判断。

## 实现概览

### 写入与读取路径

```text
DB.Put
  -> appendLogRecordWithLock
  -> DataFile
  -> IOManager
  -> FileIO
  -> os.File
```

内存索引只保存最新位置：

```text
key -> { file id, file offset, record size }
```

读取和遍历路径：

```text
DB -> Indexer -> B-tree / ART -> DataFile.ReadLogRecord
```

### Log record 格式

每条记录由 CRC、类型、长度字段、key 和 value 组成：

```text
CRC | record type | key size | value size | key | value
```

`key size` 和 `value size` 使用 varint 编码。当前记录类型包括：

| 类型 | 含义 |
| --- | --- |
| `LOG_RECORD_NORMAL` | 普通写入记录。 |
| `LOG_RECORD_DELETED` | 删除标记记录。 |
| `LOG_RECORD_TXN_FINISH` | WriteBatch 事务结束记录。 |

### 文件锁

打开数据库时会在数据目录创建并持有 `flock`。同一个目录被另一个 `DB` 实例占用时，`Open` 返回 `ErrDatabaseIsUsing`。调用 `DB.Close` 后释放锁，其他进程才可以重新打开该目录。

## 项目结构

```text
.
|-- db.go                   数据库打开、恢复、键值操作和统计
|-- db_test.go              数据库、文件锁和 mmap 启动测试
|-- iterator.go             用户侧迭代器
|-- iterator_test.go        迭代器测试
|-- batch.go                WriteBatch 实现
|-- batch_test.go           WriteBatch 测试
|-- merge.go                Merge、hint 和恢复逻辑
|-- merge_test.go           Merge 场景和前置检查测试
|-- options.go              数据库及批量写入配置
|-- errors.go               包级错误定义
|-- utils/
|   |-- file.go              目录大小统计
|   |-- disk_linux.go        Linux 可用磁盘空间
|   `-- disk_windows.go      Windows 可用磁盘空间
|-- data/
|   |-- data_file.go         数据文件和 hint 文件
|   |-- log_record.go        日志记录编解码
|   `-- *_test.go            数据文件测试
|-- fio/
|   |-- file_io.go           标准文件 IO
|   |-- mmap.go              只读 mmap IO
|   `-- *_test.go            IO 测试
|-- index/
|   |-- btree.go             B-tree 索引
|   |-- art.go               ART 索引
|   `-- *_test.go            索引测试
|-- examples/
|   `-- basic_operation.go   基础操作示例
|-- go.mod
`-- README.md
```

## 测试与开发

环境要求：Go 1.25.0 或更高版本。

运行全部测试：

```powershell
go test ./... -count=1
```

运行静态检查：

```powershell
go vet ./...
```

运行 Merge 测试：

```powershell
go test . -run '^TestDBMerge' -v
```

运行 mmap 和文件锁测试：

```powershell
go test ./fio -run '^TestMMap' -v
go test . -run '^TestDB(FileLock|MMapAtStart)' -v
```

运行示例：

```powershell
go run .\examples\basic_operation.go
```

## 更新日志

### 2026-08-02

- 完善 Merge 前置检查：回收比例不足时返回 `ErrMergeRatioUnreached`，可用空间不足时返回 `ErrNoEnoughSpaceForMerge`。
- 增加 Merge 拒绝分支、并发调用、删除 key、hint 恢复和数据完整性测试。
- 增加 `DB.Stat`，记录 key 数、数据文件数、可回收数据大小和目录占用空间。
- 增加 Linux/Windows 可用磁盘空间查询，并在测试中支持注入空间探针。
- 补充 mmap 启动加载后切换标准文件 IO、文件锁和索引实现的测试覆盖。

### 2026-08-01

- 支持启动阶段使用 mmap 读取已有数据文件。
- 启动恢复完成后将数据文件重置为标准文件 IO，保证后续写入流程可用。
- 补充文件 IO、mmap、数据文件轮换和数据库恢复测试。

## 后续计划

- 增加崩溃模拟、损坏记录和边界恢复测试。
- 完善 Merge 失败恢复、临时目录清理和大数据量压力测试。
- 增加基准测试和数据备份能力。
- 评估 CLI、HTTP API 以及更多 Redis 数据结构支持。

## 许可证

本项目使用 MIT License 发布。

详细内容请参阅 [LICENSE](LICENSE.md)。

Copyright 2025 Yifan Liu
