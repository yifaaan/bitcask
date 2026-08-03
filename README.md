# Bitcask

> 一个使用 Go 编写的嵌入式、追加写键值存储。
>
> 数据顺序写入磁盘，内存索引只保存每个 key 的最新位置，适合学习日志结构存储、崩溃恢复和索引实现。

## 目录

- [特性](#特性)
- [快速开始](#快速开始)
- [核心 API](#核心-api)
- [Redis 数据结构](#redis-数据结构)
- [HTTP API](#http-api)
- [目录备份](#目录备份)
- [配置](#配置)
- [Merge 与恢复](#merge-与恢复)
- [实现概览](#实现概览)
- [项目结构](#项目结构)
- [基准测试](#基准测试)
- [测试与开发](#测试与开发)

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
| [x] | 目录备份 | `utils.Backup` 和 `DB.Backup` 递归复制数据目录，并支持 glob 模式排除数据文件。 |
| [x] | 遍历 API | 支持 `ListKeys`、`Fold`、正逆序迭代、前缀过滤和 `Seek`。 |
| [x] | 运行统计 | `DB.Stat` 提供 key 数、数据文件数、可回收大小和目录大小。 |
| [x] | 跨平台磁盘检查 | 当前为 Linux 和 Windows 提供可用磁盘空间实现。 |
| [x] | HTTP API 示例 | `http/main.go` 提供写入、读取、删除、列出 key 和查看统计信息的 HTTP 路由。 |
| [x] | Go 基准测试 | `benchmark/bench_test.go` 覆盖 Put、覆盖写、Get、WriteBatch、ListKeys 和 Fold。 |
| [x] | Redis 字符串、HASH、SET 和 LIST 基础 API | `redis` 包提供带 TTL 的 `Set`、`Get`，以及 `HSet`、`HGet`、`HDel`、`SAdd`、`SIsMember`、`SRem`、`LPush`、`RPush`、`LPop`、`RPop`、`Del` 和 `Type` 操作。 |

项目仍在持续开发中，当前定位是一个结构清晰、可测试的嵌入式存储实验项目，并附带 HTTP 服务示例和 Redis 风格字符串、HASH、SET、LIST 封装。暂不提供 CLI 或完整 Redis 协议兼容层。

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

## Redis 数据结构

`redis` 包在 Bitcask 的 key-value 存储之上增加 Redis 风格的类型前缀和过期时间。字符串 value 编码为：

```text
data type | expire timestamp | payload
```

HASH 使用一条 metadata 记录保存类型、过期时间、版本和 field 数量，field value 使用以下内部 key 定位：

```text
hash key | version | field
```

SET 使用一条 metadata 记录保存类型、过期时间、版本和 member 数量，member 使用以下内部 key 定位；成员 key 的 value 为空，仅通过 key 是否存在表示成员关系：

```text
set key | version | member | member length
```

LIST 使用一条 metadata 记录保存类型、过期时间、版本、元素数量以及左右边界。元素使用以下内部 key 定位，`index` 是按左右 push 方向移动的无符号整数：

```text
list key | version | index
```

当前 API：

| API | 说明 |
| --- | --- |
| `NewRedisDatastruct(options)` | 使用 Bitcask 配置创建 Redis 数据封装。方法名沿用当前代码中的 `Datastruct` 拼写。 |
| `Set(key, ttl, value)` | 写入字符串 value；`ttl = 0` 表示不过期，正数表示相对过期时间。 |
| `Get(key)` | 读取字符串 value；过期 value 返回 `nil, nil`，类型不匹配返回 `ErrWrongTypeOperation`。 |
| `HSet(key, field, value)` | 设置 HASH field；新增 field 返回 `true`，更新已有 field 返回 `false`。 |
| `HGet(key, field)` | 读取 HASH field；HASH 不存在或 size 为 0 时返回 `nil, nil`，已有其他 field 但目标 field 不存在时返回 `bitcask.ErrKeyNotFound`。 |
| `HDel(key, field)` | 删除 HASH field；实际删除返回 `true`，field 不存在返回 `false`。 |
| `SAdd(key, member)` | 添加 SET member；新增 member 返回 `true`，重复 member 返回 `false`。 |
| `SIsMember(key, member)` | 判断 member 是否属于 SET；存在返回 `true`，SET 或 member 不存在返回 `false`。 |
| `SRem(key, member)` | 删除 SET member；实际删除返回 `true`，member 不存在返回 `false`。 |
| `LPush(key, element)` | 从 LIST 左侧插入 element；返回插入后的 LIST 长度。 |
| `RPush(key, element)` | 从 LIST 右侧插入 element；返回插入后的 LIST 长度。 |
| `LPop(key)` | 移除并返回 LIST 左侧元素；LIST 不存在或为空时返回 `nil, nil`。 |
| `RPop(key)` | 移除并返回 LIST 右侧元素；LIST 不存在或为空时返回 `nil, nil`。 |
| `Del(key)` | 删除 key；删除不存在的 key 不返回错误。 |
| `Type(key)` | 返回 `STRING`、`HASH`、`SET`、`LIST` 或 `ZSET` 类型标记。 |

使用示例：

```go
package main

import (
	"fmt"
	"log"
	"time"

	bitcask "github.com/yifaaan/bitcask"
	"github.com/yifaaan/bitcask/redis"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "./redis-data"

	rds, err := redis.NewRedisDatastruct(opts)
	if err != nil {
		log.Fatal(err)
	}

	if err := rds.Set([]byte("name"), time.Hour, []byte("bitcask")); err != nil {
		log.Fatal(err)
	}

	value, err := rds.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))

	dataType, err := rds.Type([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dataType == redis.STRING)

	hashKey := []byte("user:1")
	isNew, err := rds.HSet(hashKey, []byte("name"), []byte("Alice"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(isNew)

	fieldValue, err := rds.HGet(hashKey, []byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(fieldValue))

	deleted, err := rds.HDel(hashKey, []byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(deleted)

	setKey := []byte("user:1:tags")
	isNew, err = rds.SAdd(setKey, []byte("go"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(isNew)

	isMember, err := rds.SIsMember(setKey, []byte("go"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(isMember)

	deleted, err = rds.SRem(setKey, []byte("go"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(deleted)

	listKey := []byte("user:1:events")
	length, err := rds.RPush(listKey, []byte("login"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(length)

	length, err = rds.LPush(listKey, []byte("open"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(length)

	listValue, err := rds.LPop(listKey)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(listValue))
}
```

目前实现字符串 value 的 `Set` / `Get`、HASH 的 `HSet` / `HGet` / `HDel`、SET 的 `SAdd` / `SIsMember` / `SRem`、LIST 的 `LPush` / `RPush` / `LPop` / `RPop`，以及通用的 `Del` / `Type`。`ZSET` 目前仍只是编码类型常量，尚未提供对应的数据结构操作。`Set` 收到 `nil` value 时不会创建 key；当前 `RedisDataStruct` 也尚未暴露 `Close` 和 `Sync` 方法，适合作为正在扩展中的实验性封装使用。

## HTTP API

`http/main.go` 提供一个基于标准库 `net/http` 的示例服务。从仓库根目录运行：

```powershell
go run .\http
```

服务默认监听 `localhost:8080`，数据目录为当前工作目录下的 `./bitcask-db/`。路由如下：

| 方法 | 路径 | 请求 | 响应 |
| --- | --- | --- | --- |
| `POST` | `/bitcask/put` | JSON 对象，例如 `{"name":"bitcask"}`。 | 写入对象中的所有 key-value，成功时返回空响应。 |
| `GET` | `/bitcask/get?key=name` | 通过 `key` 查询参数指定 key。 | JSON 字符串。 |
| `DELETE` | `/bitcask/delete?key=name` | 通过 `key` 查询参数指定 key。 | JSON 字符串 `"OK"`。 |
| `GET` | `/bitcask/listkeys` | 无请求体。 | 当前 key 的 JSON 字符串数组。 |
| `GET` | `/bitcask/stat` | 无请求体。 | `Stat` 结构的 JSON 对象。 |

PowerShell 调用示例：

```powershell
$base = "http://localhost:8080/bitcask"

Invoke-RestMethod -Method Post -Uri "$base/put" `
    -ContentType "application/json" `
    -Body '{"name":"bitcask","version":"1"}'

Invoke-RestMethod -Method Get -Uri "$base/get?key=name"
Invoke-RestMethod -Method Get -Uri "$base/listkeys"
Invoke-RestMethod -Method Get -Uri "$base/stat"
Invoke-RestMethod -Method Delete -Uri "$base/delete?key=name"
```

这是用于演示数据库 API 的最小服务，不包含认证、请求体大小限制、并发策略和完整的 HTTP 错误码映射。当前示例中，读取不存在的 key 返回空字符串，删除不存在的 key 返回 `"OK"`。

## 目录备份

`utils.Backup` 将数据目录递归复制到目标目录，并保留目录结构。`excludeDataFiles` 使用 `filepath.Match` 模式匹配每个文件或目录的名称，例如 `*.data` 可以排除所有数据文件；目标目录不存在时会自动创建。

```go
import "github.com/yifaaan/bitcask/utils"

err := utils.Backup(
	"./data",
	"./backup",
	[]string{
		"*.data",
	},
)
if err != nil {
	log.Fatal(err)
}
```

上例会复制 `hint-index` 和其他未排除文件，但跳过所有 `.data` 文件。也可以直接通过 `DB.Backup` 调用：

```go
if err := db.Backup("./backup", []string{"*.data"}); err != nil {
	log.Fatal(err)
}
```

`DB.Backup` 会自动排除运行中的 `flock` 文件，并在复制前同步 active 数据文件；直接使用 `utils.Backup` 时，如果源数据库仍处于打开状态，需要由调用方自行排除 `flock` 或先关闭数据库。备份函数会返回遍历和模式匹配错误。

`utils.Backup` 只负责文件级复制，不会获取 `DB` 的内部锁，也不提供数据库级快照。为了保证备份内容一致，建议在关闭数据库或由调用方暂停写入后执行备份。

## 配置

默认配置定义在 `DefaultOptions`：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `DirPath` | `os.TempDir()` | 数据目录。 |
| `DataFileSize` | `256 MiB` | 单个数据文件达到该大小后轮换。 |
| `SyncWrite` | `false` | 是否在每次普通写入后同步数据。 |
| `IndexType` | `index.BTREE` | 内存索引类型，也可以设置为 `index.ART`。 |
| `BytesPerSync` | `0` | 累计写入达到该字节数后触发同步检查；按当前实现，`0` 会使每次写入都触发同步。 |
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
|   |-- backup.go             数据目录备份
|   |-- backup_test.go        备份和排除规则测试
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
|-- redis/
|   |-- types.go             Redis 风格字符串、TTL、HASH、SET 和 LIST 操作
|   |-- generic.go           Del 和 Type 操作
|   |-- meta.go              HASH/SET/LIST metadata 编解码
|   `-- types_test.go        Redis 基础 API 测试
|-- examples/
|   `-- basic_operation.go   基础操作示例
|-- http/
|   `-- main.go              HTTP API 示例服务
|-- benchmark/
|   `-- bench_test.go        Go benchmark 基准测试
|-- go.mod
`-- README.md
```

## 基准测试

基准测试位于 `benchmark/bench_test.go`，每个 benchmark 使用独立临时目录，数据库打开、预填充和关闭不会计入测量区间。覆盖的操作包括：

- `BenchmarkDBPut`：在 1024 个 key 的有界 key 空间中写入 128 字节 value。
- `BenchmarkDBPutOverwrite`：重复覆盖同一个 key。
- `BenchmarkDBGet`：从预填充的 1024 个 key 中循环读取。
- `BenchmarkDBWriteBatchCommit`：每次提交 100 条 WriteBatch 记录。
- `BenchmarkDBListKeys`：遍历索引并返回全部 key。
- `BenchmarkDBFold`：遍历全部 key 并读取对应 value。

运行快速基准：

```powershell
go test ./benchmark -bench . -benchmem -benchtime=100ms
```

一次 Windows amd64、AMD Ryzen 9 9950X3D 环境下的样例结果如下，仅用于本机回归参考：

| Benchmark | 耗时 | 内存分配 | 吞吐量 |
| --- | ---: | ---: | ---: |
| `BenchmarkDBPut` | `1.98 ms/op` | `487 B/op`，`3 allocs/op` | `0.06 MB/s` |
| `BenchmarkDBPutOverwrite` | `1.95 ms/op` | `216 B/op`，`3 allocs/op` | `0.07 MB/s` |
| `BenchmarkDBGet` | `6.23 us/op` | `384 B/op`，`6 allocs/op` | `20.56 MB/s` |
| `BenchmarkDBWriteBatchCommit` | `194.74 ms/op` | `51,408 B/op`，`650 allocs/op` | `0.07 MB/s` |
| `BenchmarkDBListKeys` | `8.54 us/op` | `36,840 B/op`，`5 allocs/op` | - |
| `BenchmarkDBFold` | `6.95 ms/op` | `370,635 B/op`，`5,124 allocs/op` | `19.60 MB/s` |

`MB/s` 由 benchmark 中的 `SetBytes` 根据逻辑 key/value 数据量计算，不是原始磁盘吞吐量。`ListKeys` 没有设置 `SetBytes`，因此不显示吞吐量。不同机器、磁盘和文件系统缓存下的结果不能直接比较，正式对比时建议使用更长的 `-benchtime`。

注意：当前 `DefaultOptions.BytesPerSync` 为 `0`，结合现有写入逻辑会让每次 `Put` 和 WriteBatch 记录都触发 `Sync`。因此上面的写入结果包含强制落盘成本，`WriteBatch` 的耗时尤其会受到同步次数影响。

## 测试与开发

环境要求：Go 1.25.0 或更高版本。

运行全部测试：

```powershell
go test ./... -count=1
```

运行 Redis 包测试：

```powershell
go test ./redis -count=1
```

运行 HASH 测试：

```powershell
go test ./redis -run '^TestRedisDataStructHash|^TestRedisMetadata' -count=1
```

运行 SET 测试：

```powershell
go test ./redis -run '^TestRedisDataStructS(Add|Rem)|^TestRedisDataStructSet(RejectsWrongType|ReopenPreservesMembers)' -count=1
```

运行 LIST 测试：

```powershell
go test ./redis -run '^TestRedisDataStructList|^TestRedisListMetadata' -count=1
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
go test ./utils -run '^TestBackup' -v
```

运行示例：

```powershell
go run .\examples\basic_operation.go
```

## 许可证

本项目使用 MIT License 发布。

详细内容请参阅 [LICENSE](LICENSE.md)。

Copyright 2025 Yifan Liu
