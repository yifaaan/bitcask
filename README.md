# Bitcask

Bitcask 是一个使用 Go 编写的嵌入式键值存储。项目采用追加写数据文件和
内存索引，索引记录每个 key 最新数据在磁盘中的位置。

## 当前进度

项目仍在持续开发中。当前 `main` 分支已经具备可工作的追加写存储流程、B-tree
和 ART 内存索引、数据库层测试以及批量原子写入（WriteBatch）支持。

| 状态 | 模块或功能 | 当前情况 |
| --- | --- | --- |
| [x] 已完成 | Go 模块和基础包结构 | 已建立项目模块及 `data`、`fio`、`index` 包。 |
| [x] 已完成 | 文件 IO | `fio.FileIO` 支持追加写、按偏移读取、同步、关闭和获取文件大小。 |
| [x] 已完成 | 数据文件 | `data.DataFile` 支持数据文件命名、写偏移维护和日志记录读取。 |
| [x] 已完成 | 日志记录 | 已实现记录编码、解码、varint 长度和 CRC32 校验。 |
| [x] 已完成 | B-tree 和 ART 索引 | 支持基于 `github.com/google/btree` 的 B-tree 和基于 `github.com/plar/go-adaptive-radix-tree/v2` 的 ART，均提供增删改查、快照迭代和 `Seek`。 |
| [x] 已完成 | 数据库启动和恢复 | 支持创建数据目录、扫描数据文件、选择 active 文件和重建内存索引。 |
| [x] 已完成 | 基础键值操作 | `DB.Put`、`DB.Get` 和 `DB.Delete` 已支持 key 校验、删除标记、文件轮换和最新值恢复。 |
| [x] 已完成 | 遍历和批量读取 | 已支持 `DB.NewIterator`、`DB.ListKeys` 和 `DB.Fold`，支持正逆序、前缀过滤和 `Seek`。 |
| [x] 已完成 | 单元测试 | 已覆盖文件 IO、日志记录、数据库操作、重启恢复、数据文件轮换、B-tree/ART 索引和遍历 API。 |
| [x] 已完成 | 基础示例 | `examples/` 下提供基础数据库操作示例。 |
| [x] 已完成 | WriteBatch 原子写 | `WriteBatch` 支持批量原子写入（Put/Delete），通过序列号保证事务性，恢复时正确重建索引。 |
| [x] 已完成 | 数据库生命周期 | `DB.Close` 已支持关闭 active 文件和旧数据文件。 |
| [ ] 进行中 | 可靠性和并发测试 | 崩溃模拟、损坏记录恢复和更完整的并发测试仍待补充。 |
| [ ] 未开始 | 命令行工具和服务端 | 当前尚未提供 CLI 或服务端程序。 |

## 后续计划

以下计划按照当前迭代顺序推进：

| 状态 | 阶段 | 计划 |
| --- | --- | --- |
| [x] | 1 | 迭代器支持 |
| [x] | 2 | WriteBatch 原子写 |
| [ ] | 3 | Merge 数据清理 |
| [ ] | 4 | 内存索引优化 |
| [ ] | 5 | 文件 IO 优化 |
| [ ] | 6 | 数据 Merge 优化 |
| [ ] | 7 | 数据备份 |
| [ ] | 8 | HTTP 接口 |
| [ ] | 9 | 基准测试 |
| [ ] | 10 | String 结构支持 |
| [ ] | 11 | Hash 结构支持 |
| [ ] | 12 | Set 结构支持 |
| [ ] | 13 | List 结构支持 |
| [ ] | 14 | SortedSet 结构支持 |
| [ ] | 15 | Redis 协议兼容 |

## 架构

当前写入链路如下：

```text
DB -> DataFile -> IOManager -> FileIO -> os.File
```

内存索引保存 key 对应的最新数据位置：

```text
key -> { file id, file offset }
```

普通读取和遍历路径如下：

```text
DB -> Iterator -> Index Iterator -> B-tree or ART
```

普通写入链路如下：

```text
DB.Put -> appendLogRecordWithLock -> DataFile -> IOManager -> FileIO -> os.File
```

`WriteBatch` 提交时沿用同样的写入链路，但其记录的 key 会附带一个全局递增的
事务序列号，并以一条 `LOG_RECORD_TXN_FINISH` 记录标识该事务结束。

日志记录的格式为编码后的 header 加 key 和 value：

```text
CRC | record type | key size | value size | key | value
```

其中 key size 和 value size 使用 varint 编码。删除操作会追加一条删除记录，
数据库恢复时根据这条记录从内存索引中移除对应 key。

`WriteBatch` 中的记录类型：

- `LOG_RECORD_NORMAL`            普通（写入）记录
- `LOG_RECORD_DELETED`           删除记录
- `LOG_RECORD_TXN_FINISH`        事务结束标记

启动恢复时，附有事务序列号的记录会被暂存，当读到对应序列号的 `LOG_RECORD_TXN_FINISH`
记录时整批更新到内存索引；若程序在事务中途崩溃，未结束的事务记录会被丢弃，
从而保证批量写入的原子性。

## 目录结构

```text
.
|-- db.go                   数据库打开、恢复、键值操作和遍历 API
|-- db_test.go              数据库集成测试
|-- iterator.go             面向用户的迭代器
|-- iterator_test.go        迭代器测试
|-- batch.go                批量原子写
|-- batch_test.go           WriteBatch 测试
|-- options.go              数据库、迭代器和 WriteBatch 配置
|-- errors.go               包级错误定义
|-- data/
|   |-- data_file.go        追加写数据文件
|   |-- data_file_test.go   数据文件测试
|   |-- log_record.go       日志记录类型和编解码
|   `-- log_record_test.go  日志记录测试
|-- fio/
|   |-- file_io.go          基于 os.File 的 IO 实现
|   |-- file_io_test.go     文件 IO 测试
|   `-- io_manager.go       IO 抽象接口
|-- index/
|   |-- index.go            索引接口和类型
|   |-- btree.go            B-tree 索引实现
|   |-- art.go              ART 索引实现
|   |-- btree_test.go       B-tree 测试
|   `-- art_test.go         ART 测试
`-- examples/
    `-- basic_operation.go  基础数据库操作示例
```

## 遍历 API

### ListKeys

`ListKeys` 返回当前索引中的所有 key。B-tree 和 ART 实现都按 key 的字典序返回，已删除的 key 不会出现在结果中。

```go
for _, key := range db.ListKeys() {
	fmt.Println(string(key))
}
```

### Fold

`Fold` 按正序遍历所有 key 和最新 value。回调返回 `false` 时停止遍历；读取数据失败时返回错误。

```go
err := db.Fold(func(key, value []byte) bool {
	fmt.Printf("%s = %s\n", key, value)
	return true
})
if err != nil {
	log.Fatal(err)
}
```

回调在数据库读锁期间执行，回调中不要调用 `Put`、`Delete` 或 `Close` 等会修改或关闭数据库的方法。

### Iterator

`NewIterator` 支持正序、逆序和前缀过滤。调用 `Rewind` 或 `Seek` 定位后，通过 `Valid`、`Key`、`Value` 和 `Next` 读取数据。

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

对于正序迭代，`Seek(key)` 定位到第一个大于等于 `key` 的位置；对于逆序迭代，定位到当前遍历方向下第一个不大于 `key` 的位置。

## 索引配置

默认使用 B-tree。可以通过 `Options.IndexType` 切换为 ART：

```go
package main

import (
	bitcask "github.com/yifaaan/bitcask"
	"github.com/yifaaan/bitcask/index"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "./data"
	opts.IndexType = index.ART

	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()
}
```

## 环境要求

- Go 1.24.5 或更高版本

## 运行测试

运行全部测试：

```powershell
go test ./...
```

运行某个数据库测试：

```powershell
go test . -run '^TestDBPutGetDelete$' -v
```

运行遍历相关测试：

```powershell
go test ./... -run 'Test(DBListKeys|DBFold|.*Iterator)' -v
```

## 运行示例

```powershell
go run .\examples\basic_operation.go
```

## 许可证

本项目使用 MIT License 发布。

详细内容请参阅 [LICENSE](LICENSE.md)。

Copyright 2025 Yifan Liu
