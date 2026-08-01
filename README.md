# Bitcask

Bitcask 是一个使用 Go 编写的嵌入式键值存储。项目采用追加写数据文件和
内存索引，索引记录每个 key 最新数据在磁盘中的位置。

## 当前进度

项目仍在持续开发中。当前 `main` 分支已经具备可工作的追加写存储流程、内存
索引和数据库层测试。

| 状态 | 模块或功能 | 当前情况 |
| --- | --- | --- |
| [x] 已完成 | Go 模块和基础包结构 | 已建立项目模块及 `data`、`fio`、`index` 包。 |
| [x] 已完成 | 文件 IO | `fio.FileIO` 支持追加写、按偏移读取、同步、关闭和获取文件大小。 |
| [x] 已完成 | 数据文件 | `data.DataFile` 支持数据文件命名、写偏移维护和日志记录读取。 |
| [x] 已完成 | 日志记录 | 已实现记录编码、解码、varint 长度和 CRC32 校验。 |
| [x] 已完成 | B-tree 索引 | 基于 `github.com/google/btree`，更新操作带锁。 |
| [x] 已完成 | 数据库启动和恢复 | 支持创建数据目录、扫描数据文件、选择 active 文件和重建内存索引。 |
| [x] 已完成 | 基础键值操作 | `DB.Put`、`DB.Get` 和 `DB.Delete` 已支持 key 校验、删除标记、文件轮换和最新值恢复。 |
| [x] 已完成 | 单元测试 | 已覆盖文件 IO、日志记录、数据库操作、重启恢复和数据文件轮换。 |
| [x] 已完成 | 基础示例 | `examples/` 下提供基础数据库操作示例。 |
| [ ] 进行中 | ART 索引 | 类型已经声明，但具体实现尚未完成。 |
| [ ] 进行中 | 数据库生命周期 | `DB` 尚未公开 `Close` 方法，目前测试直接关闭底层数据文件。 |
| [ ] 进行中 | 可靠性和并发测试 | 崩溃模拟、损坏记录恢复和更完整的并发测试仍待补充。 |
| [ ] 未开始 | 命令行工具和服务端 | 当前尚未提供 CLI 或服务端程序。 |

## 后续计划

以下计划按照当前迭代顺序推进：

| 状态 | 阶段 | 计划 |
| --- | --- | --- |
| [ ] | 1 | 迭代器支持 |
| [ ] | 2 | WriteBatch 原子写 |
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

日志记录的格式为编码后的 header 加 key 和 value：

```text
CRC | record type | key size | value size | key | value
```

其中 key size 和 value size 使用 varint 编码。删除操作会追加一条删除记录，
数据库恢复时根据这条记录从内存索引中移除对应 key。

## 目录结构

```text
.
|-- db.go                   数据库打开、恢复和键值操作
|-- db_test.go              数据库集成测试
|-- options.go              数据库配置
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
|   `-- btree_test.go       B-tree 测试
`-- examples/
    `-- basic_operation.go  基础数据库操作示例
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

## 运行示例

```powershell
go run .\examples\basic_operation.go
```

## 许可证

本项目使用 MIT License 发布。

详细内容请参阅 [LICENSE](LICENSE.md)。

Copyright 2025 Yifan Liu
