# Bitcask 架构设计文档

## 1. 整体架构

Bitcask 是一个日志结构键值存储引擎，采用 append-only 写入、内存索引、定期合并的设计。

```
┌──────────────────────────────────────────────────────────────────┐
│                          Application Layer                       │
├──────────────────────────────────────────────────────────────────┤
│  HTTP API (httplib)  │  Redis Protocol (RESP + TCP Server)      │
├──────────────────────┴───────────────────────────────────────────┤
│                        DB Engine (Core)                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Index     │  │   Batch     │  │  Iterator   │              │
│  │ (in-memory) │  │ (tx support)│  │ (scan)      │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                      │
│  ┌──────┴────────────────┴────────────────┴──────┐              │
│  │              Data Layer (LogRecord + DataFile) │              │
│  └────────────────────────┬───────────────────────┘              │
│                           │                                      │
│  ┌────────────────────────┴───────────────────────┐              │
│  │              I/O Layer (FileIO + MmapIO)        │              │
│  └────────────────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    │  Disk Files │
                    │  .data      │
                    │  hint-index │
                    │  LOCK       │
                    └─────────────┘
```

## 2. 模块依赖关系

```
Main.cpp / RedisMain.cpp
    │
    ├── Http/Server ──────────────┐
    │       └── DB                │
    │                             │
    └── Redis/Server ─────────────┤
            ├── RESP              │
            ├── Redis/Command     │
            │       └── Redis/DataStruct
            │               └── DB
            └── TCP (jthread)     │
                                ┌─┴─┐
                                │DB │
                                └─┬─┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
         ┌────┴────┐        ┌─────┴─────┐       ┌─────┴─────┐
         │  Index  │        │   Batch   │       │ Iterator  │
         │(BTree)  │        │           │       │           │
         └────┬────┘        └─────┬─────┘       └─────┬─────┘
              │                   │                   │
              └───────────────────┼───────────────────┘
                                  │
                           ┌──────┴──────┐
                           │    Data     │
                           │ (LogRecord) │
                           │ (DataFile)  │
                           └──────┬──────┘
                                  │
                           ┌──────┴──────┐
                           │     FIO     │
                           │ (FileIO)    │
                           │ (MmapIO)    │
                           └──────┬──────┘
                                  │
                           ┌──────┴──────┐
                           │    Core     │
                           │ (Error)     │
                           │ (Varint)    │
                           └─────────────┘
```

## 3. 核心数据流

### 3.1 写路径 (Put)

```
Client Request (key, value)
        │
        ▼
┌───────────────────┐
│  DB::Put()        │
│  (write lock)     │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Encode LogRecord │ ─── [CRC][Type][KeySize][ValueSize][Key][Value]
│  (CRC32C + Varint)│
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Append to        │
│  Active DataFile  │ ─── write at offset, increment write_offset_
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Update Index     │ ─── index_[key] = {fid, offset, size}
│  (in-memory map)  │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Check AutoMerge  │ ─── if (reclaimable / total > threshold)
└───────────────────┘
```

### 3.2 读路径 (Get)

```
Client Request (key)
        │
        ▼
┌───────────────────┐
│  DB::Get()        │
│  (read lock)      │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Lookup Index     │ ─── pos = index_[key]
│                   │     returns {fid, offset, size}
└─────────┬─────────┘
          │
          ▼ (if found)
┌───────────────────┐
│  Select DataFile  │ ─── if fid == active_fid → active_file_
│                   │     else → older_files_[fid]
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Read LogRecord   │ ─── file.Read(offset, size)
│  at position      │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│  Decode & Verify  │ ─── CRC32C check
│  LogRecord        │
└─────────┬─────────┘
          │
          ▼
    Return value
```

### 3.3 合并路径 (Merge)

```
DB::Merge() (write lock for setup, then releases)
        │
        ▼
┌───────────────────────────────────────────────────┐
│  Phase 1: Setup (holds write lock)               │
│  - Sync active file                               │
│  - Create merge directory                         │
│  - Open new active file                           │
│  - Collect list of older files                    │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼ (release lock)
┌───────────────────────────────────────────────────┐
│  Phase 2: Rewrite (no lock, reads can continue)  │
│  - Iterate all keys in index                      │
│  - For each key, check if position is in older    │
│    files (not active file)                        │
│  - Append live records to merge files             │
│  - Write hint-index file for fast recovery        │
│  - Write merge-finished marker                    │
└─────────────────────┬─────────────────────────────┘
                      │
                      ▼ (acquire write lock)
┌───────────────────────────────────────────────────┐
│  Phase 3: Swap (holds write lock)                │
│  - Delete old data files                          │
│  - Move merge files to data directory             │
│  - Update older_files_ map                        │
│  - Delete merge directory                         │
└───────────────────────────────────────────────────┘
```

## 4. 并发模型

### 4.1 锁策略

| 操作 | 锁类型 | 说明 |
|------|--------|------|
| Get | shared_lock (读锁) | 允许多读并发 |
| Put | unique_lock (写锁) | 独占写入 |
| Delete | unique_lock (写锁) | 独占写入 |
| NewIterator | shared_lock (读锁) | 创建时快照 |
| Merge (Phase 1, 3) | unique_lock (写锁) | 仅 setup/swap 阶段 |
| Merge (Phase 2) | 无锁 | 重写阶段读操作可继续 |

```cpp
class DB {
    mutable std::shared_mutex mutex_;  // 读写锁
    std::atomic<uint64_t> txn_seq_;    // 事务序列号（原子操作）
    std::atomic<bool> is_merging_;     // 合并标志（原子操作）
};
```

### 4.2 Redis TCP 服务器并发

```
                    ┌─────────────────┐
                    │  Accept Thread  │
                    │  (std::jthread) │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ Connection 1 │  │ Connection 2 │  │ Connection N │
    │ (jthread)    │  │ (jthread)    │  │ (jthread)    │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           │                 │                 │
           └─────────────────┼─────────────────┘
                             │
                    ┌────────┴────────┐
                    │       DB        │
                    │ (shared_mutex)  │
                    └─────────────────┘
```

每个连接在独立线程中同步阻塞读写。Redis 协议简单，thread-per-connection 足够高效。

## 5. 错误处理模式

使用 **absl::Status 和 absl::StatusOr**（保留 abseil 依赖）：

```cpp
#include "absl/status/status.h"
#include "absl/status/statusor.h"

// 返回值或错误
absl::StatusOr<std::string> DB::Get(std::string_view key) {
    if (key.empty()) {
        return absl::InvalidArgumentError("key is empty");
    }
    // ...
    if (!found) {
        return absl::NotFoundError("key not found");
    }
    return value;
}

// 仅返回成功/失败
absl::Status DB::Put(std::string_view key, std::string_view value) {
    // ...
    return absl::OkStatus();
}

// 调用方检查
auto result = db->Get("mykey");
if (!result.ok()) {
    // 处理错误
    std::println(stderr, "Error: {}", result.status().ToString());
    return;
}
// 使用值
std::string value = *result;
```

**常用错误类型**:
- `absl::OkStatus()` — 成功
- `absl::NotFoundError(msg)` — 键不存在
- `absl::InvalidArgumentError(msg)` — 参数无效
- `absl::InternalError(msg)` — 内部错误
- `absl::ResourceExhaustedError(msg)` — 资源耗尽
- `absl::FailedPreconditionError(msg)` — 前置条件失败
- `absl::DataLossError(msg)` — 数据丢失/损坏

## 6. 文件布局

```
<data_dir>/
├── 000000001.data      # 数据文件（9位数字FID）
├── 000000002.data
├── 000000003.data
├── hint-index          # 合并后生成的索引文件
├── merge-finished      # 合并完成标记
└── LOCK                # 跨进程锁文件
```

- **数据文件**: `{FID:09d}.data`，FID 单调递增
- **hint-index**: 快速重建索引，避免全量扫描数据文件
- **merge-finished**: 标记合并完成，重启时执行 swap
- **LOCK**: 防止多进程同时打开同一数据库

## 7. 内存结构

### 7.1 索引结构

```cpp
// Index/BTreeIndex.h
class BTreeIndex : public Indexer {
    std::map<std::string, LogRecordPos> map_;
    mutable std::shared_mutex mutex_;
};

// LogRecordPos - 值在数据文件中的位置
struct LogRecordPos {
    uint32_t fid;      // 数据文件ID
    int64_t offset;    // 记录起始偏移
    int64_t size;      // 记录总大小
};
```

### 7.2 DB 主要成员

```cpp
class DB {
    // 同步
    mutable std::shared_mutex mutex_;
    std::atomic<uint64_t> txn_seq_;
    std::atomic<bool> is_merging_;

    // 索引
    std::unique_ptr<Indexer> index_;

    // 文件
    std::unique_ptr<DataFile> active_file_;           // 当前活跃文件
    std::map<uint32_t, std::unique_ptr<DataFile>> older_files_;  // 历史文件
    std::vector<uint32_t> file_ids_;                  // 所有FID列表

    // 统计
    int64_t reclaimable_size_;                        // 可回收空间

    // 配置
    Options options_;
};
```

## 8. 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 索引实现 | `std::map` | 简单、稳定、无外部依赖；ART 过于复杂且依赖外部库 |
| HTTP 服务器 | httplib (vcpkg) | 成熟稳定，避免自实现 HTTP 的复杂性 |
| Redis TCP | jthread + 阻塞 socket | 实现简单，RESP 协议无长连接复杂需求 |
| CRC32C | 硬件 intrinsic | 性能最优，x86/ARM 都有硬件支持 |
| Varint | 手写 LEB128 | 仅 30 行代码，避免 protobuf 依赖 |
| 错误处理 | `std::expected` | C++23 标准，类型安全，零开销 |

## 9. 性能考量

### 9.1 写入优化
- **Append-only**: 顺序写入，避免随机 IO
- **Buffered write**: 可配置 `bytes_per_sync` 批量刷盘
- **File rotation**: 避免单个文件过大

### 9.2 读取优化
- **内存索引**: O(log n) 查找
- **MmapIO**: 只读文件使用内存映射（可选）
- **Hint file**: 快速启动恢复

### 9.3 空间回收
- **Merge**: 周期性重写，删除过期数据
- **Auto-merge**: 可配置阈值自动触发