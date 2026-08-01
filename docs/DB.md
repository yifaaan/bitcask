# DB 模块设计文档

## 1. 模块概述

DB 模块是 Bitcask 的核心引擎，实现：
- 数据库打开/关闭
- KV 操作（Put/Get/Delete）
- 文件管理（rotation、active/older files）
- 索引重建（从 data files 或 hint file）
- 合并（Merge）
- 备份（Backup）
- 统计（Stat）

## 2. 依赖关系

```
Core (Error + Varint)
    ↑
FIO (IOManager)
    ↑
Data (LogRecord + DataFile)
    ↑
Index (Indexer)
    ↑
   DB (核心引擎)
    ↑
Batch (WriteBatch)
Iterator (用户迭代器)
```

---

## 3. Options.h 设计

```cpp
#pragma once

#include <string>
#include <cstdint>

namespace bitcask {

struct Options {
    std::string data_dir = "./bitcask_data";       // 数据目录
    uint64_t max_data_file_size = 10 * 1024 * 1024; // 单文件最大 10MB
    bool sync_on_write = false;                     // 每次写入后 sync
    uint64_t bytes_per_sync = 0;                    // 每写入 N bytes sync
    double auto_merge_reclaim_ratio = 0.0;          // 自动合并阈值
};

struct IteratorOptions {
    std::string prefix;  // 前缀过滤
    bool reverse = false; // 反向遍历
};

struct WriteBatchOptions {
    uint32_t max_batch_size = 1000; // 最大批次大小
    bool sync_on_commit = true;     // 提交时 sync
};

} // namespace bitcask
```

---

## 4. DB.h 设计

```cpp
#pragma once

#include "Options.h"
#include "Data/LogRecord.h"
#include "Index/Index.h"
#include <memory>
#include <shared_mutex>
#include <atomic>
#include <vector>
#include <string>
#include <functional>

namespace bitcask {

struct Stat {
    uint64_t key_num = 0;           // key 数量
    uint64_t data_file_num = 0;     // 数据文件数量
    uint64_t reclaimable_size = 0;  // 可回收空间
    uint64_t disk_size = 0;         // 磁盘占用
};

class DB {
public:
    ~DB();
    
    // 打开数据库
    static Result<std::unique_ptr<DB>> Open(const Options& options);
    
    // 关闭数据库
    VoidResult Close();
    
    // 写入
    VoidResult Put(const std::string& key, const std::string& value);
    
    // 读取
    Result<std::string> Get(const std::string& key);
    
    // 删除
    VoidResult Delete(const std::string& key);
    
    // 列出所有 key
    Result<std::vector<std::string>> ListKeys();
    
    // 遍历所有 KV，调用 callback
    // callback 返回 false 时停止
    VoidResult Fold(std::function<bool(const std::string&, const std::string&)> callback);
    
    // 统计信息
    Result<Stat> Stat();
    
    // 同步
    VoidResult Sync();
    
    // 合并
    VoidResult Merge();
    
    // 备份
    VoidResult Backup(const std::string& dest);
    
private:
    DB(const Options& options);
    
    // 内部方法
    VoidResult Initialize();
    VoidResult AcquireLock();
    VoidResult LoadDataFiles();
    VoidResult LoadIndexFromHintFile();
    VoidResult LoadIndexFromDataFiles();
    VoidResult LoadMergeFiles();
    VoidResult SetActiveDataFile();
    
    Result<int64_t> AppendLogRecord(const LogRecord& record);
    VoidResult UpdateIndex(const std::string& key, LogRecordType type, 
                           const LogRecordPos& pos);
    
    Result<std::string> GetValueByPosition(const LogRecordPos& pos);
    
    VoidResult MaybeAutoMerge();
    
    // 成员
    Options options_;
    mutable std::shared_mutex mutex_;
    std::atomic<uint64_t> txn_seq_;
    std::atomic<bool> is_merging_;
    
    std::unique_ptr<Indexer> index_;
    std::unique_ptr<DataFile> active_file_;
    std::map<uint32_t, std::unique_ptr<DataFile>> older_files_;
    std::vector<uint32_t> file_ids_;
    uint32_t active_fid_;
    
    int64_t reclaimable_size_;
    int64_t bytes_since_last_sync_;
    
    // 文件锁
    int lock_fd_;  // Unix: fd; Windows: HANDLE 另存
};

} // namespace bitcask
```

---

## 5. 核心流程实现

### 5.1 Open 流程

```cpp
Result<std::unique_ptr<DB>> DB::Open(const Options& options) {
    auto db = std::make_unique<DB>(options);
    
    // 1. 创建数据目录
    std::filesystem::create_directories(options.data_dir);
    
    // 2. 获取文件锁
    auto lock_result = db->AcquireLock();
    if (!lock_result) {
        return std::unexpected(lock_result.error());
    }
    
    // 3. 加载合并文件（如果有未完成的合并）
    auto merge_result = db->LoadMergeFiles();
    if (!merge_result) {
        return std::unexpected(merge_result.error());
    }
    
    // 4. 加载数据文件列表
    auto files_result = db->LoadDataFiles();
    if (!files_result) {
        return std::unexpected(files_result.error());
    }
    
    // 5. 从 hint 文件加载索引（优先）
    auto hint_result = db->LoadIndexFromHintFile();
    if (!hint_result) {
        // hint 文件损坏或不存在，从数据文件重建
        auto data_result = db->LoadIndexFromDataFiles();
        if (!data_result) {
            return std::unexpected(data_result.error());
        }
    }
    
    // 6. 设置活跃数据文件
    auto active_result = db->SetActiveDataFile();
    if (!active_result) {
        return std::unexpected(active_result.error());
    }
    
    return db;
}
```

### 5.2 Put 流程

```cpp
VoidResult DB::Put(const std::string& key, const std::string& value) {
    if (key.empty()) {
        return std::unexpected(ErrInvalidArgument("key cannot be empty"));
    }
    
    std::unique_lock lock(mutex_);
    
    // 1. 编码 LogRecord
    LogRecord record;
    record.key = key;
    record.value = value;
    record.type = LogRecordType::kNormal;
    
    // 2. 写入数据文件
    auto pos_result = AppendLogRecord(record);
    if (!pos_result) {
        return std::unexpected(pos_result.error());
    }
    
    // 3. 更新索引
    auto update_result = UpdateIndex(key, LogRecordType::kNormal, *pos_result);
    if (!update_result) {
        return std::unexpected(update_result.error());
    }
    
    // 4. 检查自动合并
    return MaybeAutoMerge();
}

Result<int64_t> DB::AppendLogRecord(const LogRecord& record) {
    // 编码
    auto [data, size] = EncodeLogRecord(record);
    
    // 检查文件大小，触发 rotation
    if (active_file_->write_offset + size > options_.max_data_file_size) {
        // Sync 当前文件
        active_file_->Sync();
        
        // 当前文件转为 older
        uint32_t old_fid = active_fid_;
        older_files_[old_fid] = std::move(active_file_);
        
        // 创建新 active 文件
        active_fid_ = old_fid + 1;
        auto new_file_result = DataFile::Open(options_.data_dir, active_fid_, IOType::Standard);
        if (!new_file_result) {
            return std::unexpected(new_file_result.error());
        }
        active_file_ = *new_file_result;
        file_ids_.push_back(active_fid_);
    }
    
    // 写入
    auto write_result = active_file_->Write(data);
    if (!write_result) {
        return std::unexpected(write_result.error());
    }
    
    // 可选 sync
    if (options_.sync_on_write) {
        active_file_->Sync();
    } else if (options_.bytes_per_sync > 0) {
        bytes_since_last_sync_ += size;
        if (bytes_since_last_sync_ >= options_.bytes_per_sync) {
            active_file_->Sync();
            bytes_since_last_sync_ = 0;
        }
    }
    
    return active_file_->write_offset - size;  // 返回起始偏移
}
```

### 5.3 Get 流程

```cpp
Result<std::string> DB::Get(const std::string& key) {
    if (key.empty()) {
        return std::unexpected(ErrInvalidArgument("key cannot be empty"));
    }
    
    std::shared_lock lock(mutex_);
    
    // 1. 从索引查找位置
    auto pos_result = index_->Get(key);
    if (!pos_result) {
        return std::unexpected(pos_result.error());
    }
    
    // 2. 根据位置读取值
    return GetValueByPosition(*pos_result);
}

Result<std::string> DB::GetValueByPosition(const LogRecordPos& pos) {
    // 选择数据文件
    std::unique_ptr<DataFile>* file;
    if (pos.fid == active_fid_) {
        file = &active_file_;
    } else {
        auto it = older_files_.find(pos.fid);
        if (it == older_files_.end()) {
            return std::unexpected(ErrCorruption("data file not found"));
        }
        file = &it->second;
    }
    
    // 读取 LogRecord
    auto [record_result, size, is_eof] = (*file)->ReadLogRecord(pos.offset);
    if (!record_result) {
        return std::unexpected(record_result.error());
    }
    
    const auto& record = *record_result;
    if (record.type == LogRecordType::kDeleted) {
        return std::unexpected(ErrNotFound(record.key));
    }
    
    return record.value;
}
```

### 5.4 Merge 流程

```cpp
VoidResult DB::Merge() {
    // 检查是否已在合并
    if (is_merging_.exchange(true)) {
        return std::unexpected(ErrMergeInProgress());
    }
    
    std::unique_ptr<DB> merge_db;
    
    // Phase 1: Setup (holds lock)
    {
        std::unique_lock lock(mutex_);
        
        // Sync active file
        active_file_->Sync();
        
        // 创建 merge 目录
        std::string merge_dir = GetMergePath();
        std::filesystem::create_directories(merge_dir);
        
        // 打开 merge 数据库
        Options merge_options = options_;
        merge_options.data_dir = merge_dir;
        merge_options.auto_merge_reclaim_ratio = 0;  // 禁止递归 merge
        
        auto merge_result = DB::Open(merge_options);
        if (!merge_result) {
            is_merging_ = false;
            return std::unexpected(merge_result.error());
        }
        merge_db = *merge_result;
        
        // 收集需要合并的文件列表（排除 active file）
        std::vector<uint32_t> merge_fids;
        for (uint32_t fid : file_ids_) {
            if (fid != active_fid_) {
                merge_fids.push_back(fid);
            }
        }
    }  // 释放锁
    
    // Phase 2: Rewrite (no lock, reads can continue)
    {
        std::shared_lock lock(mutex_);
        
        // 遍历所有 key，检查是否在 older files 中
        auto iter = index_->Iterator();
        iter->Rewind();
        
        while (iter->Valid()) {
            std::string key = iter->Key();
            LogRecordPos pos = iter->Value();
            
            // 只合并 older files 中的记录
            if (pos.fid != active_fid_) {
                // 读取值
                auto value_result = GetValueByPosition(pos);
                if (value_result) {
                    // 写入 merge db
                    merge_db->Put(key, *value_result);
                    
                    // 写入 hint 记录
                    // ...
                }
            }
            
            iter->Next();
        }
    }
    
    // Phase 3: Swap (holds lock)
    {
        std::unique_lock lock(mutex_);
        
        // 写入 merge-finished
        auto finish_result = WriteMergeFinished(merge_dir);
        if (!finish_result) {
            is_merging_ = false;
            return std::unexpected(finish_result.error());
        }
        
        // 关闭 merge db
        merge_db->Close();
        
        // 删除旧文件
        for (auto& [fid, file] : older_files_) {
            file->Close();
            std::filesystem::remove(DataFileName(fid));
        }
        older_files_.clear();
        
        // 移动 merge 文件
        // ...
        
        // 更新索引和文件列表
        // ...
    }
    
    is_merging_ = false;
    return {};
}
```

---

## 6. 文件锁实现

```cpp
// FileLock.cpp

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#endif

VoidResult DB::AcquireLock() {
    std::string lock_path = (std::filesystem::path(options_.data_dir) / "LOCK").string();
    
#ifdef _WIN32
    HANDLE hFile = CreateFileA(
        lock_path.c_str(),
        GENERIC_READ | GENERIC_WRITE,
        0,  // 不共享
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    
    if (hFile == INVALID_HANDLE_VALUE) {
        return std::unexpected(ErrLockFailed());
    }
    
    lock_handle_ = hFile;
#else
    int fd = open(lock_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return std::unexpected(ErrLockFailed());
    }
    
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        close(fd);
        return std::unexpected(ErrLockFailed());
    }
    
    lock_fd_ = fd;
#endif
    
    return {};
}

void DB::ReleaseLock() {
#ifdef _WIN32
    if (lock_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(lock_handle_);
        lock_handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (lock_fd_ >= 0) {
        flock(lock_fd_, LOCK_UN);
        close(lock_fd_);
        lock_fd_ = -1;
    }
#endif
    
    std::filesystem::remove(options_.data_dir + "/LOCK");
}
```

---

## 7. 索引重建流程

### 7.1 从 Hint 文件加载

```cpp
VoidResult DB::LoadIndexFromHintFile() {
    std::string hint_path = options_.data_dir + "/hint-index";
    if (!std::filesystem::exists(hint_path)) {
        return {};  // 无 hint 文件，正常
    }
    
    auto file_result = OpenHintFile(options_.data_dir, IOType::Standard);
    if (!file_result) {
        return std::unexpected(file_result.error());
    }
    
    auto& file = *file_result;
    int64_t offset = 0;
    
    while (true) {
        auto [record_result, size, is_eof] = file->ReadLogRecord(offset);
        if (is_eof) break;
        if (!record_result) {
            // hint 文件损坏，回退到数据文件重建
            return std::unexpected(record_result.error());
        }
        
        const auto& record = *record_result;
        
        // 解码 LogRecordPos
        auto [pos_opt, pos_len] = DecodeLogRecordPos(
            std::as_bytes(std::span(record.value)));
        
        if (!pos_opt) {
            return std::unexpected(ErrCorruption("invalid hint record"));
        }
        
        // 更新索引
        index_->Put(record.key, *pos_opt);
        
        offset += size;
    }
    
    return {};
}
```

### 7.2 从数据文件重建

```cpp
VoidResult DB::LoadIndexFromDataFiles() {
    // 按文件顺序（从小到大）处理
    // 后面文件覆盖前面文件的相同 key
    
    std::map<uint64_t, std::map<std::string, LogRecordPos>> txn_pending;
    
    for (uint32_t fid : file_ids_) {
        auto& file = (fid == active_fid_) ? active_file_ : older_files_[fid];
        
        int64_t offset = 0;
        
        while (true) {
            auto [record_result, size, is_eof] = file->ReadLogRecord(offset);
            if (is_eof) break;
            if (!record_result) {
                // 处理错误...
            }
            
            const auto& record = *record_result;
            
            if (record.type == LogRecordType::kTxnFinished) {
                // 事务完成，提交 pending writes
                uint64_t seq = DecodeSeqFromKey(record.key);
                for (auto& [k, pos] : txn_pending[seq]) {
                    index_->Put(k, pos);
                }
                txn_pending.erase(seq);
            } else {
                // 检查是否是事务 key
                uint64_t seq = 0;
                std::string real_key;
                if (ParseKeyWithSeq(record.key, &seq, &real_key)) {
                    // 事务记录，暂存
                    txn_pending[seq][real_key] = {fid, offset, size};
                } else {
                    // 非事务记录，直接更新索引
                    if (record.type == LogRecordType::kDeleted) {
                        index_->Delete(record.key);
                    } else {
                        index_->Put(record.key, {fid, offset, size});
                    }
                }
            }
            
            offset += size;
        }
    }
    
    // 丢弃未完成的事务
    // txn_pending 中剩余的都是未提交的事务
    
    return {};
}
```

---

## 8. 与原版本差异

| 原版本 | 新版本 |
|--------|--------|
| `absl::Status` | `VoidResult` |
| `absl::StatusOr<T>` | `Result<T>` |
| `absl::Mutex` | `std::shared_mutex` |
| `absl::btree_map` | `std::map` |
| `absl::ReaderMutexLock` | `std::shared_lock` |
| `absl::WriterMutexLock` | `std::unique_lock` |
| ART 索引选项 | 删除，仅 BTree |

---

## 9. 测试要点

```cpp
TEST(DB, OpenClose) {
    Options options;
    options.data_dir = "/tmp/test_db";
    
    auto db_result = DB::Open(options);
    ASSERT_TRUE(db_result.has_value());
    
    auto& db = *db_result;
    EXPECT_TRUE(db->Close().has_value());
}

TEST(DB, PutGet) {
    auto db = DB::Open(Options{.data_dir = "/tmp/test_put_get"});
    ASSERT_TRUE(db.has_value());
    
    EXPECT_TRUE((*db)->Put("key1", "value1").has_value());
    
    auto value = (*db)->Get("key1");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "value1");
    
    (*db)->Close();
}

TEST(DB, Delete) {
    auto db = DB::Open(Options{.data_dir = "/tmp/test_delete"});
    ASSERT_TRUE(db.has_value());
    
    (*db)->Put("key1", "value1");
    (*db)->Delete("key1");
    
    auto value = (*db)->Get("key1");
    EXPECT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code, ErrorCode::kNotFound);
    
    (*db)->Close();
}

TEST(DB, FileRotation) {
    Options options;
    options.data_dir = "/tmp/test_rotation";
    options.max_data_file_size = 100;  // 很小的阈值
    
    auto db = DB::Open(options);
    ASSERT_TRUE(db.has_value());
    
    // 写入足够多数据触发 rotation
    for (int i = 0; i < 100; ++i) {
        (*db)->Put(std::format("key{}", i), "long_value...");
    }
    
    // 验证多个数据文件存在
    // ...
    
    (*db)->Close();
}

TEST(DB, Merge) {
    auto db = DB::Open(Options{.data_dir = "/tmp/test_merge"});
    ASSERT_TRUE(db.has_value());
    
    // 写入大量数据
    for (int i = 0; i < 1000; ++i) {
        (*db)->Put(std::format("key{}", i), "value");
    }
    
    // 删除部分
    for (int i = 0; i < 500; ++i) {
        (*db)->Delete(std::format("key{}", i));
    }
    
    // 合并
    EXPECT_TRUE((*db)->Merge().has_value());
    
    // 验证剩余数据
    auto keys = (*db)->ListKeys();
    EXPECT_TRUE(keys.has_value());
    EXPECT_EQ(keys->size(), 500);
    
    (*db)->Close();
}

TEST(DB, RestartRecovery) {
    Options options;
    options.data_dir = "/tmp/test_recovery";
    
    // 第一次打开，写入数据
    auto db1 = DB::Open(options);
    (*db1)->Put("key1", "value1");
    (*db1)->Put("key2", "value2");
    (*db1)->Close();
    
    // 第二次打开，验证数据恢复
    auto db2 = DB::Open(options);
    EXPECT_EQ((*db2)->Get("key1").value(), "value1");
    EXPECT_EQ((*db2)->Get("key2").value(), "value2");
    (*db2)->Close();
}
```