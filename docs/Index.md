# Index 模块设计文档

## 1. 模块概述

Index 模块提供内存索引的抽象接口和实现：
- **Indexer**: 索引接口
- **IndexIterator**: 迭代器接口
- **BTreeIndex**: 基于 `std::map` 的实现

原版本有 ART 索引（基于 libart），新版本删除，仅保留 BTree。

## 2. 依赖关系

```
Core (Error)
    ↑
Data (LogRecordPos)
    ↑
   Index (Indexer + BTreeIndex)
    ↑
   DB (核心引擎使用 Indexer)
```

---

## 3. 接口设计

### 3.1 Index.h

```cpp
#pragma once

#include "Data/LogRecord.h"
#include "Core/Error.h"
#include <string>
#include <memory>
#include <vector>

namespace bitcask {

// 索引迭代器接口
class IndexIterator {
public:
    virtual ~IndexIterator() = default;
    
    // 重置到起始位置
    virtual void Rewind() = 0;
    
    // 定位到 >= key 的位置
    virtual void Seek(const std::string& key) = 0;
    
    // 移动到下一个
    virtual void Next() = 0;
    
    // 是否有效
    virtual bool Valid() = 0;
    
    // 当前 key
    virtual std::string Key() = 0;
    
    // 当前 value (LogRecordPos)
    virtual LogRecordPos Value() = 0;
};

// 索引接口
class Indexer {
public:
    virtual ~Indexer() = default;
    
    // 插入或更新 key -> pos
    virtual VoidResult Put(const std::string& key, const LogRecordPos& pos) = 0;
    
    // 获取 key 的位置
    virtual Result<LogRecordPos> Get(const std::string& key) = 0;
    
    // 删除 key
    virtual VoidResult Delete(const std::string& key) = 0;
    
    // 创建迭代器
    // reverse = true 时反向遍历
    virtual std::unique_ptr<IndexIterator> Iterator(bool reverse = false) = 0;
    
    // 索引大小（key 数量）
    virtual size_t Size() = 0;
};

// 工厂函数（仅 BTree）
std::unique_ptr<Indexer> CreateBTreeIndex();

} // namespace bitcask
```

### 3.2 BTreeIndex.h

```cpp
#pragma once

#include "Index.h"
#include <map>
#include <shared_mutex>
#include <vector>
#include <algorithm>

namespace bitcask {

class BTreeIndex : public Indexer {
public:
    BTreeIndex();
    ~BTreeIndex() override;
    
    VoidResult Put(const std::string& key, const LogRecordPos& pos) override;
    Result<LogRecordPos> Get(const std::string& key) override;
    VoidResult Delete(const std::string& key) override;
    std::unique_ptr<IndexIterator> Iterator(bool reverse = false) override;
    size_t Size() override;
    
private:
    std::map<std::string, LogRecordPos> map_;
    mutable std::shared_mutex mutex_;
};

// BTree 迭代器（快照式）
class BTreeIndexIterator : public IndexIterator {
public:
    BTreeIndexIterator(const std::map<std::string, LogRecordPos>& snapshot, bool reverse);
    
    void Rewind() override;
    void Seek(const std::string& key) override;
    void Next() override;
    bool Valid() override;
    std::string Key() override;
    LogRecordPos Value() override;
    
private:
    std::vector<std::pair<std::string, LogRecordPos>> entries_;
    size_t current_;
    bool reverse_;
};

} // namespace bitcask
```

---

## 4. 实现

### 4.1 BTreeIndex.cpp

```cpp
#include "BTreeIndex.h"

namespace bitcask {

BTreeIndex::BTreeIndex() = default;
BTreeIndex::~BTreeIndex() = default;

VoidResult BTreeIndex::Put(const std::string& key, const LogRecordPos& pos) {
    std::unique_lock lock(mutex_);
    map_[key] = pos;
    return {};
}

Result<LogRecordPos> BTreeIndex::Get(const std::string& key) {
    std::shared_lock lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
        return std::unexpected(ErrNotFound(key));
    }
    return it->second;
}

VoidResult BTreeIndex::Delete(const std::string& key) {
    std::unique_lock lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
        map_.erase(it);
    }
    return {};
}

std::unique_ptr<IndexIterator> BTreeIndex::Iterator(bool reverse) {
    std::shared_lock lock(mutex_);
    return std::make_unique<BTreeIndexIterator>(map_, reverse);
}

size_t BTreeIndex::Size() {
    std::shared_lock lock(mutex_);
    return map_.size();
}

// === BTreeIndexIterator ===

BTreeIndexIterator::BTreeIndexIterator(
    const std::map<std::string, LogRecordPos>& snapshot, 
    bool reverse)
    : reverse_(reverse), current_(0) {
    
    // 复制快照到 vector
    entries_.reserve(snapshot.size());
    for (const auto& [k, v] : snapshot) {
        entries_.emplace_back(k, v);
    }
    
    // 如果反向，反转 vector
    if (reverse_) {
        std::reverse(entries_.begin(), entries_.end());
    }
    
    Rewind();
}

void BTreeIndexIterator::Rewind() {
    current_ = 0;
}

void BTreeIndexIterator::Seek(const std::string& key) {
    if (reverse_) {
        // 反向：找到最后一个 <= key 的位置
        auto it = std::upper_bound(entries_.rbegin(), entries_.rend(),
            std::make_pair(key, LogRecordPos{}),
            [](const auto& a, const auto& b) {
                return a.first > b.first;
            });
        current_ = entries_.rend() - it;
    } else {
        // 正向：找到第一个 >= key 的位置
        auto it = std::lower_bound(entries_.begin(), entries_.end(),
            std::make_pair(key, LogRecordPos{}),
            [](const auto& a, const auto& b) {
                return a.first < b.first;
            });
        current_ = it - entries_.begin();
    }
}

void BTreeIndexIterator::Next() {
    if (Valid()) {
        ++current_;
    }
}

bool BTreeIndexIterator::Valid() {
    return current_ < entries_.size();
}

std::string BTreeIndexIterator::Key() {
    if (!Valid()) {
        return "";
    }
    return entries_[current_].first;
}

LogRecordPos BTreeIndexIterator::Value() {
    if (!Valid()) {
        return LogRecordPos{};
    }
    return entries_[current_].second;
}

// === 工厂函数 ===

std::unique_ptr<Indexer> CreateBTreeIndex() {
    return std::make_unique<BTreeIndex>();
}

} // namespace bitcask
```

---

## 5. 迭代器设计说明

### 5.1 快照式迭代器

创建迭代器时复制索引快照到 vector：
- **优点**: 遍历期间不受其他线程修改影响，无需长时间持有锁
- **缺点**: 内存开销（复制整个索引）

适用于：
- 索引大小可控（通常数百万键）
- 遍历时间长（Merge、Backup 等操作）
- 不影响其他读写操作

### 5.2 正向 vs 反向

- **正向**: 按 key 字典序从小到大遍历
- **反向**: 按字典序从大到小遍历

Seek 行为：
- 正向 Seek(key): 移动到第一个 >= key 的位置
- 反向 Seek(key): 移动到最后一个 <= key 的位置

---

## 6. 与原版本差异

| 原版本 | 新版本 |
|--------|--------|
| `absl::btree_map` | `std::map` |
| `absl::Mutex` | `std::shared_mutex` |
| `absl::ReaderMutexLock` | `std::shared_lock` |
| `absl::WriterMutexLock` | `std::unique_lock` |
| ART 索引 (libart) | **删除** |

**std::map vs absl::btree_map**:
- `std::map`: 红黑树，节点级分配，稳定迭代器
- `absl::btree_map`: B-tree，连续内存块，更紧凑内存布局
- 性能差异：对于百万级键，差异约 10-20%

新版本选择 `std::map`：
- 无外部依赖
- 足够的性能
- 简单实现

---

## 7. 测试要点

```cpp
TEST(BTreeIndex, PutGetDelete) {
    auto index = CreateBTreeIndex();
    
    LogRecordPos pos1 = {1, 0, 100};
    EXPECT_TRUE(index->Put("key1", pos1).has_value());
    
    auto result = index->Get("key1");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->fid, 1);
    
    EXPECT_TRUE(index->Delete("key1").has_value());
    EXPECT_FALSE(index->Get("key1").has_value());
}

TEST(BTreeIndexIterator, Forward) {
    auto index = CreateBTreeIndex();
    index->Put("a", {1, 0, 10});
    index->Put("b", {1, 10, 20});
    index->Put("c", {1, 30, 30});
    
    auto iter = index->Iterator(false);
    iter->Rewind();
    
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ(iter->Key(), "a");
    iter->Next();
    EXPECT_EQ(iter->Key(), "b");
    iter->Next();
    EXPECT_EQ(iter->Key(), "c");
    iter->Next();
    EXPECT_FALSE(iter->Valid());
}

TEST(BTreeIndexIterator, Reverse) {
    auto index = CreateBTreeIndex();
    index->Put("a", {1, 0, 10});
    index->Put("b", {1, 10, 20});
    index->Put("c", {1, 30, 30});
    
    auto iter = index->Iterator(true);
    iter->Rewind();
    
    EXPECT_EQ(iter->Key(), "c");
    iter->Next();
    EXPECT_EQ(iter->Key(), "b");
    iter->Next();
    EXPECT_EQ(iter->Key(), "a");
}

TEST(BTreeIndexIterator, Seek) {
    auto index = CreateBTreeIndex();
    index->Put("apple", {1, 0, 10});
    index->Put("banana", {1, 10, 20});
    index->Put("cherry", {1, 30, 30});
    
    auto iter = index->Iterator(false);
    iter->Seek("blueberry");  // > banana, < cherry
    
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ(iter->Key(), "cherry");
}
```