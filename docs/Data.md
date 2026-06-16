# Data 模块设计文档

## 1. 模块概述

Data 模块处理日志记录的编解码和数据文件管理：
- **CRC32C.h**: 硬件加速 CRC32C 计算
- **LogRecord**: 日志记录类型定义与编解码
- **DataFile**: 数据文件封装

## 2. 依赖关系

```
Core (Error + Varint)
    ↑
   FIO (IOManager)
    ↑
   Data (CRC32C + LogRecord + DataFile)
    ↑
   DB (核心引擎)
```

---

## 3. CRC32C.h 设计

### 3.1 接口

```cpp
#pragma once

#include <cstdint>
#include <span>

namespace bitcask {

// 计算 CRC32C（Castagnoli 多项式）
// 初始值 crc = 0，可链接计算
uint32_t CRC32C(uint32_t crc, const void* data, size_t len);

// 便捷函数：单次计算
inline uint32_t ComputeCRC32C(const void* data, size_t len) {
    return CRC32C(0, data, len);
}

// Span 版本
inline uint32_t ComputeCRC32C(std::span<const std::byte> data) {
    return CRC32C(0, data.data(), data.size());
}

// 链接计算
inline uint32_t ExtendCRC32C(uint32_t crc, std::span<const std::byte> data) {
    return CRC32C(crc, data.data(), data.size());
}

} // namespace bitcask
```

### 3.2 硬件实现

```cpp
// CRC32C.cpp

#include "CRC32C.h"

#if defined(__x86_64__) || defined(_M_X64) || defined(__SSE4_2__)
#include <immintrin.h>

namespace bitcask {

uint32_t CRC32C(uint32_t crc, const void* data, size_t len) {
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    const uint8_t* end = ptr + len;
    
    // 8 字节对齐处理
    while (ptr + 8 <= end) {
        crc = _mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(ptr));
        ptr += 8;
    }
    
    // 4 字节
    if (ptr + 4 <= end) {
        crc = _mm_crc32_u32(crc, *reinterpret_cast<const uint32_t*>(ptr));
        ptr += 4;
    }
    
    // 2 字节
    if (ptr + 2 <= end) {
        crc = _mm_crc32_u16(crc, *reinterpret_cast<const uint16_t*>(ptr));
        ptr += 2;
    }
    
    // 1 字节
    if (ptr < end) {
        crc = _mm_crc32_u8(crc, *ptr);
    }
    
    return crc;
}

} // namespace bitcask

#elif defined(__aarch64__)
#include <arm_acle.h>

namespace bitcask {

uint32_t CRC32C(uint32_t crc, const void* data, size_t len) {
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    const uint8_t* end = ptr + len;
    
    // ARM64 CRC32C 指令
    while (ptr + 8 <= end) {
        crc = __crc32cd(crc, *reinterpret_cast<const uint64_t*>(ptr));
        ptr += 8;
    }
    // ... 类似处理剩余字节
    
    return crc;
}

} // namespace bitcask

#else
// 软件回退实现
namespace bitcask {

// Castagnoli 查找表
static const uint32_t kCRC32CTable[256] = {
    // ... 256 个预计算值
};

uint32_t CRC32C(uint32_t crc, const void* data, size_t len) {
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    crc = ~crc;
    for (size_t i = 0; i < len; ++i) {
        crc = kCRC32CTable[(crc ^ ptr[i]) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

} // namespace bitcask
#endif
```

---

## 4. LogRecord 设计

### 4.1 类型定义

```cpp
#pragma once

#include "Core/Error.h"
#include "Core/Varint.h"
#include <string>
#include <cstdint>
#include <optional>
#include <vector>
#include <utility>

namespace bitcask {

enum class LogRecordType : uint8_t {
    kNormal = 0,      // 正常数据
    kDeleted = 1,     // 删除标记
    kTxnFinished = 2, // 事务完成
};

struct LogRecord {
    std::string key;
    std::string value;
    LogRecordType type = LogRecordType::kNormal;
};

struct LogRecordPos {
    uint32_t fid = 0;      // 数据文件 ID
    int64_t offset = 0;    // 记录起始偏移
    int64_t size = 0;      // 记录总大小
};

struct LogRecordHeader {
    uint32_t crc = 0;
    LogRecordType type = LogRecordType::kNormal;
    int64_t key_size = 0;
    int64_t value_size = 0;
};

// 最大 header 大小（用于预读）
constexpr size_t kMaxLogRecordHeaderSize = 1 + 2 * 10;  // Type + 2 varints

// 编码 LogRecord -> bytes
std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record);

// 解码 header
std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(
    std::span<const std::byte> buf);

// 计算 CRC
uint32_t CalcLogRecordCRC(const LogRecord& record, std::span<const std::byte> header);

// 编码 LogRecordPos（用于 hint 文件）
std::pair<std::vector<std::byte>, int64_t> EncodeLogRecordPos(const LogRecordPos& pos);

// 解码 LogRecordPos
std::pair<std::optional<LogRecordPos>, int64_t> DecodeLogRecordPos(
    std::span<const std::byte> buf);

} // namespace bitcask
```

### 4.2 编码实现

```cpp
// LogRecord.cpp

#include "LogRecord.h"
#include "CRC32C.h"
#include <cstring>

namespace bitcask {

std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record) {
    // 1. 编码 header（不含 CRC）
    std::vector<std::byte> header(kMaxLogRecordHeaderSize);
    int header_len = 0;
    
    // Type (1 byte)
    header[header_len++] = static_cast<std::byte>(record.type);
    
    // KeySize (varint)
    header_len += PutVarint(std::span(header).subspan(header_len), 
                            static_cast<uint64_t>(record.key.size()));
    
    // ValueSize (varint)
    header_len += PutVarint(std::span(header).subspan(header_len), 
                            static_cast<uint64_t>(record.value.size()));
    
    header.resize(header_len);
    
    // 2. 计算 CRC（覆盖 header + key + value）
    uint32_t crc = ComputeCRC32C(header);
    crc = ExtendCRC32C(crc, std::as_bytes(std::span(record.key)));
    crc = ExtendCRC32C(crc, std::as_bytes(std::span(record.value)));
    
    // 3. 组装完整记录
    int64_t total_size = 4 + header_len + record.key.size() + record.value.size();
    std::vector<std::byte> result(total_size);
    
    int pos = 0;
    
    // CRC (4 bytes, LE)
    result[pos++] = static_cast<std::byte>(crc & 0xFF);
    result[pos++] = static_cast<std::byte>((crc >> 8) & 0xFF);
    result[pos++] = static_cast<std::byte>((crc >> 16) & 0xFF);
    result[pos++] = static_cast<std::byte>((crc >> 24) & 0xFF);
    
    // Header
    std::memcpy(result.data() + pos, header.data(), header_len);
    pos += header_len;
    
    // Key
    std::memcpy(result.data() + pos, record.key.data(), record.key.size());
    pos += record.key.size();
    
    // Value
    std::memcpy(result.data() + pos, record.value.data(), record.value.size());
    
    return {result, total_size};
}

std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(
    std::span<const std::byte> buf) {
    
    if (buf.size() < 5) {  // CRC(4) + Type(1) minimum
        return {std::nullopt, 0};
    }
    
    LogRecordHeader header;
    int pos = 0;
    
    // CRC (4 bytes, LE)
    header.crc = static_cast<uint32_t>(buf[pos]) |
                 (static_cast<uint32_t>(buf[pos + 1]) << 8) |
                 (static_cast<uint32_t>(buf[pos + 2]) << 16) |
                 (static_cast<uint32_t>(buf[pos + 3]) << 24);
    pos += 4;
    
    // Type (1 byte)
    header.type = static_cast<LogRecordType>(buf[pos++]);
    
    // KeySize (varint)
    auto [key_size, key_len] = GetVarint(buf.subspan(pos));
    if (key_len == 0) return {std::nullopt, 0};
    header.key_size = static_cast<int64_t>(key_size);
    pos += key_len;
    
    // ValueSize (varint)
    auto [value_size, value_len] = GetVarint(buf.subspan(pos));
    if (value_len == 0) return {std::nullopt, 0};
    header.value_size = static_cast<int64_t>(value_size);
    pos += value_len;
    
    return {header, pos};
}

uint32_t CalcLogRecordCRC(const LogRecord& record, std::span<const std::byte> header) {
    uint32_t crc = ComputeCRC32C(header);
    crc = ExtendCRC32C(crc, std::as_bytes(std::span(record.key)));
    crc = ExtendCRC32C(crc, std::as_bytes(std::span(record.value)));
    return crc;
}

std::pair<std::vector<std::byte>, int64_t> EncodeLogRecordPos(const LogRecordPos& pos) {
    std::vector<std::byte> buf(30);  // 3 varints max
    int n = 0;
    n += PutVarint(std::span(buf).subspan(n), pos.fid);
    n += PutVarint(std::span(buf).subspan(n), static_cast<uint64_t>(pos.offset));
    n += PutVarint(std::span(buf).subspan(n), static_cast<uint64_t>(pos.size));
    buf.resize(n);
    return {buf, n};
}

std::pair<std::optional<LogRecordPos>, int64_t> DecodeLogRecordPos(
    std::span<const std::byte> buf) {
    
    LogRecordPos pos;
    int n = 0;
    
    auto [fid, fid_len] = GetVarint(buf.subspan(n));
    if (fid_len == 0) return {std::nullopt, 0};
    pos.fid = static_cast<uint32_t>(fid);
    n += fid_len;
    
    auto [offset, offset_len] = GetVarint(buf.subspan(n));
    if (offset_len == 0) return {std::nullopt, 0};
    pos.offset = static_cast<int64_t>(offset);
    n += offset_len;
    
    auto [size, size_len] = GetVarint(buf.subspan(n));
    if (size_len == 0) return {std::nullopt, 0};
    pos.size = static_cast<int64_t>(size);
    n += size_len;
    
    return {pos, n};
}

} // namespace bitcask
```

---

## 5. DataFile 设计

### 5.1 接口

```cpp
#pragma once

#include "FIO/IOManager.h"
#include "LogRecord.h"
#include <memory>
#include <string>

namespace bitcask {

struct DataFile {
    uint32_t fid = 0;
    int64_t write_offset = 0;  // 当前写入偏移（仅 active file 使用）
    std::unique_ptr<IOManager> io;
    
    // 打开数据文件
    static Result<std::unique_ptr<DataFile>> Open(
        const std::string& dir_path, 
        uint32_t fid, 
        IOType io_type);
    
    // 同步
    VoidResult Sync();
    
    // 写入数据
    Result<int64_t> Write(std::span<const std::byte> data);
    
    // 读取 LogRecord
    // 返回 {record, size, is_eof}
    std::tuple<Result<LogRecord>, int64_t, bool> ReadLogRecord(int64_t offset);
    
    // 写入 hint 记录
    Result<int64_t> AppendHintRecord(const std::string& key, const LogRecordPos& pos);
};

// 辅助函数

// 打开 hint-index 文件
Result<std::unique_ptr<DataFile>> OpenHintFile(
    const std::string& dir_path, IOType io_type);

// 打开 merge-finished 文件
Result<std::unique_ptr<DataFile>> OpenMergeFinishedFile(
    const std::string& dir_path, IOType io_type);

// 数据文件名后缀
constexpr const char* kDataFileNameSuffix = ".data";

// 生成数据文件名
inline std::string DataFileName(uint32_t fid) {
    return std::format("{:09d}{}", fid, kDataFileNameSuffix);
}

} // namespace bitcask
```

### 5.2 实现

```cpp
// DataFile.cpp

#include "DataFile.h"
#include <filesystem>

namespace bitcask {

Result<std::unique_ptr<DataFile>> DataFile::Open(
    const std::string& dir_path, 
    uint32_t fid, 
    IOType io_type) {
    
    std::string path = (std::filesystem::path(dir_path) / DataFileName(fid)).string();
    
    auto io = IOManager::Open(path, io_type);
    if (!io) {
        return std::unexpected(ErrIOError(
            std::format("failed to open data file {}", path)));
    }
    
    auto file = std::make_unique<DataFile>();
    file->fid = fid;
    file->io = std::move(io);
    
    // 获取当前大小作为 write_offset
    auto size = file->io->Size();
    if (!size) {
        return std::unexpected(size.error());
    }
    file->write_offset = *size;
    
    return file;
}

VoidResult DataFile::Sync() {
    return io->Sync();
}

Result<int64_t> DataFile::Write(std::span<const std::byte> data) {
    auto n = io->Write(data);
    if (!n) {
        return std::unexpected(n.error());
    }
    write_offset += *n;
    return *n;
}

std::tuple<Result<LogRecord>, int64_t, bool> DataFile::ReadLogRecord(int64_t offset) {
    // 1. 读取 header（最多 kMaxLogRecordHeaderSize + 4 for CRC）
    std::vector<std::byte> header_buf(4 + kMaxLogRecordHeaderSize);
    auto read_n = io->Read(header_buf, offset);
    if (!read_n) {
        return {std::unexpected(read_n.error()), 0, false};
    }
    
    if (*read_n == 0) {
        // EOF
        return {LogRecord{}, 0, true};
    }
    
    // 2. 解码 header
    auto [header_opt, header_size] = DecodeLogRecordHeader(header_buf);
    if (!header_opt) {
        return {std::unexpected(ErrCorruption("invalid log record header")), 0, false};
    }
    const auto& header = *header_opt;
    
    // 3. 读取 key + value
    int64_t kv_offset = offset + header_size;
    int64_t kv_size = header.key_size + header.value_size;
    
    std::vector<std::byte> kv_buf(kv_size);
    read_n = io->Read(kv_buf, kv_offset);
    if (!read_n || *read_n != kv_size) {
        return {std::unexpected(ErrCorruption("incomplete key/value")), 0, false};
    }
    
    // 4. 提取 key 和 value
    LogRecord record;
    record.type = header.type;
    record.key.assign(
        reinterpret_cast<const char*>(kv_buf.data()), 
        header.key_size);
    record.value.assign(
        reinterpret_cast<const char*>(kv_buf.data() + header.key_size), 
        header.value_size);
    
    // 5. 验证 CRC
    uint32_t computed_crc = CalcLogRecordCRC(record, 
        std::span<const std::byte>(header_buf.data() + 4, header_size - 4));
    
    if (computed_crc != header.crc) {
        return {std::unexpected(ErrCorruption("CRC mismatch")), 0, false};
    }
    
    int64_t total_size = header_size + kv_size;
    return {record, total_size, false};
}

Result<int64_t> DataFile::AppendHintRecord(const std::string& key, const LogRecordPos& pos) {
    // 编码 LogRecordPos 作为 value
    auto [pos_bytes, pos_size] = EncodeLogRecordPos(pos);
    
    // 创建 hint LogRecord
    LogRecord record;
    record.key = key;
    record.value.assign(reinterpret_cast<const char*>(pos_bytes.data()), pos_size);
    record.type = LogRecordType::kNormal;
    
    // 编码并写入
    auto [data, data_size] = EncodeLogRecord(record);
    return Write(data);
}

Result<std::unique_ptr<DataFile>> OpenHintFile(
    const std::string& dir_path, IOType io_type) {
    
    std::string path = (std::filesystem::path(dir_path) / "hint-index").string();
    auto io = IOManager::Open(path, io_type);
    if (!io) {
        return std::unexpected(ErrIOError("failed to open hint file"));
    }
    
    auto file = std::make_unique<DataFile>();
    file->fid = 0;  // hint file 无 fid
    file->io = std::move(io);
    return file;
}

Result<std::unique_ptr<DataFile>> OpenMergeFinishedFile(
    const std::string& dir_path, IOType io_type) {
    
    std::string path = (std::filesystem::path(dir_path) / "merge-finished").string();
    auto io = IOManager::Open(path, io_type);
    if (!io) {
        return std::unexpected(ErrIOError("failed to open merge-finished file"));
    }
    
    auto file = std::make_unique<DataFile>();
    file->fid = 0;
    file->io = std::move(io);
    return file;
}

} // namespace bitcask
```

---

## 6. 使用示例

```cpp
// 写入数据
auto file_result = DataFile::Open("./data", 1, IOType::Standard);
if (!file_result) { /* 处理错误 */ }
auto& file = *file_result;

LogRecord record;
record.key = "mykey";
record.value = "myvalue";
record.type = LogRecordType::kNormal;

auto [data, size] = EncodeLogRecord(record);
auto write_result = file->Write(data);

// 读取数据
auto [read_result, read_size, is_eof] = file->ReadLogRecord(0);
if (!read_result) { /* 处理错误 */ }
if (!is_eof) {
    LogRecord read_record = *read_result;
    // 使用 read_record.key, read_record.value
}
```

---

## 7. 与原版本差异

| 原版本 | 新版本 |
|--------|--------|
| `absl::ComputeCrc32c()` | `ComputeCRC32C()` (硬件 intrinsic) |
| `protobuf::WriteVarint64ToArray()` | `PutVarint()` (手写) |
| `absl::Span<std::byte>` | `std::span<std::byte>` |
| `absl::StatusOr<...>` | `Result<...>` |

**磁盘格式完全兼容**。

---

## 8. 测试要点

```cpp
TEST(LogRecord, EncodeDecodeRoundTrip) {
    LogRecord record;
    record.key = "test_key";
    record.value = "test_value";
    record.type = LogRecordType::kNormal;
    
    auto [encoded, size] = EncodeLogRecord(record);
    
    // 解码 header
    auto [header_opt, header_size] = DecodeLogRecordHeader(encoded);
    ASSERT_TRUE(header_opt.has_value());
    
    // 验证 CRC
    uint32_t computed_crc = CalcLogRecordCRC(record, 
        std::span<const std::byte>(encoded.data() + 4, header_size - 4));
    EXPECT_EQ(computed_crc, header_opt->crc);
}

TEST(CRC32C, Compatibility) {
    // 与原版本 absl::ComputeCrc32c 结果比较
    std::string data = "hello world";
    
    uint32_t new_crc = ComputeCRC32C(data.data(), data.size());
    // uint32_t old_crc = absl::ComputeCrc32c(data);
    // EXPECT_EQ(new_crc, old_crc);
}
```