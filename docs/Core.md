# Core 模块设计文档

## 1. 模块概述

Core 模块提供基础工具，是所有其他模块的依赖。包含：
- **Varint.h**: LEB128 varint 编解码

**注意**: 不包含 Error.h，错误处理使用 **absl::Status 和 absl::StatusOr**。

## 2. 依赖关系

```
Core (Varint)
    ↑
    ├── Data (LogRecord 编解码使用 Varint)
    ├── DB (所有操作返回 absl::StatusOr)
    ├── Index (返回 absl::StatusOr)
    ├── RESP (解析返回 absl::StatusOr)
    └── Redis (命令处理返回 absl::StatusOr)
```

Core 模块**无外部依赖**，仅使用 C++23 标准库。

其他模块依赖 **abseil** 提供 Status/StatusOr。

---

## 3. Varint.h 设计

### 3.1 接口定义

```cpp
#pragma once

#include <span>
#include <cstdint>
#include <utility>
#include <vector>

namespace bitcask {

// 编码 uint64 为 varint，返回写入字节数
int PutVarint(std::span<std::byte> buf, uint64_t value);

// 解码 varint 为 uint64，返回 {值, 读取字节数}
// 如果失败，返回 {0, 0}
std::pair<uint64_t, int> GetVarint(std::span<const std::byte> buf);

// 编码 int64（负数转为 uint64 再编码）
int PutVarintSigned(std::span<std::byte> buf, int64_t value);

// 解码 int64
std::pair<int64_t, int> GetVarintSigned(std::span<const std::byte> buf);

// 辅助：计算 varint 编码后的字节数
inline int VarintLength(uint64_t value) {
    int len = 0;
    while (value >= 0x80) {
        value >>= 7;
        ++len;
    }
    return len + 1;
}

// 最大 varint 长度
constexpr int kMaxVarintLength = 10;  // 64-bit 最大 10 字节

// 编码到 vector（便捷函数）
inline std::vector<std::byte> EncodeVarint(uint64_t value) {
    std::vector<std::byte> buf(kMaxVarintLength);
    int len = PutVarint(buf, value);
    buf.resize(len);
    return buf;
}

} // namespace bitcask
```

### 3.2 实现要点

```cpp
// Varint.h (inline 实现)

int PutVarint(std::span<std::byte> buf, uint64_t value) {
    int n = 0;
    while (value >= 0x80) {
        buf[n++] = static_cast<std::byte>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    buf[n++] = static_cast<std::byte>(value);
    return n;
}

std::pair<uint64_t, int> GetVarint(std::span<const std::byte> buf) {
    if (buf.empty()) return {0, 0};
    
    uint64_t result = 0;
    int shift = 0;
    int n = 0;
    
    while (n < (int)buf.size() && shift < 64) {
        uint8_t byte = static_cast<uint8_t>(buf[n]);
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        ++n;
        if (!(byte & 0x80)) {
            return {result, n};  // 完成
        }
        shift += 7;
    }
    
    // 不完整或溢出
    return {0, 0};
}

// int64 使用 ZigZag 编码（可选，或直接 cast）
int PutVarintSigned(std::span<std::byte> buf, int64_t value) {
    // ZigZag: (n << 1) ^ (n >> 63)
    uint64_t zigzag = (static_cast<uint64_t>(value) << 1) 
                      ^ static_cast<uint64_t>(value >> 63);
    return PutVarint(buf, zigzag);
}

std::pair<int64_t, int> GetVarintSigned(std::span<const std::byte> buf) {
    auto [zigzag, len] = GetVarint(buf);
    if (len == 0) return {0, 0};
    int64_t value = static_cast<int64_t>((zigzag >> 1) ^ -(zigzag & 1));
    return {value, len};
}
```

### 3.3 与 Protobuf 的兼容性

原版本使用 `google::protobuf::io::CodedOutputStream::WriteVarint64ToArray`。

**对比**:
- Protobuf varint = LEB128 unsigned encoding
- 新版本手写 varint = 相同编码
- 对于 uint64，两者产生相同字节序列

**测试用例**:

```cpp
// 原版本 protobuf 编码
std::vector<uint8_t> proto_buf(10);
google::protobuf::io::CodedOutputStream::WriteVarint64ToArray(300, proto_buf.data());
// 结果: {0xAC, 0x02}

// 新版本手写编码
std::vector<std::byte> new_buf(10);
int len = PutVarint(new_buf, 300);
// 结果: {std::byte{0xAC}, std::byte{0x02}}
```

---

## 4. 与原版本差异

| 原版本 (C++20) | 新版本 (C++23) |
|----------------|----------------|
| `absl::StatusOr<T>` | **保留**（继续使用 abseil） |
| `absl::Status` | **保留** |
| `protobuf::CodedOutputStream` | 手写 `PutVarint` |
| `protobuf::CodedInputStream` | 手写 `GetVarint` |

---

## 5. 性能考量

### Varint 编码性能
- 内联函数，零函数调用开销
- 简单位操作，编译器高度优化
- 比 Protobuf 等效调用更快（无额外间接层）

### Error 性能
- `std::expected` 无额外内存开销（T 和 Error 共用空间）
- 返回值优化（RVO）避免拷贝
- 比 `absl::StatusOr` 等效或略优

---

## 6. 测试要点

```cpp
// Test/TestCore/TestVarint.cpp

TEST(Varint, EncodeDecode) {
    std::vector<uint64_t> values = {0, 1, 127, 128, 300, 
                                     UINT32_MAX, UINT64_MAX};
    for (uint64_t v : values) {
        std::vector<std::byte> buf(10);
        int len = PutVarint(buf, v);
        
        auto [decoded, dec_len] = GetVarint(buf);
        EXPECT_EQ(decoded, v);
        EXPECT_EQ(dec_len, len);
    }
}

TEST(Varint, CompatibilityWithProtobuf) {
    // 与原版本 protobuf 编码结果比较
    uint64_t value = 1234567890;
    
    // 原版本编码（如果可用）
    // ...
    
    // 新版本编码
    std::vector<std::byte> new_buf(10);
    int len = PutVarint(new_buf, value);
    
    // 字节序列应相同
    // ...
}

TEST(Error, ResultBasic) {
    Result<int> success = 42;
    EXPECT_TRUE(success.has_value());
    EXPECT_EQ(*success, 42);
    
    Result<int> failure = std::unexpected(ErrNotFound("key"));
    EXPECT_FALSE(failure.has_value());
    EXPECT_EQ(failure.error().code, ErrorCode::kNotFound);
}
```