# RESP 模块设计文档

## 1. 模块概述

RESP 模块实现 Redis 序列化协议（RESP2）：
- **Value**: RESP 值类型
- **Serialize**: 值到 RESP 字节序列
- **StreamParser**: 字节流到 RESP 值（替换 hiredis）

## 2. 依赖关系

```
Core (Error)
    ↑
   RESP (Value + Serialize + StreamParser)
    ↑
   Redis (Command)
```

---

## 3. Value 类型设计

### 3.1 RESP.h

```cpp
#pragma once

#include "Core/Error.h"
#include <variant>
#include <string>
#include <vector>
#include <cstdint>

namespace bitcask::resp {

// RESP 值类型
struct NullTag {};

using SimpleString = std::string;
using Error = std::string;        // Error message
using Integer = int64_t;
using BulkString = std::string;   // empty means null bulk
using Array = std::vector<struct Value>;

struct Value {
    std::variant<
        NullTag,          // Null bulk string
        SimpleString,     // +OK\r\n
        Error,            // -ERR message\r\n
        Integer,          // :42\r\n
        BulkString,       // $5\r\nhello\r\n or $-1\r\n
        Array             // *2\r\n...\r\n
    > data;
    
    // 构造函数
    static Value Null() { return Value{NullTag{}}; }
    static Value Simple(std::string s) { return Value{SimpleString(std::move(s))}; }
    static Value Err(std::string e) { return Value{Error(std::move(e))}; }
    static Value Int(int64_t i) { return Value{Integer(i)}; }
    static Value Bulk(std::string b) { return Value{BulkString(std::move(b))}; }
    static Value Arr(Array a) { return Value{Array(std::move(a))}; }
    static Value EmptyArray() { return Value{Array{}}; }
    
    // 类型检查
    bool IsNull() const { return std::holds_alternative<NullTag>(data); }
    bool IsSimple() const { return std::holds_alternative<SimpleString>(data); }
    bool IsError() const { return std::holds_alternative<Error>(data); }
    bool IsInteger() const { return std::holds_alternative<Integer>(data); }
    bool IsBulk() const { return std::holds_alternative<BulkString>(data); }
    bool IsArray() const { return std::holds_alternative<Array>(data); }
    
    // 取值（需先检查类型）
    const std::string& AsSimple() const { return std::get<SimpleString>(data); }
    const std::string& AsError() const { return std::get<Error>(data); }
    int64_t AsInteger() const { return std::get<Integer>(data); }
    const std::string& AsBulk() const { return std::get<BulkString>(data); }
    const Array& AsArray() const { return std::get<Array>(data); }
};

// 序列化 Value -> RESP 字节序列
std::string Serialize(const Value& value);

// 流式解析器
class StreamParser {
public:
    StreamParser();
    
    // 追加输入数据
    void Append(std::string_view data);
    
    // 尝试解析下一个完整值
    // 返回 {value, 成功} 或 {nullopt, false}（数据不完整）
    std::pair<std::optional<Value>, bool> Next();
    
    // 清空缓冲
    void Clear();
    
    // 缓冲区大小
    size_t Buffered() const;
    
private:
    std::string buffer_;
    
    // 解析辅助函数
    std::pair<std::optional<Value>, size_t> ParseValue();
    std::pair<std::optional<std::string>, size_t> ParseLine();
    std::pair<std::optional<int64_t>, size_t> ParseInt();
    std::pair<std::optional<std::string>, size_t> ParseBulk();
};

} // namespace bitcask::resp
```

---

## 4. Serialize 实现

```cpp
// RESP.cpp

#include "RESP.h"
#include <sstream>

namespace bitcask::resp {

std::string Serialize(const Value& value) {
    std::ostringstream oss;
    
    if (value.IsNull()) {
        oss << "$-1\r\n";
    } else if (value.IsSimple()) {
        oss << "+" << value.AsSimple() << "\r\n";
    } else if (value.IsError()) {
        oss << "-" << value.AsError() << "\r\n";
    } else if (value.IsInteger()) {
        oss << ":" << value.AsInteger() << "\r\n";
    } else if (value.IsBulk()) {
        const auto& bulk = value.AsBulk();
        oss << "$" << bulk.size() << "\r\n" << bulk << "\r\n";
    } else if (value.IsArray()) {
        const auto& arr = value.AsArray();
        oss << "*" << arr.size() << "\r\n";
        for (const auto& item : arr) {
            oss << Serialize(item);
        }
    }
    
    return oss.str();
}

} // namespace bitcask::resp
```

---

## 5. StreamParser 实现

```cpp
// RESP.cpp (续)

StreamParser::StreamParser() {}

void StreamParser::Append(std::string_view data) {
    buffer_.append(data);
}

void StreamParser::Clear() {
    buffer_.clear();
}

size_t StreamParser::Buffered() const {
    return buffer_.size();
}

std::pair<std::optional<Value>, bool> StreamParser::Next() {
    auto [value, consumed] = ParseValue();
    
    if (consumed > 0) {
        buffer_.erase(0, consumed);
        return {value, true};
    }
    
    return {std::nullopt, false};  // 数据不完整
}

std::pair<std::optional<Value>, size_t> StreamParser::ParseValue() {
    if (buffer_.empty()) {
        return {std::nullopt, 0};
    }
    
    char type = buffer_[0];
    
    switch (type) {
        case '+': {  // Simple String
            auto [line, len] = ParseLine();
            if (len == 0) return {std::nullopt, 0};
            return {Value::Simple(*line), len};
        }
        case '-': {  // Error
            auto [line, len] = ParseLine();
            if (len == 0) return {std::nullopt, 0};
            return {Value::Err(*line), len};
        }
        case ':': {  // Integer
            auto [num, len] = ParseInt();
            if (len == 0) return {std::nullopt, 0};
            return {Value::Int(*num), len};
        }
        case '$': {  // Bulk String
            auto [bulk, len] = ParseBulk();
            if (len == 0) return {std::nullopt, 0};
            if (!bulk) {
                return {Value::Null(), len};
            }
            return {Value::Bulk(*bulk), len};
        }
        case '*': {  // Array
            // 解析数组长度
            auto [count, header_len] = ParseInt();
            if (header_len == 0) return {std::nullopt, 0};
            
            if (*count == -1) {
                // Null array
                return {Value::Null(), header_len};
            }
            
            if (*count < 0) {
                return {std::nullopt, 0};  // 无效
            }
            
            // 逐个解析元素
            size_t consumed = header_len;
            Array arr;
            arr.reserve(*count);
            
            for (int64_t i = 0; i < *count; ++i) {
                // 临时调整 buffer 以解析子元素
                std::string temp = buffer_.substr(consumed);
                StreamParser sub_parser;
                sub_parser.buffer_ = temp;
                
                auto [elem, elem_len] = sub_parser.ParseValue();
                if (elem_len == 0) {
                    return {std::nullopt, 0};  // 数据不完整
                }
                
                arr.push_back(*elem);
                consumed += elem_len;
            }
            
            return {Value::Arr(std::move(arr)), consumed};
        }
        default:
            // 未知类型，视为错误
            return {Value::Err("invalid RESP type"), 1};
    }
}

std::pair<std::optional<std::string>, size_t> StreamParser::ParseLine() {
    // 查找 \r\n
    size_t pos = buffer_.find("\r\n");
    if (pos == std::string::npos) {
        return {std::nullopt, 0};  // 不完整
    }
    
    // 提取行内容（跳过类型前缀）
    std::string line = buffer_.substr(1, pos - 1);
    size_t len = pos + 2;  // 包含 \r\n
    
    return {line, len};
}

std::pair<std::optional<int64_t>, size_t> StreamParser::ParseInt() {
    auto [line, len] = ParseLine();
    if (len == 0) return {std::nullopt, 0};
    
    try {
        int64_t num = std::stoll(*line);
        return {num, len};
    } catch (...) {
        return {std::nullopt, 0};  // 解析失败
    }
}

std::pair<std::optional<std::string>, size_t> StreamParser::ParseBulk() {
    // 解析长度
    auto [size, header_len] = ParseInt();
    if (header_len == 0) return {std::nullopt, 0};
    
    if (*size == -1) {
        // Null bulk
        return {std::nullopt, header_len};
    }
    
    if (*size < 0) {
        return {std::nullopt, 0};  // 无效
    }
    
    // 检查是否有足够数据
    if (buffer_.size() < header_len + *size + 2) {
        return {std::nullopt, 0};  // 数据不完整
    }
    
    // 提取 bulk 内容
    std::string bulk = buffer_.substr(header_len, *size);
    size_t total_len = header_len + *size + 2;  // +2 for trailing \r\n
    
    return {bulk, total_len};
}

} // namespace bitcask::resp
```

---

## 6. 使用示例

```cpp
// 序列化
auto value = Value::Arr({
    Value::Bulk("GET"),
    Value::Bulk("mykey")
});
std::string resp = Serialize(value);
// 结果: "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"

// 解析
StreamParser parser;
parser.Append("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");

auto [result, ok] = parser.Next();
if (ok && result->IsArray()) {
    auto& arr = result->AsArray();
    // arr[0] = "GET", arr[1] = "mykey"
}

// Redis 命令解析（简化）
Value ParseCommand(const std::string& input) {
    StreamParser parser;
    parser.Append(input);
    
    auto [value, ok] = parser.Next();
    if (!ok || !value->IsArray()) {
        return Value::Err("invalid command");
    }
    
    return *value;
}
```

---

## 7. 与原版本差异

| 原版本 | 新版本 |
|--------|--------|
| hiredis `redisReader` | 手写 `StreamParser` |
| hiredis `redisReply` | 自定义 `Value` 类型 |
| hiredis C API | 纯 C++23 实现 |

**功能等价**，无外部依赖。

---

## 8. 测试要点

```cpp
TEST(RESP, SerializeSimple) {
    auto v = Value::Simple("OK");
    EXPECT_EQ(Serialize(v), "+OK\r\n");
}

TEST(RESP, SerializeError) {
    auto v = Value::Err("ERR unknown command");
    EXPECT_EQ(Serialize(v), "-ERR unknown command\r\n");
}

TEST(RESP, SerializeInteger) {
    auto v = Value::Int(42);
    EXPECT_EQ(Serialize(v), ":42\r\n");
}

TEST(RESP, SerializeBulk) {
    auto v = Value::Bulk("hello");
    EXPECT_EQ(Serialize(v), "$5\r\nhello\r\n");
}

TEST(RESP, SerializeNullBulk) {
    auto v = Value::Null();
    EXPECT_EQ(Serialize(v), "$-1\r\n");
}

TEST(RESP, SerializeArray) {
    auto v = Value::Arr({Value::Bulk("GET"), Value::Bulk("key")});
    EXPECT_EQ(Serialize(v), "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
}

TEST(StreamParser, ParseSimple) {
    StreamParser p;
    p.Append("+OK\r\n");
    
    auto [v, ok] = p.Next();
    ASSERT_TRUE(ok);
    EXPECT_TRUE(v->IsSimple());
    EXPECT_EQ(v->AsSimple(), "OK");
}

TEST(StreamParser, ParseBulk) {
    StreamParser p;
    p.Append("$5\r\nhello\r\n");
    
    auto [v, ok] = p.Next();
    ASSERT_TRUE(ok);
    EXPECT_TRUE(v->IsBulk());
    EXPECT_EQ(v->AsBulk(), "hello");
}

TEST(StreamParser, ParseArray) {
    StreamParser p;
    p.Append("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    
    auto [v, ok] = p.Next();
    ASSERT_TRUE(ok);
    ASSERT_TRUE(v->IsArray());
    EXPECT_EQ(v->AsArray().size(), 2);
    EXPECT_EQ(v->AsArray()[0].AsBulk(), "GET");
    EXPECT_EQ(v->AsArray()[1].AsBulk(), "key");
}

TEST(StreamParser, IncompleteData) {
    StreamParser p;
    p.Append("$5\r\nhel");  // 不完整
    
    auto [v, ok] = p.Next();
    EXPECT_FALSE(ok);  // 数据不完整
    
    p.Append("lo\r\n");  // 补全
    
    auto [v2, ok2] = p.Next();
    ASSERT_TRUE(ok2);
    EXPECT_EQ(v2->AsBulk(), "hello");
}

TEST(StreamParser, Pipelining) {
    StreamParser p;
    p.Append("+OK\r\n+PONG\r\n");
    
    auto [v1, ok1] = p.Next();
    ASSERT_TRUE(ok1);
    EXPECT_EQ(v1->AsSimple(), "OK");
    
    auto [v2, ok2] = p.Next();
    ASSERT_TRUE(ok2);
    EXPECT_EQ(v2->AsSimple(), "PONG");
}
```