# Redis 模块设计文档

## 1. 模块概述

Redis 模块实现 Redis 兼容服务器：
- **DataKey.h**: Redis 子键编码
- **DataStruct.h/cpp**: Redis 数据类型（String/Hash/List/Set/ZSet）
- **Command.h/cpp**: Redis 命令处理
- **Server.h/cpp**: TCP 服务器

## 2. 依赖关系

```
Core (Error)
    ↑
RESP (Value + Parser)
    ↑
DB (核心引擎)
    ↑
Redis/DataStruct → Redis/Command → Redis/Server
```

---

## 3. DataKey.h - 子键编码

### 3.1 接口

```cpp
#pragma once

#include <string>
#include <cstdint>
#include <span>

namespace bitcask {

// Redis 数据类型
enum class RedisDataType : uint8_t {
    kString = 0,
    kHash = 1,
    kList = 2,
    kSet = 3,
    kZSet = 4,
};

// Value 元数据
struct ValueMetadata {
    RedisDataType type;
    int64_t expiry = 0;     // 过期时间（steady_clock ns），0=永不过期
    uint64_t version = 0;   // 集合版本号
    uint64_t size = 0;      // 元素数
    int64_t head = 0;       // List 头索引
    int64_t tail = 0;       // List 尾索引
};

// 编码/解码元数据
std::string EncodeMetadata(const ValueMetadata& meta);
ValueMetadata DecodeMetadata(std::span<const std::byte> data);

// 子键编码
std::string EncodeHashKey(const std::string& key, uint64_t version, const std::string& field);
std::string EncodeSetKey(const std::string& key, uint64_t version, const std::string& member);
std::string EncodeListKey(const std::string& key, uint64_t version, int64_t index);
std::string EncodeZSetMemberKey(const std::string& key, uint64_t version, const std::string& member);
std::string EncodeZSetScoreKey(const std::string& key, uint64_t version, double score, const std::string& member);

// Double 排序编码
uint64_t EncodeScore(double score);
double DecodeScore(uint64_t bits);

} // namespace bitcask
```

### 3.2 实现

```cpp
#include "DataKey.h"
#include <cstring>
#include <bit>
#include <algorithm>

namespace bitcask {

std::string EncodeMetadata(const ValueMetadata& meta) {
    std::string result;
    result.reserve(1 + 8 + 8 + 8 + 8 + 8);
    
    // Type (1 byte)
    result.push_back(static_cast<char>(meta.type));
    
    // Expiry (8 bytes LE)
    uint64_t expiry = static_cast<uint64_t>(meta.expiry);
    result.append(reinterpret_cast<const char*>(&expiry), 8);
    
    // Version (8 bytes LE)
    result.append(reinterpret_cast<const char*>(&meta.version), 8);
    
    // Size (8 bytes LE)
    result.append(reinterpret_cast<const char*>(&meta.size), 8);
    
    // Head + Tail (List only)
    if (meta.type == RedisDataType::kList) {
        uint64_t head = static_cast<uint64_t>(meta.head);
        uint64_t tail = static_cast<uint64_t>(meta.tail);
        result.append(reinterpret_cast<const char*>(&head), 8);
        result.append(reinterpret_cast<const char*>(&tail), 8);
    }
    
    return result;
}

ValueMetadata DecodeMetadata(std::span<const std::byte> data) {
    ValueMetadata meta;
    if (data.size() < 1 + 8 + 8 + 8) return meta;
    
    const char* ptr = reinterpret_cast<const char*>(data.data());
    
    meta.type = static_cast<RedisDataType>(ptr[0]);
    ptr += 1;
    
    std::memcpy(&meta.expiry, ptr, 8); ptr += 8;
    std::memcpy(&meta.version, ptr, 8); ptr += 8;
    std::memcpy(&meta.size, ptr, 8); ptr += 8;
    
    if (meta.type == RedisDataType::kList && data.size() >= 1 + 8 + 8 + 8 + 8 + 8) {
        std::memcpy(&meta.head, ptr, 8); ptr += 8;
        std::memcpy(&meta.tail, ptr, 8);
    }
    
    return meta;
}

std::string EncodeHashKey(const std::string& key, uint64_t version, const std::string& field) {
    // key || version(LE) || field
    std::string result;
    result.reserve(key.size() + 8 + field.size());
    result.append(key);
    result.append(reinterpret_cast<const char*>(&version), 8);
    result.append(field);
    return result;
}

std::string EncodeSetKey(const std::string& key, uint64_t version, const std::string& member) {
    // key || version(LE) || member || member_size(LE)
    std::string result;
    result.reserve(key.size() + 8 + member.size() + 4);
    result.append(key);
    result.append(reinterpret_cast<const char*>(&version), 8);
    result.append(member);
    uint32_t size = static_cast<uint32_t>(member.size());
    result.append(reinterpret_cast<const char*>(&size), 4);
    return result;
}

std::string EncodeListKey(const std::string& key, uint64_t version, int64_t index) {
    // key || version(LE) || index(LE)
    std::string result;
    result.reserve(key.size() + 8 + 8);
    result.append(key);
    result.append(reinterpret_cast<const char*>(&version), 8);
    uint64_t idx = static_cast<uint64_t>(index);
    result.append(reinterpret_cast<const char*>(&idx), 8);
    return result;
}

uint64_t EncodeScore(double score) {
    // IEEE 754 + sign bit flip for ordering
    uint64_t bits = std::bit_cast<uint64_t>(score);
    if (bits & 0x8000000000000000) {
        // 负数：取反
        return ~bits;
    } else {
        // 正数：翻转符号位
        return bits ^ 0x8000000000000000;
    }
}

double DecodeScore(uint64_t bits) {
    uint64_t original;
    if (bits & 0x8000000000000000) {
        // 原来是正数
        original = bits ^ 0x8000000000000000;
    } else {
        // 原来是负数
        original = ~bits;
    }
    return std::bit_cast<double>(original);
}

std::string EncodeZSetScoreKey(const std::string& key, uint64_t version, 
                                double score, const std::string& member) {
    // key || version(LE) || score(BE, encoded) || member || member_size(LE)
    std::string result;
    result.reserve(key.size() + 8 + 8 + member.size() + 4);
    result.append(key);
    result.append(reinterpret_cast<const char*>(&version), 8);
    
    // Score 大端编码（有序）
    uint64_t encoded_score = EncodeScore(score);
    for (int i = 7; i >= 0; --i) {
        result.push_back(static_cast<char>((encoded_score >> (i * 8)) & 0xFF));
    }
    
    result.append(member);
    uint32_t size = static_cast<uint32_t>(member.size());
    result.append(reinterpret_cast<const char*>(&size), 4);
    return result;
}

} // namespace bitcask
```

---

## 4. DataStruct - Redis 数据类型

### 4.1 接口

```cpp
#pragma once

#include "DataKey.h"
#include "DB/DB.h"
#include <string>
#include <vector>
#include <optional>

namespace bitcask {

class RedisDataStruct {
public:
    explicit RedisDataStruct(DB* db);
    
    // String
    VoidResult Set(const std::string& key, const std::string& value, 
                   std::optional<int64_t> ttl_ms = std::nullopt);
    Result<std::string> Get(const std::string& key);
    VoidResult Del(const std::string& key);
    Result<RedisDataType> Type(const std::string& key);
    
    // Hash
    VoidResult HSet(const std::string& key, const std::string& field, 
                    const std::string& value, std::optional<int64_t> ttl_ms = std::nullopt);
    Result<std::string> HGet(const std::string& key, const std::string& field);
    VoidResult HDel(const std::string& key, const std::string& field);
    
    // List
    VoidResult LPush(const std::string& key, const std::string& value);
    VoidResult RPush(const std::string& key, const std::string& value);
    Result<std::string> LPop(const std::string& key);
    Result<std::string> RPop(const std::string& key);
    Result<int64_t> LLen(const std::string& key);
    
    // Set
    VoidResult SAdd(const std::string& key, const std::string& member,
                    std::optional<int64_t> ttl_ms = std::nullopt);
    Result<int64_t> SIsMember(const std::string& key, const std::string& member);
    VoidResult SRem(const std::string& key, const std::string& member);
    
    // Sorted Set
    VoidResult ZAdd(const std::string& key, const std::string& member, double score,
                    std::optional<int64_t> ttl_ms = std::nullopt);
    Result<double> ZScore(const std::string& key, const std::string& member);
    VoidResult ZRem(const std::string& key, const std::string& member);
    
private:
    DB* db_;
    std::atomic<uint64_t> version_counter_;
    
    uint64_t NewVersion();
    bool IsExpired(const ValueMetadata& meta);
    VoidResult CheckType(const std::string& key, RedisDataType expected);
};

} // namespace bitcask
```

### 4.2 实现（部分）

```cpp
#include "DataStruct.h"
#include "DB/Batch.h"
#include <chrono>

namespace bitcask {

RedisDataStruct::RedisDataStruct(DB* db) : db_(db), version_counter_(0) {}

uint64_t RedisDataStruct::NewVersion() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return now + version_counter_.fetch_add(1);
}

bool RedisDataStruct::IsExpired(const ValueMetadata& meta) {
    if (meta.expiry == 0) return false;
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return now >= meta.expiry;
}

// === String ===

VoidResult RedisDataStruct::Set(const std::string& key, const std::string& value,
                                 std::optional<int64_t> ttl_ms) {
    ValueMetadata meta;
    meta.type = RedisDataType::kString;
    if (ttl_ms) {
        meta.expiry = std::chrono::steady_clock::now().time_since_epoch().count() 
                      + *ttl_ms * 1'000'000;
    }
    
    std::string encoded = EncodeMetadata(meta) + value;
    return db_->Put(key, encoded);
}

Result<std::string> RedisDataStruct::Get(const std::string& key) {
    auto value = db_->Get(key);
    if (!value) {
        return std::unexpected(value.error());
    }
    
    // 解码元数据
    auto meta = DecodeMetadata(std::as_bytes(std::span(*value)));
    
    // 检查类型
    if (meta.type != RedisDataType::kString) {
        return std::unexpected(ErrFailedPrecondition("WRONGTYPE"));
    }
    
    // 检查过期
    if (IsExpired(meta)) {
        db_->Delete(key);
        return std::unexpected(ErrNotFound(key));
    }
    
    // 返回值部分
    return value->substr(1 + 8 + 8 + 8);  // 跳过元数据
}

// === Hash ===

VoidResult RedisDataStruct::HSet(const std::string& key, const std::string& field,
                                  const std::string& value, std::optional<int64_t> ttl_ms) {
    // 获取或创建元数据
    ValueMetadata meta;
    auto existing = db_->Get(key);
    if (existing) {
        meta = DecodeMetadata(std::as_bytes(std::span(*existing)));
        if (meta.type != RedisDataType::kHash) {
            return std::unexpected(ErrFailedPrecondition("WRONGTYPE"));
        }
        if (IsExpired(meta)) {
            // 过期，重新创建
            meta = ValueMetadata{};
            meta.type = RedisDataType::kHash;
            meta.version = NewVersion();
            meta.size = 0;
        }
    } else {
        meta.type = RedisDataType::kHash;
        meta.version = NewVersion();
        meta.size = 0;
    }
    
    if (ttl_ms) {
        meta.expiry = std::chrono::steady_clock::now().time_since_epoch().count()
                      + *ttl_ms * 1'000'000;
    }
    
    // 使用 Batch 原子写入
    WriteBatch batch(db_);
    
    // 更新元数据
    meta.size++;
    batch.Put(key, EncodeMetadata(meta));
    
    // 写入字段
    std::string field_key = EncodeHashKey(key, meta.version, field);
    batch.Put(field_key, value);
    
    return batch.Commit();
}

Result<std::string> RedisDataStruct::HGet(const std::string& key, const std::string& field) {
    auto existing = db_->Get(key);
    if (!existing) {
        return std::unexpected(ErrNotFound(key));
    }
    
    auto meta = DecodeMetadata(std::as_bytes(std::span(*existing)));
    if (meta.type != RedisDataType::kHash) {
        return std::unexpected(ErrFailedPrecondition("WRONGTYPE"));
    }
    if (IsExpired(meta)) {
        return std::unexpected(ErrNotFound(key));
    }
    
    std::string field_key = EncodeHashKey(key, meta.version, field);
    return db_->Get(field_key);
}

// === List ===

VoidResult RedisDataStruct::LPush(const std::string& key, const std::string& value) {
    // 类似 HSet，但操作 head 索引
    // ...
}

Result<std::string> RedisDataStruct::LPop(const std::string& key) {
    // ...
}

// === Set ===

VoidResult RedisDataStruct::SAdd(const std::string& key, const std::string& member,
                                  std::optional<int64_t> ttl_ms) {
    // ...
}

// === ZSet ===

VoidResult RedisDataStruct::ZAdd(const std::string& key, const std::string& member,
                                  double score, std::optional<int64_t> ttl_ms) {
    // 需要写入两个键：member->score 和 score->member
    // ...
}

} // namespace bitcask
```

---

## 5. Command.h - 命令处理

### 5.1 接口

```cpp
#pragma once

#include "DataStruct.h"
#include "RESP/RESP.h"
#include <functional>
#include <unordered_map>

namespace bitcask {

struct CommandResult {
    resp::Value reply;
    bool close_connection = false;
};

using CommandHandler = std::function<CommandResult(RedisDataStruct*, const std::vector<std::string>&)>;

// 执行 Redis 命令
CommandResult ExecuteCommand(RedisDataStruct* rdb, const std::vector<std::string>& args);

// 注册命令处理器
void RegisterCommand(const std::string& name, CommandHandler handler);

// 内置命令
CommandResult CmdPing(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdEcho(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdQuit(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdGet(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdSet(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdDel(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdType(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdHSet(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdHGet(RedisDataStruct* rdb, const std::vector<std::string>& args);
CommandResult CmdHDel(RedisDataStruct* rdb, const std::vector<std::string>& args);
// ... 其他命令

} // namespace bitcask
```

### 5.2 实现

```cpp
#include "Command.h"
#include <cstdlib>

namespace bitcask {

static std::unordered_map<std::string, CommandHandler> command_map;

void RegisterCommand(const std::string& name, CommandHandler handler) {
    command_map[name] = std::move(handler);
}

CommandResult ExecuteCommand(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    if (args.empty()) {
        return {resp::Value::Err("ERR no command"), false};
    }
    
    std::string cmd = args[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    
    auto it = command_map.find(cmd);
    if (it == command_map.end()) {
        return {resp::Value::Err("ERR unknown command '" + cmd + "'"), false};
    }
    
    return it->second(rdb, args);
}

// === 内置命令实现 ===

CommandResult CmdPing(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    if (args.size() > 1) {
        return {resp::Value::Bulk(args[1]), false};
    }
    return {resp::Value::Simple("PONG"), false};
}

CommandResult CmdEcho(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return {resp::Value::Err("ERR wrong number of arguments for 'echo' command"), false};
    }
    return {resp::Value::Bulk(args[1]), false};
}

CommandResult CmdQuit(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    return {resp::Value::Simple("OK"), true};  // close_connection = true
}

CommandResult CmdGet(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return {resp::Value::Err("ERR wrong number of arguments for 'get' command"), false};
    }
    
    auto result = rdb->Get(args[1]);
    if (!result) {
        if (result.error().code == ErrorCode::kNotFound) {
            return {resp::Value::Null(), false};
        }
        return {resp::Value::Err(result.error().message), false};
    }
    
    return {resp::Value::Bulk(*result), false};
}

CommandResult CmdSet(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return {resp::Value::Err("ERR wrong number of arguments for 'set' command"), false};
    }
    
    std::optional<int64_t> ttl_ms;
    
    // 解析选项：EX, PX
    for (size_t i = 3; i < args.size(); ++i) {
        std::string opt = args[i];
        std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
        
        if (opt == "EX" && i + 1 < args.size()) {
            ttl_ms = std::stoll(args[++i]) * 1000;
        } else if (opt == "PX" && i + 1 < args.size()) {
            ttl_ms = std::stoll(args[++i]);
        }
    }
    
    auto result = rdb->Set(args[1], args[2], ttl_ms);
    if (!result) {
        return {resp::Value::Err(result.error().message), false};
    }
    
    return {resp::Value::Simple("OK"), false};
}

CommandResult CmdDel(RedisDataStruct* rdb, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return {resp::Value::Err("ERR wrong number of arguments for 'del' command"), false};
    }
    
    int64_t deleted = 0;
    for (size_t i = 1; i < args.size(); ++i) {
        auto result = rdb->Del(args[i]);
        if (result) {
            ++deleted;
        }
    }
    
    return {resp::Value::Int(deleted), false};
}

// 初始化命令注册
struct CommandInitializer {
    CommandInitializer() {
        RegisterCommand("PING", CmdPing);
        RegisterCommand("ECHO", CmdEcho);
        RegisterCommand("QUIT", CmdQuit);
        RegisterCommand("GET", CmdGet);
        RegisterCommand("SET", CmdSet);
        RegisterCommand("DEL", CmdDel);
        RegisterCommand("TYPE", CmdType);
        RegisterCommand("HSET", CmdHSet);
        RegisterCommand("HGET", CmdHGet);
        RegisterCommand("HDEL", CmdHDel);
        // ... 注册其他命令
    }
};

static CommandInitializer init;

} // namespace bitcask
```

---

## 6. Server.h - TCP 服务器

### 6.1 接口

```cpp
#pragma once

#include "DB/DB.h"
#include <string>
#include <jthread>
#include <stop_token>
#include <functional>

namespace bitcask {

struct RedisServerOptions {
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    int backlog = 128;
    size_t max_buffer_size = 1024 * 1024;  // 1MB
};

class RedisServer {
public:
    RedisServer(DB* db, const RedisServerOptions& options);
    ~RedisServer();
    
    // 启动服务器（阻塞）
    VoidResult Start();
    
    // 停止服务器
    void Stop();
    
    // 获取实际端口
    uint16_t Port() const;
    
private:
    void AcceptLoop(std::stop_token st);
    void HandleConnection(int client_fd, std::stop_token st);
    
    DB* db_;
    RedisServerOptions options_;
    std::jthread accept_thread_;
    int listen_fd_;
    uint16_t actual_port_;
};

// 便捷函数
VoidResult Listen(DB* db, const RedisServerOptions& options);

} // namespace bitcask
```

### 6.2 实现

```cpp
#include "Server.h"
#include "DataStruct.h"
#include "Command.h"
#include "RESP/RESP.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

namespace bitcask {

RedisServer::RedisServer(DB* db, const RedisServerOptions& options)
    : db_(db), options_(options), listen_fd_(-1), actual_port_(0) {}

RedisServer::~RedisServer() {
    Stop();
}

VoidResult RedisServer::Start() {
    // 创建 socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return std::unexpected(ErrIOError("socket creation failed"));
    }
    
    // 设置 SO_REUSEADDR
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // 绑定地址
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options_.port);
    inet_pton(AF_INET, options_.host.c_str(), &addr.sin_addr);
    
    if (bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(listen_fd_);
        return std::unexpected(ErrIOError("bind failed"));
    }
    
    // 监听
    if (listen(listen_fd_, options_.backlog) < 0) {
        close(listen_fd_);
        return std::unexpected(ErrIOError("listen failed"));
    }
    
    // 获取实际端口
    socklen_t len = sizeof(addr);
    getsockname(listen_fd_, (struct sockaddr*)&addr, &len);
    actual_port_ = ntohs(addr.sin_port);
    
    // 启动 accept 线程
    accept_thread_ = std::jthread(&RedisServer::AcceptLoop, this);
    
    return {};
}

void RedisServer::Stop() {
    if (listen_fd_ >= 0) {
        accept_thread_.request_stop();
        close(listen_fd_);
        listen_fd_ = -1;
    }
}

uint16_t RedisServer::Port() const {
    return actual_port_;
}

void RedisServer::AcceptLoop(std::stop_token st) {
    while (!st.stop_requested()) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (st.stop_requested()) break;
            continue;
        }
        
        // 每个连接一个线程
        std::jthread(&RedisServer::HandleConnection, this, client_fd).detach();
    }
}

void RedisServer::HandleConnection(int client_fd, std::stop_token st) {
    RedisDataStruct rdb(db_);
    resp::StreamParser parser;
    std::string buffer;
    buffer.resize(4096);
    
    while (!st.stop_requested()) {
        // 读取数据
        ssize_t n = recv(client_fd, buffer.data(), buffer.size(), 0);
        if (n <= 0) {
            break;  // 连接关闭
        }
        
        parser.Append(std::string_view(buffer.data(), n));
        
        // 解析并处理所有完整命令
        while (true) {
            auto [value, ok] = parser.Next();
            if (!ok) break;  // 数据不完整
            
            if (!value->IsArray()) {
                // 无效命令
                auto err = resp::Value::Err("ERR invalid command format");
                send(client_fd, resp::Serialize(err).data(), err_size, 0);
                continue;
            }
            
            // 转换为参数列表
            std::vector<std::string> args;
            for (const auto& item : value->AsArray()) {
                if (item.IsBulk()) {
                    args.push_back(item.AsBulk());
                }
            }
            
            // 执行命令
            auto result = ExecuteCommand(&rdb, args);
            
            // 发送响应
            auto reply = resp::Serialize(result.reply);
            send(client_fd, reply.data(), reply.size(), 0);
            
            if (result.close_connection) {
                close(client_fd);
                return;
            }
        }
        
        // 检查缓冲区大小
        if (parser.Buffered() > options_.max_buffer_size) {
            auto err = resp::Value::Err("ERR buffer overflow");
            send(client_fd, resp::Serialize(err).data(), err_size, 0);
            close(client_fd);
            return;
        }
    }
    
    close(client_fd);
}

VoidResult Listen(DB* db, const RedisServerOptions& options) {
    RedisServer server(db, options);
    return server.Start();
}

} // namespace bitcask
```

---

## 7. 测试要点

```cpp
TEST(RedisDataStruct, StringSetGet) {
    // ...
}

TEST(RedisDataStruct, HashSetGet) {
    // ...
}

TEST(Command, Ping) {
    RedisDataStruct rdb(nullptr);
    auto result = CmdPing(&rdb, {"PING"});
    EXPECT_TRUE(result.reply.IsSimple());
    EXPECT_EQ(result.reply.AsSimple(), "PONG");
}

TEST(Command, SetGet) {
    // 需要真实 DB
    auto db = DB::Open(Options{.data_dir = "/tmp/test_redis"});
    RedisDataStruct rdb(*db);
    
    CmdSet(&rdb, {"SET", "key1", "value1"});
    auto result = CmdGet(&rdb, {"GET", "key1"});
    
    EXPECT_TRUE(result.reply.IsBulk());
    EXPECT_EQ(result.reply.AsBulk(), "value1");
}
```