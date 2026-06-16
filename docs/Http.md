# HTTP 模块设计文档

## 1. 模块概述

HTTP 模块实现 REST API 服务器，使用 **cpp-httplib**（通过 vcpkg）。

## 2. 依赖关系

```
Core (Error)
    ↑
DB (核心引擎)
    ↑
HTTP (Server) ← httplib (vcpkg)
```

---

## 3. Server.h 设计

### 3.1 接口

```cpp
#pragma once

#include "DB/DB.h"
#include <string>

namespace bitcask::http {

struct HttpServerOptions {
    std::string host = "127.0.0.1";
    uint16_t port = 8080;
};

// 注册路由到 httplib::Server
void RegisterRoutes(DB* db, httplib::Server& server);

// 启动 HTTP 服务器（阻塞）
VoidResult Listen(DB* db, const HttpServerOptions& options);

} // namespace bitcask::http
```

### 3.2 实现

```cpp
#include "Server.h"

#include <httplib.h>
#include <nlohmann/json.hpp>  // 可选，或手动构建 JSON

namespace bitcask::http {

void RegisterRoutes(DB* db, httplib::Server& server) {
    
    // === Health Check ===
    server.Get("/v1/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{\"ok\":true}", "application/json");
    });
    
    // === Key-Value ===
    
    // PUT /v1/kv/:key
    server.Put("/v1/kv/(.*)", [db](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.matches[1];
        std::string value = req.body;
        
        auto result = db->Put(key, value);
        if (!result) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"message\":\"" + result.error().message + "\"}", 
                           "application/json");
            return;
        }
        
        res.set_content("{\"ok\":true}", "application/json");
    });
    
    // GET /v1/kv/:key
    server.Get("/v1/kv/(.*)", [db](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.matches[1];
        
        auto result = db->Get(key);
        if (!result) {
            if (result.error().code == ErrorCode::kNotFound) {
                res.status = 404;
                res.set_content("{\"ok\":false,\"message\":\"key not found\"}", 
                               "application/json");
            } else {
                res.status = 500;
                res.set_content("{\"ok\":false,\"message\":\"" + result.error().message + "\"}", 
                               "application/json");
            }
            return;
        }
        
        res.set_content("{\"ok\":true,\"value\":\"" + *result + "\"}", "application/json");
    });
    
    // DELETE /v1/kv/:key
    server.Delete("/v1/kv/(.*)", [db](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.matches[1];
        
        auto result = db->Delete(key);
        res.set_content("{\"ok\":true}", "application/json");
    });
    
    // === Keys ===
    
    // GET /v1/keys
    server.Get("/v1/keys", [db](const httplib::Request&, httplib::Response& res) {
        auto result = db->ListKeys();
        if (!result) {
            res.status = 500;
            res.set_content("{\"ok\":false,\"message\":\"" + result.error().message + "\"}", 
                           "application/json");
            return;
        }
        
        // 构建 JSON 数组
        std::string json = "{\"ok\":true,\"keys\":[";
        for (size_t i = 0; i < result->size(); ++i) {
            if (i > 0) json += ",";
            json += "\"" + (*result)[i] + "\"";
        }
        json += "]}";
        
        res.set_content(json, "application/json");
    });
    
    // === Entries ===
    
    // GET /v1/entries
    server.Get("/v1/entries", [db](const httplib::Request& req, httplib::Response& res) {
        std::string prefix = req.get_param_value("prefix");
        bool reverse = req.has_param("reverse");
        
        // 使用迭代器
        auto iter_result = db->NewIterator(IteratorOptions{prefix, reverse});
        if (!iter_result) {
            res.status = 500;
            res.set_content("{\"ok\":false}", "application/json");
            return;
        }
        
        auto& iter = *iter_result;
        iter->Rewind();
        
        std::string json = "{\"ok\":true,\"entries\":[";
        bool first = true;
        
        while (iter->Valid()) {
            if (!first) json += ",";
            first = false;
            
            std::string key = iter->Key();
            auto value = iter->Value();
            
            json += "{\"key\":\"" + key + "\",\"value\":\"" + value + "\"}";
            iter->Next();
        }
        
        json += "]}";
        res.set_content(json, "application/json");
    });
    
    // === Stats ===
    
    // GET /v1/stats
    server.Get("/v1/stats", [db](const httplib::Request&, httplib::Response& res) {
        auto result = db->Stat();
        if (!result) {
            res.status = 500;
            res.set_content("{\"ok\":false}", "application/json");
            return;
        }
        
        auto& stat = *result;
        std::string json = std::format(
            "{{\"ok\":true,\"key_num\":{},\"data_file_num\":{},\"reclaimable_size\":{},\"disk_size\":{}}}",
            stat.key_num, stat.data_file_num, stat.reclaimable_size, stat.disk_size);
        
        res.set_content(json, "application/json");
    });
    
    // === Sync ===
    
    // POST /v1/sync
    server.Post("/v1/sync", [db](const httplib::Request&, httplib::Response& res) {
        auto result = db->Sync();
        if (!result) {
            res.status = 500;
            res.set_content("{\"ok\":false}", "application/json");
            return;
        }
        res.set_content("{\"ok\":true}", "application/json");
    });
    
    // === Merge ===
    
    // POST /v1/merge
    server.Post("/v1/merge", [db](const httplib::Request&, httplib::Response& res) {
        auto result = db->Merge();
        if (!result) {
            res.status = 500;
            res.set_content("{\"ok\":false,\"message\":\"" + result.error().message + "\"}", 
                           "application/json");
            return;
        }
        res.set_content("{\"ok\":true}", "application/json");
    });
    
    // === Backup ===
    
    // POST /v1/backup
    server.Post("/v1/backup", [db](const httplib::Request& req, httplib::Response& res) {
        std::string dest = req.get_param_value("dest");
        if (dest.empty()) {
            dest = req.get_param_value("path");
        }
        
        if (dest.empty()) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"message\":\"dest path required\"}", 
                           "application/json");
            return;
        }
        
        auto result = db->Backup(dest);
        if (!result) {
            res.status = 500;
            res.set_content("{\"ok\":false,\"message\":\"" + result.error().message + "\"}", 
                           "application/json");
            return;
        }
        res.set_content("{\"ok\":true}", "application/json");
    });
    
    // === CORS ===
    
    server.Options(".*", [](const httplib::Request&, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Methods", "GET,PUT,DELETE,POST,OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "Content-Type");
    });
}

VoidResult Listen(DB* db, const HttpServerOptions& options) {
    httplib::Server server;
    
    RegisterRoutes(db, server);
    
    if (!server.listen(options.host, options.port)) {
        return std::unexpected(ErrIOError("failed to start HTTP server"));
    }
    
    // listen() 是阻塞调用
    return {};
}

} // namespace bitcask::http
```

---

## 4. Main.cpp - HTTP 入口

```cpp
#include "DB/DB.h"
#include "Http/Server.h"

#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    // 解析命令行参数
    bitcask::Options db_options;
    bitcask::http::HttpServerOptions http_options;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--data-dir" && i + 1 < argc) {
            db_options.data_dir = argv[++i];
        } else if (arg == "--host" && i + 1 < argc) {
            http_options.host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            http_options.port = std::stoi(argv[++i]);
        } else if (arg == "--max-data-file-size" && i + 1 < argc) {
            db_options.max_data_file_size = std::stoll(argv[++i]);
        } else if (arg == "--sync-on-write") {
            db_options.sync_on_write = true;
        } else if (arg == "--help") {
            std::cout << "Usage: bitcask_http [options]\n"
                      << "Options:\n"
                      << "  --data-dir <path>       Data directory (default: ./bitcask_data)\n"
                      << "  --host <host>           HTTP host (default: 127.0.0.1)\n"
                      << "  --port <port>           HTTP port (default: 8080)\n"
                      << "  --max-data-file-size N  Max data file size in bytes (default: 10MB)\n"
                      << "  --sync-on-write         Sync after each write\n"
                      << "  --help                  Show this help\n";
            return 0;
        }
    }
    
    // 打开数据库
    auto db_result = bitcask::DB::Open(db_options);
    if (!db_result) {
        std::cerr << "Failed to open database: " << db_result.error().message << "\n";
        return 1;
    }
    
    auto& db = *db_result;
    
    std::cout << "Bitcask HTTP server starting on " 
              << http_options.host << ":" << http_options.port << "\n";
    
    // 启动 HTTP 服务器
    auto result = bitcask::http::Listen(db, http_options);
    if (!result) {
        std::cerr << "Failed to start HTTP server: " << result.error().message << "\n";
        return 1;
    }
    
    return 0;
}
```

---

## 5. RedisMain.cpp - Redis 入口

```cpp
#include "DB/DB.h"
#include "Redis/Server.h"

#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    // 解析命令行参数（与 HTTP 版类似）
    bitcask::Options db_options;
    bitcask::RedisServerOptions redis_options;
    
    // ... 参数解析
    
    // 打开数据库
    auto db_result = bitcask::DB::Open(db_options);
    if (!db_result) {
        std::cerr << "Failed to open database: " << db_result.error().message << "\n";
        return 1;
    }
    
    auto& db = *db_result;
    
    std::cout << "Bitcask Redis server starting on " 
              << redis_options.host << ":" << redis_options.port << "\n";
    
    // 启动 Redis 服务器
    auto result = bitcask::RedisListen(db, redis_options);
    if (!result) {
        std::cerr << "Failed to start Redis server: " << result.error().message << "\n";
        return 1;
    }
    
    return 0;
}
```

---

## 6. API 文档

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/health` | 健康检查 |
| PUT | `/v1/kv/:key` | 写入 key-value |
| GET | `/v1/kv/:key` | 读取 key 的 value |
| DELETE | `/v1/kv/:key` | 删除 key |
| GET | `/v1/keys` | 列出所有 keys |
| GET | `/v1/entries` | 列出所有 key-value（支持 `?prefix=` 和 `?reverse=1`） |
| GET | `/v1/stats` | 统计信息 |
| POST | `/v1/sync` | 同步数据文件 |
| POST | `/v1/merge` | 执行合并 |
| POST | `/v1/backup?dest=<path>` | 备份到指定路径 |

---

## 7. 测试要点

```cpp
TEST(HTTP, Health) {
    httplib::Client client("http://127.0.0.1:8080");
    auto res = client.Get("/v1/health");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    EXPECT_EQ(res->body, "{\"ok\":true}");
}

TEST(HTTP, PutGet) {
    httplib::Client client("http://127.0.0.1:8080");
    
    auto put_res = client.Put("/v1/kv/testkey", "testvalue", "text/plain");
    ASSERT_TRUE(put_res);
    EXPECT_EQ(put_res->status, 200);
    
    auto get_res = client.Get("/v1/kv/testkey");
    ASSERT_TRUE(get_res);
    EXPECT_EQ(get_res->status, 200);
    // 验证 JSON 包含 testvalue
}

TEST(HTTP, Delete) {
    httplib::Client client("http://127.0.0.1:8080");
    
    client.Put("/v1/kv/delkey", "value");
    auto del_res = client.Delete("/v1/kv/delkey");
    EXPECT_EQ(del_res->status, 200);
    
    auto get_res = client.Get("/v1/kv/delkey");
    EXPECT_EQ(get_res->status, 404);
}

TEST(HTTP, Keys) {
    httplib::Client client("http://127.0.0.1:8080");
    
    client.Put("/v1/kv/a", "1");
    client.Put("/v1/kv/b", "2");
    
    auto res = client.Get("/v1/keys");
    EXPECT_EQ(res->status, 200);
    // 验证 JSON 包含 ["a", "b"]
}
```