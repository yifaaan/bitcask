# FIO 模块设计文档

## 1. 模块概述

FIO (File I/O) 模块提供文件 I/O 抽象层，支持：
- **FileIO**: 标准 `FILE*` / `os.File` 封装，支持读写
- **MmapIO**: 内存映射只读文件（用于快速加载）
- **IOManager**: 抽象接口，统一两种实现

## 2. 依赖关系

```
abseil (absl::Status/StatusOr)
    ↑
   FIO (IOManager)
    ↑
   Data (DataFile 使用 IOManager)
```

## 3. 接口定义

### 3.1 IOManager.h

```cpp
#pragma once

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include <span>
#include <cstdint>
#include <memory>

namespace bitcask {

enum class IOType {
    Standard,  // FILE* / os.File
    MMap,      // Memory-mapped (read-only)
};

class IOManager {
public:
    virtual ~IOManager() = default;
    
    // 读取 [offset, offset + len) 到 buf
    // 返回实际读取字节数，或错误
    virtual absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) = 0;
    
    // 写入数据到文件末尾（append-only）
    // 返回写入字节数，或错误
    virtual absl::StatusOr<int64_t> Write(std::span<const std::byte> data) = 0;
    
    // 同步到磁盘
    virtual absl::Status Sync() = 0;
    
    // 关闭文件
    virtual absl::Status Close() = 0;
    
    // 获取文件大小
    virtual absl::StatusOr<int64_t> Size() = 0;
    
    // 工厂函数
    static std::unique_ptr<IOManager> Open(const std::string& path, IOType type);
};

} // namespace bitcask
```

### 3.2 FileIO.h

```cpp
#pragma once

#include "IOManager.h"
#include <cstdio>
#include <string>

namespace bitcask {

class FileIO : public IOManager {
public:
    ~FileIO() override;
    
    // 打开文件（读写 + append + create）
    static absl::StatusOr<std::unique_ptr<FileIO>> Open(const std::string& path);
    
    absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
    absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override;
    absl::Status Sync() override;
    absl::Status Close() override;
    absl::StatusOr<int64_t> Size() override;
    
private:
    FileIO(FILE* file) : file_(file) {}
    FILE* file_;
    std::string path_;
};

} // namespace bitcask
```

### 3.3 MmapIO.h

```cpp
#pragma once

#include "IOManager.h"
#include <string>

namespace bitcask {

class MmapIO : public IOManager {
public:
    ~MmapIO() override;
    
    // 打开只读内存映射文件
    static absl::StatusOr<std::unique_ptr<MmapIO>> Open(const std::string& path);
    
    absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
    absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override;  // 总是返回错误
    absl::Status Sync() override;  // 无操作（只读）
    absl::Status Close() override;
    absl::StatusOr<int64_t> Size() override;
    
private:
    MmapIO(void* addr, int64_t size, int fd)
        : addr_(addr), size_(size), fd_(fd) {}
    
    void* addr_;      // 映射地址
    int64_t size_;    // 文件大小
    int fd_;          // 文件描述符 (Unix) 或 -1 (Windows HANDLE 另存)
};

} // namespace bitcask
```

---

## 4. 实现要点

### 4.1 FileIO 实现

```cpp
// FileIO.cpp

#include "FileIO.h"
#include <cerrno>
#include <cstring>

#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#else
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace bitcask {

FileIO::~FileIO() {
    if (file_) {
        Close();
    }
}

Result<std::unique_ptr<FileIO>> FileIO::Open(const std::string& path) {
    // 打开文件：读写 + 二进制 + append + create
    FILE* file = std::fopen(path.c_str(), "a+b");
    if (!file) {
        return std::unexpected(ErrIOError(
            std::format("failed to open file '{}': {}", path, std::strerror(errno))));
    }
    auto io = std::unique_ptr<FileIO>(new FileIO(file));
    io->path_ = path;
    return io;
}

Result<int64_t> FileIO::Read(std::span<std::byte> buf, int64_t offset) {
    if (!file_) {
        return std::unexpected(ErrIOError("file not open"));
    }
    
    // 定位到 offset
#ifdef _WIN32
    if (_fseeki64(file_, offset, SEEK_SET) != 0) {
#else
    if (fseeko(file_, offset, SEEK_SET) != 0) {
#endif
        return std::unexpected(ErrIOError("fseek failed"));
    }
    
    // 读取
    size_t n = std::fread(buf.data(), 1, buf.size(), file_);
    if (n == 0 && ferror(file_)) {
        return std::unexpected(ErrIOError("fread failed"));
    }
    return static_cast<int64_t>(n);
}

Result<int64_t> FileIO::Write(std::span<const std::byte> data) {
    if (!file_) {
        return std::unexpected(ErrIOError("file not open"));
    }
    
    // 定位到末尾（确保 append）
#ifdef _WIN32
    if (_fseeki64(file_, 0, SEEK_END) != 0) {
#else
    if (fseeko(file_, 0, SEEK_END) != 0) {
#endif
        return std::unexpected(ErrIOError("fseek to end failed"));
    }
    
    // 写入
    size_t n = std::fwrite(data.data(), 1, data.size(), file_);
    if (n != data.size()) {
        return std::unexpected(ErrIOError("fwrite incomplete"));
    }
    return static_cast<int64_t>(n);
}

VoidResult FileIO::Sync() {
    if (!file_) {
        return std::unexpected(ErrIOError("file not open"));
    }
    
    std::fflush(file_);
    
#ifdef _WIN32
    // Windows: _commit 确保写入磁盘
    if (_commit(_fileno(file_)) != 0) {
        return std::unexpected(ErrIOError("_commit failed"));
    }
#else
    // POSIX: fsync
    if (fsync(fileno(file_)) != 0) {
        return std::unexpected(ErrIOError("fsync failed"));
    }
#endif
    return {};
}

VoidResult FileIO::Close() {
    if (!file_) return {};
    
    if (std::fclose(file_) != 0) {
        file_ = nullptr;
        return std::unexpected(ErrIOError("fclose failed"));
    }
    file_ = nullptr;
    return {};
}

Result<int64_t> FileIO::Size() {
    if (!file_) {
        return std::unexpected(ErrIOError("file not open"));
    }
    
    std::fflush(file_);
    
#ifdef _WIN32
    struct _stat64 st;
    if (_fstat64(_fileno(file_), &st) != 0) {
#else
    struct stat st;
    if (fstat(fileno(file_), &st) != 0) {
#endif
        return std::unexpected(ErrIOError("fstat failed"));
    }
    return st.st_size;
}

} // namespace bitcask
```

### 4.2 MmapIO 实现（跨平台）

```cpp
// MmapIO.cpp

#include "MmapIO.h"
#include <cstring>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace bitcask {

MmapIO::~MmapIO() {
    if (addr_) {
        Close();
    }
}

Result<std::unique_ptr<MmapIO>> MmapIO::Open(const std::string& path) {
#ifdef _WIN32
    // Windows 实现
    HANDLE hFile = CreateFileA(
        path.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    
    if (hFile == INVALID_HANDLE_VALUE) {
        return std::unexpected(ErrIOError(
            std::format("CreateFile failed for '{}'", path)));
    }
    
    LARGE_INTEGER fileSize;
    if (!GetFileSizeEx(hFile, &fileSize)) {
        CloseHandle(hFile);
        return std::unexpected(ErrIOError("GetFileSize failed"));
    }
    
    HANDLE hMap = CreateFileMappingA(
        hFile,
        nullptr,
        PAGE_READONLY,
        0, 0,
        nullptr);
    
    if (!hMap) {
        CloseHandle(hFile);
        return std::unexpected(ErrIOError("CreateFileMapping failed"));
    }
    
    void* addr = MapViewOfFile(
        hMap,
        FILE_MAP_READ,
        0, 0, 0);
    
    CloseHandle(hMap);  // 映射后可以关闭
    if (!addr) {
        CloseHandle(hFile);
        return std::unexpected(ErrIOError("MapViewOfFile failed"));
    }
    
    auto io = std::unique_ptr<MmapIO>(new MmapIO(addr, fileSize.QuadPart, -1));
    io->handle_ = hFile;  // 需要额外存储 HANDLE
    return io;
    
#else
    // POSIX 实现
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return std::unexpected(ErrIOError(
            std::format("open failed for '{}': {}", path, strerror(errno))));
    }
    
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return std::unexpected(ErrIOError("fstat failed"));
    }
    
    void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        return std::unexpected(ErrIOError("mmap failed"));
    }
    
    return std::unique_ptr<MmapIO>(new MmapIO(addr, st.st_size, fd));
#endif
}

Result<int64_t> MmapIO::Read(std::span<std::byte> buf, int64_t offset) {
    if (!addr_) {
        return std::unexpected(ErrIOError("file not mapped"));
    }
    if (offset < 0 || offset > size_) {
        return std::unexpected(ErrIOError("offset out of range"));
    }
    
    int64_t avail = size_ - offset;
    int64_t to_read = std::min(avail, static_cast<int64_t>(buf.size()));
    
    if (to_read > 0) {
        std::memcpy(buf.data(), static_cast<char*>(addr_) + offset, to_read);
    }
    
    return to_read;
}

Result<int64_t> MmapIO::Write(std::span<const std::byte> data) {
    // MmapIO 只读，不支持写入
    return std::unexpected(ErrIOError("MmapIO is read-only"));
}

VoidResult MmapIO::Sync() {
    // 只读映射，无需 sync
    return {};
}

VoidResult MmapIO::Close() {
    if (!addr_) return {};
    
#ifdef _WIN32
    UnmapViewOfFile(addr_);
    if (handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }
#else
    munmap(addr_, size_);
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
#endif
    addr_ = nullptr;
    size_ = 0;
    return {};
}

Result<int64_t> MmapIO::Size() {
    if (!addr_) {
        return std::unexpected(ErrIOError("file not mapped"));
    }
    return size_;
}

} // namespace bitcask
```

---

## 5. IOManager 工厂

```cpp
// IOManager.cpp

#include "IOManager.h"
#include "FileIO.h"
#include "MmapIO.h"

namespace bitcask {

std::unique_ptr<IOManager> IOManager::Open(const std::string& path, IOType type) {
    switch (type) {
        case IOType::Standard: {
            auto result = FileIO::Open(path);
            if (!result) return nullptr;  // 或抛异常
            return *result;
        }
        case IOType::MMap: {
            auto result = MmapIO::Open(path);
            if (!result) return nullptr;
            return *result;
        }
    }
    return nullptr;
}

} // namespace bitcask
```

---

## 6. 使用场景

| 场景 | 使用类型 | 理由 |
|------|----------|------|
| Active data file | Standard (FileIO) | 需要写入，append-only |
| Older data files (read) | Standard 或 MMap | 读取历史数据 |
| Hint file (read) | Standard 或 MMap | 启动恢复时快速读取 |
| Merge files (write) | Standard | 合并时写入新文件 |

**MMap 适用条件**:
- 文件大小已知且固定
- 仅需读取
- 频繁随机读取（避免系统调用开销）

**FileIO 适用条件**:
- 需要写入
- 文件持续增长
- 简单场景，避免 mmap 复杂性

---

## 7. 与原版本差异

| 原版本 | 新版本 |
|--------|--------|
| `absl::Span<std::byte>` | `std::span<std::byte>` |
| `absl::StatusOr<...>` | **保留** |

---

## 8. 测试要点

```cpp
// Test/TestFIO/TestFileIO.cpp

TEST(FileIO, WriteAndRead) {
    std::string path = "/tmp/test_file_io.data";
    
    // 写入
    auto write_result = FileIO::Open(path);
    ASSERT_TRUE(write_result.has_value());
    auto& writer = *write_result;
    
    std::vector<std::byte> data = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
    auto write_res = writer->Write(data);
    ASSERT_TRUE(write_res.has_value());
    EXPECT_EQ(*write_res, 3);
    
    writer->Sync();
    writer->Close();
    
    // 读取
    auto read_result = FileIO::Open(path);
    ASSERT_TRUE(read_result.has_value());
    auto& reader = *read_result;
    
    std::vector<std::byte> buf(3);
    auto read_res = reader->Read(buf, 0);
    ASSERT_TRUE(read_res.has_value());
    EXPECT_EQ(*read_res, 3);
    EXPECT_EQ(buf, data);
    
    reader->Close();
    std::remove(path.c_str());
}

TEST(MmapIO, Read) {
    // 创建测试文件
    std::string path = "/tmp/test_mmap.data";
    // ...
    
    auto result = MmapIO::Open(path);
    ASSERT_TRUE(result.has_value());
    
    auto size = (*result)->Size();
    ASSERT_TRUE(size.has_value());
    
    // 读取测试...
}
```