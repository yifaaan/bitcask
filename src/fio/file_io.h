#pragma once

#include <cstdio>
#include <filesystem>
#include <memory>

#include "io_manager.h"

namespace bitcask
{
    class FileIO final : public IOManager
    {
    public:
        // static Open() 工厂 — 构造可能失败（文件不存在等），用工厂返回对象而不是在构造函数里抛异常
        static std::unique_ptr<FileIO> Open(const std::filesystem::path& path);

        ~FileIO() override;

        FileIO(const FileIO&) = delete;
        auto operator=(const FileIO&) -> FileIO& = delete;
        FileIO(FileIO&&) = delete;
        auto operator=(FileIO&&) -> FileIO& = delete;

        // 从文件的指定位置读取指定长度的数据，返回读取到的数据
        std::vector<std::byte> Read(uint64_t offset, size_t length) override;
        // 向文件末尾写入数据，返回写入数据的大小
        size_t Write(std::span<const std::byte> data) override;

        
        bool Sync() override;
        void Close() override;
        // 获取文件大小
        auto Size() const -> uint64_t;

    private:
        explicit FileIO(std::FILE* fd);

        std::FILE* fd_;
    };
} // namespace bitcask