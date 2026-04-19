#pragma once

#include <cstdint>
#include <span>
#include <filesystem>
#include <vector>

namespace bitcask
{
    // 负责管理数据文件的读写，目前仅仅支持文件IO
    class IOManager
    {
    public:
        virtual ~IOManager() = default;

        // 从文件的指定位置读取指定长度的数据，返回读取到的数据
        virtual std::vector<std::byte> Read(uint64_t offset, size_t length) = 0;
        // 向文件末尾写入数据，返回写入数据的大小
        virtual size_t Write(std::span<const std::byte>) = 0;
        // 将文件缓冲区中的数据同步到磁盘
        virtual bool Sync() = 0;
        // 关闭
        virtual void Close() = 0;
    };

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path);

} // namespace bitcask