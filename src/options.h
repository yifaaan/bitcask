#pragma once

#include <filesystem>

namespace bitcask
{

    struct Options
    {
        std::filesystem::path data_dir; // 数据文件存储目录
        uint64_t max_data_file_size = 1024 * 1024 * 10; // 每个数据文件的最大大小，默认10MB
        bool sync_on_write; // 是否在每次写入后立即将数据同步到磁盘
    };

} // namespace bitcask