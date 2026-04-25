#pragma once

#include <filesystem>
#include <cstdint>

#include "index/index.h"

namespace bitcask
{

    struct Options
    {
        std::filesystem::path data_dir;
        uint64_t max_data_file_size = 1024 * 1024 * 10; // 10MB
        bool sync_on_write = false;
        IndexType index_type = IndexType::BTree;
    };

    // 迭代器选项
    struct IteratorOptions
    {
        absl::string_view prefix;
        bool reverse = false;
    };

    // 批量写入选项
    struct WriteBatchOptions
    {
        // 批量写入时的最大记录数，超过后会自动提交
        uint32_t max_batch_size = 1000;
        // 提交时是否强制同步数据文件，确保数据持久化
        bool sync_on_commit = true;
    };

} // namespace bitcask
