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

    struct IteratorOptions
    {
        absl::string_view prefix;
        bool reverse = false;
    };

} // namespace bitcask
