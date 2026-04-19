#pragma once

#include <cstdint>
#include <string>

namespace bitcask
{
    enum class LogRecordType : uint8_t
    {
        kNormal = 0,
        kDeleted = 1, // 删除标记
    };

    // 写入数据文件的记录
    struct LogRecord
    {
        std::string key;
        std::string value;
        LogRecordType type;
    };

    struct LogRecordPos
    {
        uint32_t fid;
        uint64_t offset;
    };
} // namespace bitcask