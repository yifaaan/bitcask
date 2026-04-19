#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

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
        static std::vector<std::byte> Encode(const LogRecord& record)
        {
            // TODO: 实现编码逻辑，将 LogRecord 序列化为字节流
            return {};
        }

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