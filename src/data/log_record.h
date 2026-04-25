#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "absl/crc/crc32c.h"

namespace bitcask
{

    enum class LogRecordType : uint8_t
    {
        kNormal = 0,
        kDeleted = 1,
    };

    constexpr size_t kMaxLogRecordHeaderSize = 5 + 2 * 5; // type(1) + 2 * maxVarintLen32(5)

    struct LogRecord
    {
        std::string key;
        std::string value;
        LogRecordType type = LogRecordType::kNormal;
    };

    struct LogRecordPos
    {
        uint32_t fid = 0;
        int64_t offset = 0;
    };

    struct LogRecordHeader
    {
        uint32_t crc = 0;
        LogRecordType type = LogRecordType::kNormal;
        int64_t key_size = 0;
        int64_t value_size = 0;
    };

    // Encode a varint (signed, zigzag-encoded, same as Go's binary.PutVarint)
    inline int PutVarint(std::span<std::byte> buf, int64_t value)
    {
        uint64_t uval = static_cast<uint64_t>(value);
        int i = 0;
        while (uval >= 0x80)
        {
            buf[i++] = static_cast<std::byte>((uval & 0x7F) | 0x80);
            uval >>= 7;
        }
        buf[i++] = static_cast<std::byte>(uval);
        return i;
    }

    // Decode a varint, returns {value, bytes_read}
    inline std::pair<int64_t, int> Varint(std::span<const std::byte> buf)
    {
        uint64_t result = 0;
        int shift = 0;
        int i = 0;
        while (i < static_cast<int>(buf.size()))
        {
            auto b = static_cast<uint8_t>(buf[i]);
            result |= static_cast<uint64_t>(b & 0x7F) << shift;
            i++;
            if ((b & 0x80) == 0)
                break;
            shift += 7;
        }
        return {static_cast<int64_t>(result), i};
    }

    // Encode a LogRecord into bytes, returns {encoded_data, total_size}
    std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record);

    // Decode header from bytes, returns {header, header_size}
    std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(std::span<const std::byte> buf);

    // Calculate CRC for a log record (same as Go's calcLogRecordCRC)
    uint32_t CalcLogRecordCRC(const LogRecord& record, std::span<const std::byte> header);

} // namespace bitcask
