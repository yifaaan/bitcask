#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <absl/crc/crc32c.h>
#include <absl/types/span.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <utility>

namespace bitcask
{

    enum class LogRecordType : uint8_t
    {
        kNormal = 0,
        kDeleted = 1,
        kTxnFinished = 2, // 用于标记一个事务的完成
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

    // Encode a varint using protobuf's WriteVarint64ToArray
    inline int PutVarint(absl::Span<std::byte> buf, int64_t value)
    {
        auto start = reinterpret_cast<uint8_t*>(buf.data());
        auto end = google::protobuf::io::CodedOutputStream::WriteVarint64ToArray(static_cast<uint64_t>(value), start);
        return static_cast<int>(end - start);
    }

    // Decode a varint using protobuf's CodedInputStream
    inline std::pair<int64_t, int> Varint(absl::Span<const std::byte> buf)
    {
        google::protobuf::io::CodedInputStream stream(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
        uint64_t value;
        stream.ReadVarint64(&value);
        return { static_cast<int64_t>(value), static_cast<int>(stream.CurrentPosition()) };
    }

    // Encode a LogRecord into bytes, returns {encoded_data, total_size}
    std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record);

    // Decode header from bytes, returns {header, header_size}
    std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(absl::Span<const std::byte> buf);

    // Calculate CRC for a log record (same as Go's calcLogRecordCRC)
    uint32_t CalcLogRecordCRC(const LogRecord& record, absl::Span<const std::byte> header);

} // namespace bitcask
