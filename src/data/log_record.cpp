#include "log_record.h"

#include <cstring>

namespace bitcask
{

    static absl::string_view ToSV(absl::Span<const std::byte> s)
    {
        return {reinterpret_cast<const char*>(s.data()), s.size()};
    }

    std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record)
    {
        std::vector<std::byte> header(kMaxLogRecordHeaderSize);

        header[4] = static_cast<std::byte>(record.type);
        int index = 5;
        index += PutVarint(absl::Span<std::byte>(header).subspan(index), static_cast<int64_t>(record.key.size()));
        index += PutVarint(absl::Span<std::byte>(header).subspan(index), static_cast<int64_t>(record.value.size()));

        auto length = static_cast<int64_t>(index + record.key.size() + record.value.size());
        std::vector<std::byte> buf(length);

        // Copy header
        std::memcpy(buf.data(), header.data(), index);

        // Copy key
        if (!record.key.empty())
        {
            std::memcpy(buf.data() + index, record.key.data(), record.key.size());
        }

        // Copy value
        if (!record.value.empty())
        {
            std::memcpy(buf.data() + index + record.key.size(), record.value.data(), record.value.size());
        }

        // Calculate and store CRC in little-endian (over everything after the CRC field)
        uint32_t crc_val = absl::ComputeCrc32c(ToSV(absl::Span<const std::byte>(buf).subspan(4))).value();
        // Store CRC as little-endian, matching Go's binary.LittleEndian.PutUint32
        buf[0] = static_cast<std::byte>(crc_val & 0xFF);
        buf[1] = static_cast<std::byte>((crc_val >> 8) & 0xFF);
        buf[2] = static_cast<std::byte>((crc_val >> 16) & 0xFF);
        buf[3] = static_cast<std::byte>((crc_val >> 24) & 0xFF);

        return {std::move(buf), length};
    }

    std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(absl::Span<const std::byte> buf)
    {
        if (buf.size() <= 4)
        {
            return {std::nullopt, 0};
        }

        LogRecordHeader header;
        // Read CRC as little-endian
        header.crc = static_cast<uint32_t>(static_cast<uint8_t>(buf[0])) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(buf[1])) << 8) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(buf[2])) << 16) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(buf[3])) << 24);
        header.type = static_cast<LogRecordType>(buf[4]);

        int index = 5;
        auto [key_size, n1] = Varint(buf.subspan(index));
        header.key_size = key_size;
        index += n1;

        auto [value_size, n2] = Varint(buf.subspan(index));
        header.value_size = value_size;
        index += n2;

        return {header, static_cast<int64_t>(index)};
    }

    uint32_t CalcLogRecordCRC(const LogRecord& record, absl::Span<const std::byte> header)
    {
        auto crc_val = absl::ComputeCrc32c(ToSV(header));

        if (!record.key.empty())
        {
            auto key_span = absl::Span<const std::byte>(
                reinterpret_cast<const std::byte*>(record.key.data()), record.key.size());
            crc_val = absl::ExtendCrc32c(crc_val, ToSV(key_span));
        }

        if (!record.value.empty())
        {
            auto value_span = absl::Span<const std::byte>(
                reinterpret_cast<const std::byte*>(record.value.data()), record.value.size());
            crc_val = absl::ExtendCrc32c(crc_val, ToSV(value_span));
        }

        return crc_val.value();
    }

} // namespace bitcask
