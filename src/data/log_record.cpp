#include "log_record.h"

#include <cstring>

namespace bitcask
{

    namespace
    {
        absl::string_view ToSV(absl::Span<const std::byte> s)
        {
            return { reinterpret_cast<const char*>(s.data()), s.size() };
        }
    }

    std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record)
    {
        std::vector<std::byte> header(kMaxLogRecordHeaderSize);

        header[4] = static_cast<std::byte>(record.type);
        int index = 5;
        index += PutVarint(absl::MakeSpan(header).subspan(index), static_cast<int64_t>(record.key.size()));
        index += PutVarint(absl::MakeSpan(header).subspan(index), static_cast<int64_t>(record.value.size()));

        auto length = static_cast<int64_t>(index + record.key.size() + record.value.size()); // record length
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

        auto crc_val = static_cast<uint32_t>(absl::ComputeCrc32c(ToSV(absl::MakeConstSpan(buf).subspan(4))));
        std::memcpy(buf.data(), &crc_val, sizeof(crc_val)); // little endian

        return { std::move(buf), length };
    }

    std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(absl::Span<const std::byte> buf)
    {
        if (buf.size() <= 4)
        {
            return { std::nullopt, 0 };
        }

        LogRecordHeader header;
        std::memcpy(&header.crc, buf.data(), sizeof(header.crc));
        header.type = static_cast<LogRecordType>(buf[4]);

        int index = 5;
        auto [key_size, n1] = Varint(buf.subspan(index));
        header.key_size = key_size;
        index += n1;

        auto [value_size, n2] = Varint(buf.subspan(index));
        header.value_size = value_size;
        index += n2;

        return { header, static_cast<int64_t>(index) };
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

        return static_cast<uint32_t>(crc_val);
    }

} // namespace bitcask
