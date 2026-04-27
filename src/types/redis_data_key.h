#pragma once

#include <absl/strings/string_view.h>
#include <absl/types/span.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "../data/log_record.h"

namespace bitcask
{

    // Redis value layouts:
    // String: [type(1 byte)][expiry(varint)][raw value]
    // Hash/Set metadata: [type(1 byte)][expiry(varint)][version(varint)][size(varint)]
    // Example: HSet("user:1", "name", "alice") stores metadata at "user:1".
    enum class RedisDataType : uint8_t
    {
        kString,
        kHash,
        kSet,
        kList,
        kZSet,
    };

    struct ValueMetadata
    {
        RedisDataType type = RedisDataType::kString;
        int64_t expiry = 0;
        int64_t version = 0;
        int64_t size = 0;
    };

    namespace redis_data_key_detail
    {
        inline void AppendVarint(std::string& dst, int64_t value)
        {
            std::array<std::byte, 10> varint_buf{};
            const int varint_len = PutVarint(
                absl::Span<std::byte>(varint_buf.data(), varint_buf.size()), value);
            dst.append(reinterpret_cast<const char*>(varint_buf.data()), static_cast<size_t>(varint_len));
        }
    } // namespace redis_data_key_detail

    // Hash field data key layout: [key][version(varint)][field]
    // Example: HSet("user:1", "name", "alice") stores value at "user:1"|version|"name".
    struct HashDataKey
    {
        absl::string_view key;
        int64_t version;
        absl::string_view field;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + 10 + field.size());
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendVarint(encoded, version);
            encoded.append(field.data(), field.size());
            return encoded;
        }
    };

    // Set member data key layout: [key][version(varint)][member][member_size(varint)]
    // Example: SAdd("tags", "cpp") stores an empty value at "tags"|version|"cpp"|3.
    struct SetDataKey
    {
        absl::string_view key;
        int64_t version;
        absl::string_view member;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + 20 + member.size());
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendVarint(encoded, version);
            encoded.append(member.data(), member.size());
            redis_data_key_detail::AppendVarint(encoded, static_cast<int64_t>(member.size()));
            return encoded;
        }
    };

} // namespace bitcask
