#pragma once

#include <absl/strings/string_view.h>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace bitcask
{

    // Redis value layouts:
    // String: [type(1 byte)][expiry(8-byte little-endian)][raw value]
    // Hash/Set/ZSet metadata: [type(1 byte)][expiry(8)][version(8)][size(8)]
    // List metadata: [type(1 byte)][expiry(8)][version(8)][size(8)][head(8)][tail(8)]
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
        uint64_t head = 0;
        uint64_t tail = 0;
    };

    namespace redis_data_key_detail
    {
        static_assert(std::endian::native == std::endian::little);

        constexpr size_t kFixedIntSize = sizeof(uint64_t);

        inline void AppendUint64(std::string& dst, uint64_t value)
        {
            std::array<char, kFixedIntSize> buf{};
            std::memcpy(buf.data(), &value, buf.size());
            dst.append(buf.data(), buf.size());
        }

        inline void AppendInt64(std::string& dst, int64_t value)
        {
            AppendUint64(dst, static_cast<uint64_t>(value));
        }

        inline uint64_t ReadUint64(absl::string_view src)
        {
            uint64_t value = 0;
            std::memcpy(&value, src.data(), kFixedIntSize);
            return value;
        }

        inline int64_t ReadInt64(absl::string_view src)
        {
            return static_cast<int64_t>(ReadUint64(src));
        }

        constexpr uint64_t kDoubleSignBit = uint64_t{1} << 63;

        inline double NormalizeScore(double score)
        {
            return score == 0.0 ? 0.0 : score;
        }

        inline void AppendBigEndianUint64(std::string& dst, uint64_t value)
        {
            for (int shift = 56; shift >= 0; shift -= 8)
            {
                dst.push_back(static_cast<char>((value >> shift) & 0xff));
            }
        }

        inline uint64_t ReadBigEndianUint64(absl::string_view src)
        {
            uint64_t value = 0;
            for (size_t i = 0; i < kFixedIntSize; ++i)
            {
                value = (value << 8) | static_cast<unsigned char>(src[i]);
            }
            return value;
        }

        inline uint64_t OrderedDoubleBits(double score)
        {
            const auto bits = std::bit_cast<uint64_t>(NormalizeScore(score));
            if ((bits & kDoubleSignBit) != 0)
            {
                return ~bits;
            }
            return bits ^ kDoubleSignBit;
        }

        inline double DoubleFromOrderedBits(uint64_t ordered)
        {
            const auto bits = (ordered & kDoubleSignBit) != 0 ? ordered ^ kDoubleSignBit : ~ordered;
            return std::bit_cast<double>(bits);
        }

        inline void AppendScore(std::string& dst, double score)
        {
            AppendBigEndianUint64(dst, OrderedDoubleBits(score));
        }

        inline double ReadScore(absl::string_view src)
        {
            return DoubleFromOrderedBits(ReadBigEndianUint64(src));
        }
    } // namespace redis_data_key_detail

    // Hash field data key layout: [key][version(8-byte little-endian)][field]
    // Example: HSet("user:1", "name", "alice") stores value at "user:1"|version|"name".
    struct HashDataKey
    {
        absl::string_view key;
        int64_t version;
        absl::string_view field;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + redis_data_key_detail::kFixedIntSize + field.size());
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendInt64(encoded, version);
            encoded.append(field.data(), field.size());
            return encoded;
        }
    };

    // Set member data key layout: [key][version(8-byte little-endian)][member][member_size(8-byte little-endian)]
    // Example: SAdd("tags", "cpp") stores an empty value at "tags"|version|"cpp"|3.
    struct SetDataKey
    {
        absl::string_view key;
        int64_t version;
        absl::string_view member;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + (2 * redis_data_key_detail::kFixedIntSize) + member.size());
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendInt64(encoded, version);
            encoded.append(member.data(), member.size());
            redis_data_key_detail::AppendUint64(encoded, static_cast<uint64_t>(member.size()));
            return encoded;
        }
    };

    // ZSet member data key layout: [key][version(8-byte little-endian)][member]
    // Example: ZAdd("scores", "alice", 42.0) stores score at "scores"|version|"alice".
    struct ZSetMemberDataKey
    {
        absl::string_view key;
        int64_t version;
        absl::string_view member;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + redis_data_key_detail::kFixedIntSize + member.size());
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendInt64(encoded, version);
            encoded.append(member.data(), member.size());
            return encoded;
        }
    };

    // ZSet score data key layout: [key][version(8-byte little-endian)][score(8-byte sortable)][member][member_size(8-byte little-endian)]
    // Example: ZAdd("scores", "alice", 42.0) stores an empty value at "scores"|version|score|"alice"|5.
    struct ZSetScoreDataKey
    {
        absl::string_view key;
        int64_t version;
        double score;
        absl::string_view member;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + (3 * redis_data_key_detail::kFixedIntSize) + member.size());
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendInt64(encoded, version);
            redis_data_key_detail::AppendScore(encoded, score);
            encoded.append(member.data(), member.size());
            redis_data_key_detail::AppendUint64(encoded, static_cast<uint64_t>(member.size()));
            return encoded;
        }
    };

    // List item data key layout: [key][version(8-byte little-endian)][index(8-byte little-endian)]
    // Example: RPush("items", "a") stores value at "items"|version|index.
    struct ListDataKey
    {
        absl::string_view key;
        int64_t version;
        uint64_t index;

        std::string Encode() const
        {
            std::string encoded;
            encoded.reserve(key.size() + (2 * redis_data_key_detail::kFixedIntSize));
            encoded.append(key.data(), key.size());
            redis_data_key_detail::AppendInt64(encoded, version);
            redis_data_key_detail::AppendUint64(encoded, index);
            return encoded;
        }
    };

} // namespace bitcask
