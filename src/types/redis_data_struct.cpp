#include "redis_data_struct.h"

#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <iterator>
#include <random>
#include <set>

namespace bitcask
{

    namespace
    {
        std::mt19937& GetRng()
        {
            thread_local std::mt19937 rng{ std::random_device{}() };
            return rng;
        }

        template<typename T>
        T RandomSelect(const std::unordered_set<T>& set)
        {
            std::uniform_int_distribution<size_t> dist(0, set.size() - 1);
            size_t idx = dist(GetRng());
            auto it = set.begin();
            std::advance(it, idx);
            return *it;
        }
    }

    std::string RedisDataStruct::EncodeValue(RedisDataType type, int64_t expiry, absl::string_view value)
    {
        // Layout: [type(1 byte)][expiry(varint)][value]
        std::array<std::byte, 10> varint_buf{};
        const int varint_len = PutVarint(
            absl::Span<std::byte>(varint_buf.data(), varint_buf.size()), expiry);

        std::string encoded;
        encoded.reserve(1 + varint_len + value.size());
        encoded.push_back(static_cast<char>(type));
        encoded.append(reinterpret_cast<const char*>(varint_buf.data()), varint_len);
        encoded.append(value.data(), value.size());

        return encoded;
    }

    std::pair<ValueMetadata, absl::string_view> RedisDataStruct::DecodeValueMetadata(
        absl::string_view encoded)
    {
        if (encoded.size() < 2)
        {
            return { {}, {} };
        }

        ValueMetadata meta;
        meta.type = static_cast<RedisDataType>(encoded[0]);

        const auto remaining = absl::Span<const std::byte>(
            reinterpret_cast<const std::byte*>(encoded.data() + 1),
            encoded.size() - 1);
        const auto [expiry, varint_len] = Varint(remaining);
        meta.expiry = expiry;

        const auto value_offset = 1 + static_cast<size_t>(varint_len);
        return { meta, encoded.substr(value_offset) };
    }

    absl::Status RedisDataStruct::Set(absl::string_view key, absl::string_view value, int64_t ttl)
    {
        int64_t expiry = 0;
        if (ttl > 0)
        {
            const auto now_ns = std::chrono::steady_clock::now().time_since_epoch();
            expiry = std::chrono::duration_cast<std::chrono::nanoseconds>(now_ns).count() + ttl * 1'000'000'000LL;
        }

        return db_->Put(key, EncodeValue(RedisDataType::kString, expiry, value));
    }

    absl::StatusOr<std::string> RedisDataStruct::Get(absl::string_view key)
    {
        auto encoded = db_->Get(key);
        if (!encoded.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Key not found: ", key));
        }

        const auto [meta, raw_value] = DecodeValueMetadata(*encoded);

        if (meta.expiry > 0)
        {
            const auto now_ns = std::chrono::steady_clock::now().time_since_epoch();
            const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(now_ns).count();
            if (now >= meta.expiry)
            {
                // Best-effort cleanup of expired key
                db_->Delete(key);
                return absl::NotFoundError(absl::StrCat("Key expired: ", key));
            }
        }

        return std::string(raw_value);
    }

    absl::Status RedisDataStruct::Delete(absl::string_view key)
    {
        auto encoded = db_->Get(key);
        if (!encoded.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Key not found: ", key));
        }
        return db_->Delete(key);
    }

    absl::StatusOr<RedisDataType> RedisDataStruct::Type(absl::string_view key)
    {
        auto encoded = db_->Get(key);
        if (!encoded.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Key not found: ", key));
        }

        const auto [meta, raw_value] = DecodeValueMetadata(*encoded);

        if (meta.expiry > 0)
        {
            const auto now_ns = std::chrono::steady_clock::now().time_since_epoch();
            const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(now_ns).count();
            if (now >= meta.expiry)
            {
                db_->Delete(key);
                return absl::NotFoundError(absl::StrCat("Key expired: ", key));
            }
        }

        return meta.type;
    }

} // namespace bitcask
