#pragma once

#include <absl/container/btree_set.h>
#include <absl/synchronization/mutex.h>
#include <absl/strings/string_view.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstdint>
#include <string>

#include "../db.h"

namespace bitcask
{

    enum class RedisDataType : uint8_t
    {
        kString,
        kHash,
        kSet,
        kList,
        kZSet,
    };
    // Value layout: [type(1 byte)][expiry(varint)][raw value]
    // When expiry == 0, the key has no TTL.
    struct ValueMetadata
    {
        RedisDataType type = RedisDataType::kString;
        int64_t expiry = 0;
    };

    class RedisDataStruct
    {
    public:
        RedisDataStruct() = default;
        explicit RedisDataStruct(DB* db) : db_(db) {}

        RedisDataStruct(const RedisDataStruct&) = delete;
        RedisDataStruct& operator=(const RedisDataStruct&) = delete;
        RedisDataStruct(RedisDataStruct&&) = delete;
        RedisDataStruct& operator=(RedisDataStruct&&) = delete;

        // String operations
        absl::Status Set(absl::string_view key, absl::string_view value, int64_t ttl);
        absl::StatusOr<std::string> Get(absl::string_view key);

        // Common operations
        absl::Status Delete(absl::string_view key);
        absl::StatusOr<RedisDataType> Type(absl::string_view key);

        // Set operations

    private:
        static std::string EncodeValue(RedisDataType type, int64_t expiry, absl::string_view value);
        static std::pair<ValueMetadata, absl::string_view> DecodeValueMetadata(absl::string_view encoded);

        DB* db_;
    };

} // namespace bitcask
