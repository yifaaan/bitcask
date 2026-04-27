#pragma once

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
    // String value layout: [type(1 byte)][expiry(varint)][raw value]
    // Hash metadata layout: [type(1 byte)][expiry(varint)][version(varint)][size(varint)]
    // Hash data key layout: [key][version(varint)][field]
    // When expiry == 0, the key has no TTL.
    struct ValueMetadata
    {
        RedisDataType type = RedisDataType::kString;
        int64_t expiry = 0;
        int64_t version = 0;
        int64_t size = 0;
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

        // Hash operations
        // HSet stores one field in a hash.
        //
        // Example:
        //   HSet("user:1", "name", "alice")
        //   metadata: "user:1" -> [kHash][ttl][version][size]
        //   data:     "user:1"|version|"name" -> "alice"
        absl::Status HSet(
            absl::string_view key,
            absl::string_view field,
            absl::string_view value,
            int64_t ttl = 0);

        // HGet reads one field by loading metadata first, then using the
        // metadata version to build "key|version|field".
        //
        // Example:
        //   HGet("user:1", "name") -> "alice"
        absl::StatusOr<std::string> HGet(absl::string_view key, absl::string_view field);

        // HDel removes one field and updates the hash metadata size in the
        // same WriteBatch.
        //
        // Example:
        //   HDel("user:1", "name")
        //   deletes "user:1"|version|"name"; deletes "user:1" when size is 0.
        absl::Status HDel(absl::string_view key, absl::string_view field);

    private:
        absl::StatusOr<ValueMetadata> LoadHashMetadata(absl::string_view key);

        DB* db_;
    };

} // namespace bitcask
