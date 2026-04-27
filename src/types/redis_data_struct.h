#pragma once

#include <absl/strings/string_view.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstdint>
#include <string>

#include "redis_data_key.h"
#include "../db.h"

namespace bitcask
{

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

        // Set operations
        // SAdd stores one member in a set.
        //
        // Example:
        //   SAdd("tags", "cpp")
        //   metadata: "tags" -> [kSet][ttl][version][size]
        //   data:     "tags"|version|"cpp"|member_size -> ""
        absl::Status SAdd(absl::string_view key, absl::string_view member, int64_t ttl = 0);

        // SIsMember checks one member by loading set metadata first, then using
        // the metadata version to build "key|version|member|member_size".
        //
        // Example:
        //   SIsMember("tags", "cpp") -> true
        absl::StatusOr<bool> SIsMember(absl::string_view key, absl::string_view member);

        // SRem removes one member and updates the set metadata size in the
        // same WriteBatch.
        //
        // Example:
        //   SRem("tags", "cpp")
        //   deletes "tags"|version|"cpp"|member_size; deletes "tags" when size is 0.
        absl::Status SRem(absl::string_view key, absl::string_view member);

    private:
        absl::StatusOr<ValueMetadata> LoadMetadata(absl::string_view key, RedisDataType expected_type);

        DB* db_;
    };

} // namespace bitcask
