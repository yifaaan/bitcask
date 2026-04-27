#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>
#include <thread>

#include "types/redis_data_struct.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_redis_ds";

    struct RedisDSFixture
    {
        RedisDSFixture()
        {
            std::filesystem::remove_all(kTestDir);
            std::filesystem::create_directories(kTestDir);
        }
        ~RedisDSFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }
    };
} // namespace

// --- Get ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Get returns value after Set", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("key1", "hello", 0).ok());

    auto result = rds.Get("key1");
    REQUIRE(result.ok());
    REQUIRE(*result == "hello");
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Get returns NotFound for nonexistent key", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    auto result = rds.Get("no_such_key");
    REQUIRE_FALSE(result.ok());
    REQUIRE(absl::IsNotFound(result.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Get returns updated value after overwrite", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("key1", "v1", 0).ok());
    REQUIRE(rds.Set("key1", "v2", 0).ok());

    auto result = rds.Get("key1");
    REQUIRE(result.ok());
    REQUIRE(*result == "v2");
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Get returns NotFound after Delete", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("key1", "value", 0).ok());
    REQUIRE(rds.Delete("key1").ok());

    auto result = rds.Get("key1");
    REQUIRE_FALSE(result.ok());
    REQUIRE(absl::IsNotFound(result.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Get with TTL expires key", "[redis_ds][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("ephemeral", "data", 1).ok());

    // Should be available immediately
    auto before = rds.Get("ephemeral");
    REQUIRE(before.ok());
    REQUIRE(*before == "data");

    // Wait for TTL to expire (1 second + margin)
    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto after = rds.Get("ephemeral");
    REQUIRE_FALSE(after.ok());
    REQUIRE(absl::IsNotFound(after.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Get with zero TTL never expires", "[redis_ds][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("permanent", "data", 0).ok());

    auto result = rds.Get("permanent");
    REQUIRE(result.ok());
    REQUIRE(*result == "data");
}

// --- Delete ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Delete removes existing key", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("key1", "value1", 0).ok());
    REQUIRE(rds.Delete("key1").ok());

    auto result = rds.Get("key1");
    REQUIRE_FALSE(result.ok());
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Delete returns ok for nonexistent key", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    auto status = rds.Delete("no_such_key");
    REQUIRE(status.ok());
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Delete only affects targeted key", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("a", "1", 0).ok());
    REQUIRE(rds.Set("b", "2", 0).ok());

    REQUIRE(rds.Delete("a").ok());

    REQUIRE_FALSE(rds.Get("a").ok());
    auto b = rds.Get("b");
    REQUIRE(b.ok());
    REQUIRE(*b == "2");
}

// --- Type ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Type returns kString after Set", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("key1", "value", 0).ok());

    auto result = rds.Type("key1");
    REQUIRE(result.ok());
    REQUIRE(*result == bitcask::RedisDataType::kString);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Type returns NotFound for nonexistent key", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    auto result = rds.Type("no_such_key");
    REQUIRE_FALSE(result.ok());
    REQUIRE(absl::IsNotFound(result.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Type returns NotFound after Delete", "[redis_ds]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("key1", "value", 0).ok());
    REQUIRE(rds.Delete("key1").ok());

    auto result = rds.Type("key1");
    REQUIRE_FALSE(result.ok());
    REQUIRE(absl::IsNotFound(result.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Type with TTL expires key", "[redis_ds][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("ephemeral", "data", 1).ok());

    auto before = rds.Type("ephemeral");
    REQUIRE(before.ok());
    REQUIRE(*before == bitcask::RedisDataType::kString);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto after = rds.Type("ephemeral");
    REQUIRE_FALSE(after.ok());
    REQUIRE(absl::IsNotFound(after.status()));
}

// --- Hash ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS HGet returns value after HSet", "[redis_ds][hash]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.HSet("user:1", "name", "alice").ok());

    auto value = rds.HGet("user:1", "name");
    REQUIRE(value.ok());
    REQUIRE(*value == "alice");

    auto type = rds.Type("user:1");
    REQUIRE(type.ok());
    REQUIRE(*type == bitcask::RedisDataType::kHash);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS HSet updates existing hash field", "[redis_ds][hash]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.HSet("user:1", "name", "alice").ok());
    REQUIRE(rds.HSet("user:1", "name", "bob").ok());

    auto value = rds.HGet("user:1", "name");
    REQUIRE(value.ok());
    REQUIRE(*value == "bob");

    REQUIRE(rds.HDel("user:1", "name").ok());
    auto type = rds.Type("user:1");
    REQUIRE_FALSE(type.ok());
    REQUIRE(absl::IsNotFound(type.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS HDel removes one hash field", "[redis_ds][hash]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.HSet("user:1", "name", "alice").ok());
    REQUIRE(rds.HSet("user:1", "email", "alice@example.com").ok());

    REQUIRE(rds.HDel("user:1", "name").ok());

    auto deleted = rds.HGet("user:1", "name");
    REQUIRE_FALSE(deleted.ok());
    REQUIRE(absl::IsNotFound(deleted.status()));

    auto remaining = rds.HGet("user:1", "email");
    REQUIRE(remaining.ok());
    REQUIRE(*remaining == "alice@example.com");
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Hash delete isolates old fields by version", "[redis_ds][hash]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.HSet("user:1", "name", "alice").ok());
    REQUIRE(rds.Delete("user:1").ok());
    REQUIRE(rds.HSet("user:1", "email", "alice@example.com").ok());

    auto old_field = rds.HGet("user:1", "name");
    REQUIRE_FALSE(old_field.ok());
    REQUIRE(absl::IsNotFound(old_field.status()));

    auto new_field = rds.HGet("user:1", "email");
    REQUIRE(new_field.ok());
    REQUIRE(*new_field == "alice@example.com");
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Hash rejects string key", "[redis_ds][hash]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("user:1", "raw", 0).ok());

    auto hset = rds.HSet("user:1", "name", "alice");
    REQUIRE_FALSE(hset.ok());
    REQUIRE(absl::IsFailedPrecondition(hset));

    auto hget = rds.HGet("user:1", "name");
    REQUIRE_FALSE(hget.ok());
    REQUIRE(absl::IsFailedPrecondition(hget.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Hash with TTL expires metadata and fields", "[redis_ds][hash][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.HSet("session:1", "token", "abc", 1).ok());

    auto before = rds.HGet("session:1", "token");
    REQUIRE(before.ok());
    REQUIRE(*before == "abc");

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto after = rds.HGet("session:1", "token");
    REQUIRE_FALSE(after.ok());
    REQUIRE(absl::IsNotFound(after.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Hash survives reopen", "[redis_ds][hash]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        REQUIRE(rds.HSet("user:1", "name", "alice").ok());
        REQUIRE(rds.HSet("user:1", "email", "alice@example.com").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto name = rds.HGet("user:1", "name");
        REQUIRE(name.ok());
        REQUIRE(*name == "alice");

        REQUIRE(rds.HDel("user:1", "name").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto name = rds.HGet("user:1", "name");
        REQUIRE_FALSE(name.ok());
        REQUIRE(absl::IsNotFound(name.status()));

        auto email = rds.HGet("user:1", "email");
        REQUIRE(email.ok());
        REQUIRE(*email == "alice@example.com");
    }
}
