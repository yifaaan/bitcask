#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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

// --- Set ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS SIsMember returns true after SAdd", "[redis_ds][set]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.SAdd("tags", "cpp").ok());

    auto is_member = rds.SIsMember("tags", "cpp");
    REQUIRE(is_member.ok());
    REQUIRE(*is_member);

    auto missing = rds.SIsMember("tags", "go");
    REQUIRE(missing.ok());
    REQUIRE_FALSE(*missing);

    auto type = rds.Type("tags");
    REQUIRE(type.ok());
    REQUIRE(*type == bitcask::RedisDataType::kSet);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS SAdd is idempotent for duplicate member", "[redis_ds][set]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.SAdd("tags", "cpp").ok());
    REQUIRE(rds.SAdd("tags", "cpp").ok());

    REQUIRE(rds.SRem("tags", "cpp").ok());
    auto type = rds.Type("tags");
    REQUIRE_FALSE(type.ok());
    REQUIRE(absl::IsNotFound(type.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS SRem removes one set member", "[redis_ds][set]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.SAdd("tags", "cpp").ok());
    REQUIRE(rds.SAdd("tags", "storage").ok());

    REQUIRE(rds.SRem("tags", "cpp").ok());

    auto removed = rds.SIsMember("tags", "cpp");
    REQUIRE(removed.ok());
    REQUIRE_FALSE(*removed);

    auto remaining = rds.SIsMember("tags", "storage");
    REQUIRE(remaining.ok());
    REQUIRE(*remaining);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Set delete isolates old members by version", "[redis_ds][set]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.SAdd("tags", "cpp").ok());
    REQUIRE(rds.Delete("tags").ok());
    REQUIRE(rds.SAdd("tags", "go").ok());

    auto old_member = rds.SIsMember("tags", "cpp");
    REQUIRE(old_member.ok());
    REQUIRE_FALSE(*old_member);

    auto new_member = rds.SIsMember("tags", "go");
    REQUIRE(new_member.ok());
    REQUIRE(*new_member);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Set rejects string key", "[redis_ds][set]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("tags", "raw", 0).ok());

    auto sadd = rds.SAdd("tags", "cpp");
    REQUIRE_FALSE(sadd.ok());
    REQUIRE(absl::IsFailedPrecondition(sadd));

    auto is_member = rds.SIsMember("tags", "cpp");
    REQUIRE_FALSE(is_member.ok());
    REQUIRE(absl::IsFailedPrecondition(is_member.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Set with TTL expires metadata and members", "[redis_ds][set][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.SAdd("tags", "cpp", 1).ok());

    auto before = rds.SIsMember("tags", "cpp");
    REQUIRE(before.ok());
    REQUIRE(*before);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto after = rds.SIsMember("tags", "cpp");
    REQUIRE(after.ok());
    REQUIRE_FALSE(*after);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Set survives reopen", "[redis_ds][set]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        REQUIRE(rds.SAdd("tags", "cpp").ok());
        REQUIRE(rds.SAdd("tags", "storage").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto cpp = rds.SIsMember("tags", "cpp");
        REQUIRE(cpp.ok());
        REQUIRE(*cpp);

        REQUIRE(rds.SRem("tags", "cpp").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto cpp = rds.SIsMember("tags", "cpp");
        REQUIRE(cpp.ok());
        REQUIRE_FALSE(*cpp);

        auto storage = rds.SIsMember("tags", "storage");
        REQUIRE(storage.ok());
        REQUIRE(*storage);
    }
}

// --- Sorted Set ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS ZScore returns score after ZAdd", "[redis_ds][zset]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.ZAdd("scores", "alice", 42.5).ok());

    auto score = rds.ZScore("scores", "alice");
    REQUIRE(score.ok());
    REQUIRE(*score == 42.5);

    auto missing = rds.ZScore("scores", "bob");
    REQUIRE_FALSE(missing.ok());
    REQUIRE(absl::IsNotFound(missing.status()));

    auto type = rds.Type("scores");
    REQUIRE(type.ok());
    REQUIRE(*type == bitcask::RedisDataType::kZSet);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS ZAdd updates existing member score", "[redis_ds][zset]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.ZAdd("scores", "alice", 1.0).ok());
    REQUIRE(rds.ZAdd("scores", "alice", 2.0).ok());

    auto score = rds.ZScore("scores", "alice");
    REQUIRE(score.ok());
    REQUIRE(*score == 2.0);

    REQUIRE(rds.ZRem("scores", "alice").ok());
    auto type = rds.Type("scores");
    REQUIRE_FALSE(type.ok());
    REQUIRE(absl::IsNotFound(type.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS ZRem removes one sorted set member", "[redis_ds][zset]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.ZAdd("scores", "alice", 1.0).ok());
    REQUIRE(rds.ZAdd("scores", "bob", 2.0).ok());

    REQUIRE(rds.ZRem("scores", "alice").ok());

    auto removed = rds.ZScore("scores", "alice");
    REQUIRE_FALSE(removed.ok());
    REQUIRE(absl::IsNotFound(removed.status()));

    auto remaining = rds.ZScore("scores", "bob");
    REQUIRE(remaining.ok());
    REQUIRE(*remaining == 2.0);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Sorted Set delete isolates old members by version", "[redis_ds][zset]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.ZAdd("scores", "alice", 1.0).ok());
    REQUIRE(rds.Delete("scores").ok());
    REQUIRE(rds.ZAdd("scores", "bob", 2.0).ok());

    auto old_member = rds.ZScore("scores", "alice");
    REQUIRE_FALSE(old_member.ok());
    REQUIRE(absl::IsNotFound(old_member.status()));

    auto new_member = rds.ZScore("scores", "bob");
    REQUIRE(new_member.ok());
    REQUIRE(*new_member == 2.0);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Sorted Set rejects string key", "[redis_ds][zset]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("scores", "raw", 0).ok());

    auto zadd = rds.ZAdd("scores", "alice", 1.0);
    REQUIRE_FALSE(zadd.ok());
    REQUIRE(absl::IsFailedPrecondition(zadd));

    auto score = rds.ZScore("scores", "alice");
    REQUIRE_FALSE(score.ok());
    REQUIRE(absl::IsFailedPrecondition(score.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Sorted Set with TTL expires metadata and scores", "[redis_ds][zset][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.ZAdd("scores", "alice", 1.5, 1).ok());

    auto before = rds.ZScore("scores", "alice");
    REQUIRE(before.ok());
    REQUIRE(*before == 1.5);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto after = rds.ZScore("scores", "alice");
    REQUIRE_FALSE(after.ok());
    REQUIRE(absl::IsNotFound(after.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS Sorted Set survives reopen", "[redis_ds][zset]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        REQUIRE(rds.ZAdd("scores", "alice", 1.0).ok());
        REQUIRE(rds.ZAdd("scores", "bob", 2.0).ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto alice = rds.ZScore("scores", "alice");
        REQUIRE(alice.ok());
        REQUIRE(*alice == 1.0);

        REQUIRE(rds.ZRem("scores", "alice").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto alice = rds.ZScore("scores", "alice");
        REQUIRE_FALSE(alice.ok());
        REQUIRE(absl::IsNotFound(alice.status()));

        auto bob = rds.ZScore("scores", "bob");
        REQUIRE(bob.ok());
        REQUIRE(*bob == 2.0);
    }
}

// --- List ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS List push pop and len", "[redis_ds][list]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.LPush("items", "b").ok());
    REQUIRE(rds.LPush("items", "a").ok());
    REQUIRE(rds.RPush("items", "c").ok());

    auto len = rds.LLen("items");
    REQUIRE(len.ok());
    REQUIRE(*len == 3);

    auto type = rds.Type("items");
    REQUIRE(type.ok());
    REQUIRE(*type == bitcask::RedisDataType::kList);

    auto left = rds.LPop("items");
    REQUIRE(left.ok());
    REQUIRE(*left == "a");

    auto right = rds.RPop("items");
    REQUIRE(right.ok());
    REQUIRE(*right == "c");

    auto last = rds.LPop("items");
    REQUIRE(last.ok());
    REQUIRE(*last == "b");

    auto empty_len = rds.LLen("items");
    REQUIRE(empty_len.ok());
    REQUIRE(*empty_len == 0);

    auto missing = rds.LPop("items");
    REQUIRE_FALSE(missing.ok());
    REQUIRE(absl::IsNotFound(missing.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS List pop returns NotFound for missing key", "[redis_ds][list]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    auto left = rds.LPop("items");
    REQUIRE_FALSE(left.ok());
    REQUIRE(absl::IsNotFound(left.status()));

    auto right = rds.RPop("items");
    REQUIRE_FALSE(right.ok());
    REQUIRE(absl::IsNotFound(right.status()));

    auto len = rds.LLen("items");
    REQUIRE(len.ok());
    REQUIRE(*len == 0);
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS List delete isolates old items by version", "[redis_ds][list]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.RPush("items", "old").ok());
    REQUIRE(rds.Delete("items").ok());
    REQUIRE(rds.RPush("items", "new").ok());

    auto value = rds.LPop("items");
    REQUIRE(value.ok());
    REQUIRE(*value == "new");

    auto missing = rds.LPop("items");
    REQUIRE_FALSE(missing.ok());
    REQUIRE(absl::IsNotFound(missing.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS List rejects string key", "[redis_ds][list]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("items", "raw", 0).ok());

    auto push = rds.RPush("items", "value");
    REQUIRE_FALSE(push.ok());
    REQUIRE(absl::IsFailedPrecondition(push));

    auto pop = rds.LPop("items");
    REQUIRE_FALSE(pop.ok());
    REQUIRE(absl::IsFailedPrecondition(pop.status()));

    auto len = rds.LLen("items");
    REQUIRE_FALSE(len.ok());
    REQUIRE(absl::IsFailedPrecondition(len.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS List with TTL expires metadata and items", "[redis_ds][list][ttl]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.RPush("items", "value", 1).ok());

    auto before = rds.LLen("items");
    REQUIRE(before.ok());
    REQUIRE(*before == 1);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto after_len = rds.LLen("items");
    REQUIRE(after_len.ok());
    REQUIRE(*after_len == 0);

    auto after_pop = rds.LPop("items");
    REQUIRE_FALSE(after_pop.ok());
    REQUIRE(absl::IsNotFound(after_pop.status()));
}

TEST_CASE_METHOD(RedisDSFixture, "RedisDS List survives reopen", "[redis_ds][list]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        REQUIRE(rds.RPush("items", "a").ok());
        REQUIRE(rds.RPush("items", "b").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto first = rds.LPop("items");
        REQUIRE(first.ok());
        REQUIRE(*first == "a");
        REQUIRE(rds.RPush("items", "c").ok());
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        bitcask::RedisDataStruct rds(db.get());
        auto second = rds.LPop("items");
        REQUIRE(second.ok());
        REQUIRE(*second == "b");

        auto third = rds.LPop("items");
        REQUIRE(third.ok());
        REQUIRE(*third == "c");
    }
}

// --- Fixed-size Redis numeric encoding ---

TEST_CASE_METHOD(RedisDSFixture, "RedisDS fixed numeric encoding uses 8 byte fields", "[redis_ds][encoding]")
{
    constexpr size_t kFixedIntSize = sizeof(uint64_t);
    constexpr size_t kTypeSize = 1;

    auto read_uint64 = [](const std::string& data, size_t offset) {
        uint64_t value = 0;
        std::memcpy(&value, data.data() + offset, sizeof(value));
        return value;
    };
    auto read_int64 = [&](const std::string& data, size_t offset) {
        return static_cast<int64_t>(read_uint64(data, offset));
    };

    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::RedisDataStruct rds(db.get());
    REQUIRE(rds.Set("raw:string", "value", 0).ok());
    REQUIRE(rds.HSet("raw:hash", "field", "value").ok());
    REQUIRE(rds.SAdd("raw:set", "member").ok());
    REQUIRE(rds.ZAdd("raw:zset", "member", 3.5).ok());
    REQUIRE(rds.RPush("raw:list", "first").ok());

    auto string_value = db->Get("raw:string");
    REQUIRE(string_value.has_value());
    REQUIRE(string_value->size() == kTypeSize + kFixedIntSize + 5);
    REQUIRE(static_cast<bitcask::RedisDataType>((*string_value)[0]) == bitcask::RedisDataType::kString);
    REQUIRE(read_int64(*string_value, kTypeSize) == 0);

    auto hash_meta = db->Get("raw:hash");
    REQUIRE(hash_meta.has_value());
    REQUIRE(hash_meta->size() == kTypeSize + (3 * kFixedIntSize));
    REQUIRE(static_cast<bitcask::RedisDataType>((*hash_meta)[0]) == bitcask::RedisDataType::kHash);
    const auto hash_version = read_int64(*hash_meta, kTypeSize + kFixedIntSize);
    REQUIRE(hash_version > 0);
    REQUIRE(read_int64(*hash_meta, kTypeSize + (2 * kFixedIntSize)) == 1);

    auto set_meta = db->Get("raw:set");
    REQUIRE(set_meta.has_value());
    REQUIRE(set_meta->size() == kTypeSize + (3 * kFixedIntSize));
    REQUIRE(static_cast<bitcask::RedisDataType>((*set_meta)[0]) == bitcask::RedisDataType::kSet);
    const auto set_version = read_int64(*set_meta, kTypeSize + kFixedIntSize);
    REQUIRE(set_version > 0);
    REQUIRE(read_int64(*set_meta, kTypeSize + (2 * kFixedIntSize)) == 1);

    auto zset_meta = db->Get("raw:zset");
    REQUIRE(zset_meta.has_value());
    REQUIRE(zset_meta->size() == kTypeSize + (3 * kFixedIntSize));
    REQUIRE(static_cast<bitcask::RedisDataType>((*zset_meta)[0]) == bitcask::RedisDataType::kZSet);
    const auto zset_version = read_int64(*zset_meta, kTypeSize + kFixedIntSize);
    REQUIRE(zset_version > 0);
    REQUIRE(read_int64(*zset_meta, kTypeSize + (2 * kFixedIntSize)) == 1);

    auto list_meta = db->Get("raw:list");
    REQUIRE(list_meta.has_value());
    REQUIRE(list_meta->size() == kTypeSize + (5 * kFixedIntSize));
    REQUIRE(static_cast<bitcask::RedisDataType>((*list_meta)[0]) == bitcask::RedisDataType::kList);
    const auto list_version = read_int64(*list_meta, kTypeSize + kFixedIntSize);
    REQUIRE(list_version > 0);
    REQUIRE(read_int64(*list_meta, kTypeSize + (2 * kFixedIntSize)) == 1);
    const auto list_head = read_uint64(*list_meta, kTypeSize + (3 * kFixedIntSize));
    const auto list_tail = read_uint64(*list_meta, kTypeSize + (4 * kFixedIntSize));
    REQUIRE(list_tail - list_head == 1);

    std::vector<std::string> keys;
    REQUIRE(db->Fold([&](std::string_view key, std::string) {
        keys.emplace_back(key);
        return true;
    }).ok());

    bool found_hash_data_key = false;
    bool found_set_data_key = false;
    bool found_zset_member_data_key = false;
    bool found_zset_score_data_key = false;
    bool found_list_data_key = false;
    for (const auto& key : keys)
    {
        if (key.rfind("raw:hash", 0) == 0 && key != "raw:hash")
        {
            found_hash_data_key = true;
            REQUIRE(key.size() == std::string("raw:hash").size() + kFixedIntSize + std::string("field").size());
            REQUIRE(read_int64(key, std::string("raw:hash").size()) == hash_version);
        }
        if (key.rfind("raw:set", 0) == 0 && key != "raw:set")
        {
            found_set_data_key = true;
            REQUIRE(key.size() == std::string("raw:set").size() + kFixedIntSize + std::string("member").size() + kFixedIntSize);
            REQUIRE(read_int64(key, std::string("raw:set").size()) == set_version);
            REQUIRE(read_uint64(key, key.size() - kFixedIntSize) == std::string("member").size());
        }
        if (key.rfind("raw:zset", 0) == 0 && key != "raw:zset")
        {
            const auto raw_zset_size = std::string("raw:zset").size();
            if (key.size() == raw_zset_size + kFixedIntSize + std::string("member").size())
            {
                found_zset_member_data_key = true;
                REQUIRE(read_int64(key, raw_zset_size) == zset_version);
                auto value = db->Get(key);
                REQUIRE(value.has_value());
                REQUIRE(value->size() == kFixedIntSize);
            }
            if (key.size() == raw_zset_size + (3 * kFixedIntSize) + std::string("member").size())
            {
                found_zset_score_data_key = true;
                REQUIRE(read_int64(key, raw_zset_size) == zset_version);
                REQUIRE(read_uint64(key, key.size() - kFixedIntSize) == std::string("member").size());
                auto value = db->Get(key);
                REQUIRE(value.has_value());
                REQUIRE(value->empty());
            }
        }
        if (key.rfind("raw:list", 0) == 0 && key != "raw:list")
        {
            found_list_data_key = true;
            REQUIRE(key.size() == std::string("raw:list").size() + (2 * kFixedIntSize));
            REQUIRE(read_int64(key, std::string("raw:list").size()) == list_version);
            REQUIRE(read_uint64(key, std::string("raw:list").size() + kFixedIntSize) == list_head);
        }
    }

    REQUIRE(found_hash_data_key);
    REQUIRE(found_set_data_key);
    REQUIRE(found_zset_member_data_key);
    REQUIRE(found_zset_score_data_key);
    REQUIRE(found_list_data_key);
}
