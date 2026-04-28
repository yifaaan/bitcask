#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "db.h"
#include "redis/redis_command.h"

namespace
{
    std::filesystem::path MakeTestDir()
    {
        const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
        return std::filesystem::temp_directory_path() / ("bitcask_redis_command_test_" + std::to_string(suffix));
    }

    template <typename T>
    const T& As(const bitcask::resp::Value& value)
    {
        return std::get<T>(value.data);
    }

    class RedisCommandFixture
    {
    public:
        RedisCommandFixture() : test_dir_(MakeTestDir())
        {
            std::filesystem::remove_all(test_dir_);
            std::filesystem::create_directories(test_dir_);

            db_ = bitcask::DB::Open(bitcask::Options{.data_dir = test_dir_});
            REQUIRE(db_ != nullptr);
            redis_ = std::make_unique<bitcask::RedisDataStruct>(db_.get());
        }

        ~RedisCommandFixture()
        {
            redis_.reset();
            if (db_)
            {
                db_->Close();
            }
            std::filesystem::remove_all(test_dir_);
        }

        bitcask::redis::CommandResult Run(std::vector<std::string> args)
        {
            return bitcask::redis::ExecuteCommand(*redis_, args);
        }

    private:
        std::filesystem::path test_dir_;
        std::unique_ptr<bitcask::DB> db_;
        std::unique_ptr<bitcask::RedisDataStruct> redis_;
    };
} // namespace

TEST_CASE_METHOD(RedisCommandFixture, "Redis command handles ping and echo", "[redis_command]")
{
    auto ping = Run({"PING"});
    REQUIRE(As<bitcask::resp::SimpleString>(ping.reply).value == "PONG");
    REQUIRE_FALSE(ping.close_connection);

    auto ping_payload = Run({"ping", "hello"});
    REQUIRE(As<bitcask::resp::BulkString>(ping_payload.reply).value == "hello");

    auto echo = Run({"ECHO", "world"});
    REQUIRE(As<bitcask::resp::BulkString>(echo.reply).value == "world");
}

TEST_CASE_METHOD(RedisCommandFixture, "Redis command handles string operations", "[redis_command]")
{
    auto missing = Run({"GET", "key"});
    REQUIRE_FALSE(As<bitcask::resp::BulkString>(missing.reply).value.has_value());

    auto set = Run({"SET", "key", "value"});
    REQUIRE(As<bitcask::resp::SimpleString>(set.reply).value == "OK");

    auto get = Run({"GET", "key"});
    REQUIRE(As<bitcask::resp::BulkString>(get.reply).value == "value");

    auto type = Run({"TYPE", "key"});
    REQUIRE(As<bitcask::resp::SimpleString>(type.reply).value == "string");

    auto del = Run({"DEL", "key", "missing"});
    REQUIRE(As<bitcask::resp::Integer>(del.reply).value == 1);

    auto deleted = Run({"GET", "key"});
    REQUIRE_FALSE(As<bitcask::resp::BulkString>(deleted.reply).value.has_value());
}

TEST_CASE_METHOD(RedisCommandFixture, "Redis command handles hash operations", "[redis_command]")
{
    auto insert = Run({"HSET", "user:1", "name", "alice"});
    REQUIRE(As<bitcask::resp::Integer>(insert.reply).value == 1);

    auto update = Run({"HSET", "user:1", "name", "ada"});
    REQUIRE(As<bitcask::resp::Integer>(update.reply).value == 0);

    auto get = Run({"HGET", "user:1", "name"});
    REQUIRE(As<bitcask::resp::BulkString>(get.reply).value == "ada");

    auto del = Run({"HDEL", "user:1", "name"});
    REQUIRE(As<bitcask::resp::Integer>(del.reply).value == 1);

    auto missing = Run({"HGET", "user:1", "name"});
    REQUIRE_FALSE(As<bitcask::resp::BulkString>(missing.reply).value.has_value());
}

TEST_CASE_METHOD(RedisCommandFixture, "Redis command handles list operations", "[redis_command]")
{
    auto push = Run({"LPUSH", "items", "b", "a"});
    REQUIRE(As<bitcask::resp::Integer>(push.reply).value == 2);

    auto len = Run({"LLEN", "items"});
    REQUIRE(As<bitcask::resp::Integer>(len.reply).value == 2);

    auto left = Run({"LPOP", "items"});
    REQUIRE(As<bitcask::resp::BulkString>(left.reply).value == "a");

    auto right = Run({"RPOP", "items"});
    REQUIRE(As<bitcask::resp::BulkString>(right.reply).value == "b");

    auto empty = Run({"LPOP", "items"});
    REQUIRE_FALSE(As<bitcask::resp::BulkString>(empty.reply).value.has_value());
}

TEST_CASE_METHOD(RedisCommandFixture, "Redis command handles set operations", "[redis_command]")
{
    auto add = Run({"SADD", "tags", "cpp", "storage", "cpp"});
    REQUIRE(As<bitcask::resp::Integer>(add.reply).value == 2);

    auto exists = Run({"SISMEMBER", "tags", "cpp"});
    REQUIRE(As<bitcask::resp::Integer>(exists.reply).value == 1);

    auto missing = Run({"SISMEMBER", "tags", "redis"});
    REQUIRE(As<bitcask::resp::Integer>(missing.reply).value == 0);

    auto removed = Run({"SREM", "tags", "cpp", "redis"});
    REQUIRE(As<bitcask::resp::Integer>(removed.reply).value == 1);
}

TEST_CASE_METHOD(RedisCommandFixture, "Redis command handles sorted set operations", "[redis_command]")
{
    auto add = Run({"ZADD", "scores", "1.5", "alice", "2", "bob"});
    REQUIRE(As<bitcask::resp::Integer>(add.reply).value == 2);

    auto update = Run({"ZADD", "scores", "3.25", "alice"});
    REQUIRE(As<bitcask::resp::Integer>(update.reply).value == 0);

    auto score = Run({"ZSCORE", "scores", "alice"});
    REQUIRE(As<bitcask::resp::BulkString>(score.reply).value == "3.25");

    auto removed = Run({"ZREM", "scores", "alice", "nobody"});
    REQUIRE(As<bitcask::resp::Integer>(removed.reply).value == 1);
}

TEST_CASE_METHOD(RedisCommandFixture, "Redis command reports errors and close intent", "[redis_command]")
{
    auto wrong_arity = Run({"GET"});
    REQUIRE(As<bitcask::resp::Error>(wrong_arity.reply).message.find("wrong number") != std::string::npos);

    REQUIRE(As<bitcask::resp::SimpleString>(Run({"SET", "key", "value"}).reply).value == "OK");
    auto wrong_type = Run({"HGET", "key", "field"});
    REQUIRE(As<bitcask::resp::Error>(wrong_type.reply).message.find("WRONGTYPE") != std::string::npos);

    auto quit = Run({"QUIT"});
    REQUIRE(As<bitcask::resp::SimpleString>(quit.reply).value == "OK");
    REQUIRE(quit.close_connection);
}
