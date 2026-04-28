#include "redis/redis_command.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <format>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>

namespace bitcask::redis
{
    namespace
    {
        CommandResult Reply(resp::Value value, bool close_connection = false)
        {
            return CommandResult{std::move(value), close_connection};
        }

        CommandResult Simple(std::string value)
        {
            return Reply(resp::Simple(std::move(value)));
        }

        CommandResult Bulk(std::string value)
        {
            return Reply(resp::Bulk(std::move(value)));
        }

        CommandResult NullBulk()
        {
            return Reply(resp::NullBulk());
        }

        CommandResult Integer(int64_t value)
        {
            return Reply(resp::Int(value));
        }

        CommandResult Error(std::string message)
        {
            return Reply(resp::Err(std::move(message)));
        }

        std::string ToUpper(std::string_view value)
        {
            std::string out(value);
            std::ranges::transform(out, out.begin(), [](unsigned char ch) {
                return static_cast<char>(std::toupper(ch));
            });
            return out;
        }

        std::string ToLower(std::string_view value)
        {
            std::string out(value);
            std::ranges::transform(out, out.begin(), [](unsigned char ch) {
                return static_cast<char>(std::tolower(ch));
            });
            return out;
        }

        CommandResult WrongArity(std::string_view command)
        {
            return Error(std::format("ERR wrong number of arguments for '{}' command", ToLower(command)));
        }

        CommandResult SyntaxError()
        {
            return Error("ERR syntax error");
        }

        CommandResult UnknownCommand(std::string_view command)
        {
            return Error(std::format("ERR unknown command '{}'", ToLower(command)));
        }

        CommandResult StatusError(const absl::Status& status)
        {
            if (absl::IsFailedPrecondition(status))
            {
                return Error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            if (status.message().empty())
            {
                return Error("ERR operation failed");
            }
            return Error(std::format("ERR {}", status.message()));
        }

        absl::StatusOr<int64_t> ParseInt64(std::string_view value)
        {
            if (value.empty())
            {
                return absl::InvalidArgumentError("value is not an integer");
            }
            if (value.front() == '+')
            {
                value.remove_prefix(1);
                if (value.empty())
                {
                    return absl::InvalidArgumentError("value is not an integer");
                }
            }

            int64_t out = 0;
            const auto* begin = value.data();
            const auto* end = value.data() + value.size();
            const auto [ptr, ec] = std::from_chars(begin, end, out);
            if (ec != std::errc() || ptr != end)
            {
                return absl::InvalidArgumentError("value is not an integer");
            }
            return out;
        }

        absl::StatusOr<double> ParseDouble(std::string_view value)
        {
            if (value.empty())
            {
                return absl::InvalidArgumentError("value is not a valid float");
            }
            if (value.front() == '+')
            {
                value.remove_prefix(1);
                if (value.empty())
                {
                    return absl::InvalidArgumentError("value is not a valid float");
                }
            }

            double out = 0.0;
            const auto* begin = value.data();
            const auto* end = value.data() + value.size();
            const auto [ptr, ec] = std::from_chars(begin, end, out);
            if (ec != std::errc() || ptr != end || std::isnan(out))
            {
                return absl::InvalidArgumentError("value is not a valid float");
            }
            return out;
        }

        CommandResult IntegerParseError()
        {
            return Error("ERR value is not an integer or out of range");
        }

        CommandResult FloatParseError()
        {
            return Error("ERR value is not a valid float");
        }

        std::string FormatDouble(double value)
        {
            std::ostringstream out;
            out << std::setprecision(17) << value;
            return out.str();
        }

        std::string TypeName(RedisDataType type)
        {
            switch (type)
            {
            case RedisDataType::kString:
                return "string";
            case RedisDataType::kHash:
                return "hash";
            case RedisDataType::kSet:
                return "set";
            case RedisDataType::kList:
                return "list";
            case RedisDataType::kZSet:
                return "zset";
            }
            return "none";
        }

        CommandResult Ping(const std::vector<std::string>& args)
        {
            if (args.size() == 1)
            {
                return Simple("PONG");
            }
            if (args.size() == 2)
            {
                return Bulk(args[1]);
            }
            return WrongArity(args[0]);
        }

        CommandResult Set(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 3 && args.size() != 5)
            {
                return WrongArity(args[0]);
            }

            int64_t ttl = 0;
            if (args.size() == 5)
            {
                const auto option = ToUpper(args[3]);
                auto parsed_ttl = ParseInt64(args[4]);
                if (!parsed_ttl.ok())
                {
                    return IntegerParseError();
                }
                if (*parsed_ttl <= 0)
                {
                    return Error("ERR invalid expire time in 'set' command");
                }

                if (option == "EX")
                {
                    ttl = *parsed_ttl;
                }
                else if (option == "PX")
                {
                    ttl = (*parsed_ttl + 999) / 1000;
                    if (ttl <= 0)
                    {
                        ttl = 1;
                    }
                }
                else
                {
                    return SyntaxError();
                }
            }

            auto status = redis.Set(args[1], args[2], ttl);
            if (!status.ok())
            {
                return StatusError(status);
            }
            return Simple("OK");
        }

        CommandResult Get(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 2)
            {
                return WrongArity(args[0]);
            }

            auto value = redis.Get(args[1]);
            if (value.ok())
            {
                return Bulk(*value);
            }
            if (absl::IsNotFound(value.status()))
            {
                return NullBulk();
            }
            return StatusError(value.status());
        }

        CommandResult Del(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() < 2)
            {
                return WrongArity(args[0]);
            }

            int64_t removed = 0;
            for (size_t i = 1; i < args.size(); ++i)
            {
                auto type = redis.Type(args[i]);
                if (!type.ok())
                {
                    if (absl::IsNotFound(type.status()))
                    {
                        continue;
                    }
                    return StatusError(type.status());
                }
                auto status = redis.Delete(args[i]);
                if (!status.ok())
                {
                    return StatusError(status);
                }
                ++removed;
            }
            return Integer(removed);
        }

        CommandResult Type(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 2)
            {
                return WrongArity(args[0]);
            }

            auto type = redis.Type(args[1]);
            if (type.ok())
            {
                return Simple(TypeName(*type));
            }
            if (absl::IsNotFound(type.status()))
            {
                return Simple("none");
            }
            return StatusError(type.status());
        }

        CommandResult HSet(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 4)
            {
                return WrongArity(args[0]);
            }

            auto old_value = redis.HGet(args[1], args[2]);
            if (!old_value.ok() && !absl::IsNotFound(old_value.status()))
            {
                return StatusError(old_value.status());
            }
            const bool is_new = !old_value.ok();
            auto status = redis.HSet(args[1], args[2], args[3]);
            if (!status.ok())
            {
                return StatusError(status);
            }
            return Integer(is_new ? 1 : 0);
        }

        CommandResult HGet(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 3)
            {
                return WrongArity(args[0]);
            }

            auto value = redis.HGet(args[1], args[2]);
            if (value.ok())
            {
                return Bulk(*value);
            }
            if (absl::IsNotFound(value.status()))
            {
                return NullBulk();
            }
            return StatusError(value.status());
        }

        CommandResult HDel(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 3)
            {
                return WrongArity(args[0]);
            }

            auto old_value = redis.HGet(args[1], args[2]);
            if (!old_value.ok() && !absl::IsNotFound(old_value.status()))
            {
                return StatusError(old_value.status());
            }
            auto status = redis.HDel(args[1], args[2]);
            if (!status.ok())
            {
                return StatusError(status);
            }
            return Integer(old_value.ok() ? 1 : 0);
        }

        CommandResult Push(RedisDataStruct& redis, const std::vector<std::string>& args, bool left)
        {
            if (args.size() < 3)
            {
                return WrongArity(args[0]);
            }

            for (size_t i = 2; i < args.size(); ++i)
            {
                auto status = left ? redis.LPush(args[1], args[i]) : redis.RPush(args[1], args[i]);
                if (!status.ok())
                {
                    return StatusError(status);
                }
            }

            auto len = redis.LLen(args[1]);
            if (!len.ok())
            {
                return StatusError(len.status());
            }
            return Integer(*len);
        }

        CommandResult Pop(RedisDataStruct& redis, const std::vector<std::string>& args, bool left)
        {
            if (args.size() != 2)
            {
                return WrongArity(args[0]);
            }

            auto value = left ? redis.LPop(args[1]) : redis.RPop(args[1]);
            if (value.ok())
            {
                return Bulk(*value);
            }
            if (absl::IsNotFound(value.status()))
            {
                return NullBulk();
            }
            return StatusError(value.status());
        }

        CommandResult LLen(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 2)
            {
                return WrongArity(args[0]);
            }

            auto len = redis.LLen(args[1]);
            if (!len.ok())
            {
                return StatusError(len.status());
            }
            return Integer(*len);
        }

        CommandResult SAdd(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() < 3)
            {
                return WrongArity(args[0]);
            }

            int64_t added = 0;
            for (size_t i = 2; i < args.size(); ++i)
            {
                auto exists = redis.SIsMember(args[1], args[i]);
                if (!exists.ok())
                {
                    return StatusError(exists.status());
                }
                auto status = redis.SAdd(args[1], args[i]);
                if (!status.ok())
                {
                    return StatusError(status);
                }
                if (!*exists)
                {
                    ++added;
                }
            }
            return Integer(added);
        }

        CommandResult SIsMember(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 3)
            {
                return WrongArity(args[0]);
            }

            auto exists = redis.SIsMember(args[1], args[2]);
            if (!exists.ok())
            {
                return StatusError(exists.status());
            }
            return Integer(*exists ? 1 : 0);
        }

        CommandResult SRem(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() < 3)
            {
                return WrongArity(args[0]);
            }

            int64_t removed = 0;
            for (size_t i = 2; i < args.size(); ++i)
            {
                auto exists = redis.SIsMember(args[1], args[i]);
                if (!exists.ok())
                {
                    return StatusError(exists.status());
                }
                auto status = redis.SRem(args[1], args[i]);
                if (!status.ok())
                {
                    return StatusError(status);
                }
                if (*exists)
                {
                    ++removed;
                }
            }
            return Integer(removed);
        }

        CommandResult ZAdd(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() < 4 || args.size() % 2 != 0)
            {
                return WrongArity(args[0]);
            }

            int64_t added = 0;
            for (size_t i = 2; i < args.size(); i += 2)
            {
                auto score = ParseDouble(args[i]);
                if (!score.ok())
                {
                    return FloatParseError();
                }
                auto old_score = redis.ZScore(args[1], args[i + 1]);
                if (!old_score.ok() && !absl::IsNotFound(old_score.status()))
                {
                    return StatusError(old_score.status());
                }
                auto status = redis.ZAdd(args[1], args[i + 1], *score);
                if (!status.ok())
                {
                    return StatusError(status);
                }
                if (!old_score.ok())
                {
                    ++added;
                }
            }
            return Integer(added);
        }

        CommandResult ZScore(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() != 3)
            {
                return WrongArity(args[0]);
            }

            auto score = redis.ZScore(args[1], args[2]);
            if (score.ok())
            {
                return Bulk(FormatDouble(*score));
            }
            if (absl::IsNotFound(score.status()))
            {
                return NullBulk();
            }
            return StatusError(score.status());
        }

        CommandResult ZRem(RedisDataStruct& redis, const std::vector<std::string>& args)
        {
            if (args.size() < 3)
            {
                return WrongArity(args[0]);
            }

            int64_t removed = 0;
            for (size_t i = 2; i < args.size(); ++i)
            {
                auto score = redis.ZScore(args[1], args[i]);
                if (!score.ok() && !absl::IsNotFound(score.status()))
                {
                    return StatusError(score.status());
                }
                auto status = redis.ZRem(args[1], args[i]);
                if (!status.ok())
                {
                    return StatusError(status);
                }
                if (score.ok())
                {
                    ++removed;
                }
            }
            return Integer(removed);
        }

        CommandResult Client(const std::vector<std::string>& args)
        {
            if (args.size() >= 2 && ToUpper(args[1]) == "SETINFO")
            {
                return Simple("OK");
            }
            return UnknownCommand(args[0]);
        }

        CommandResult Select(const std::vector<std::string>& args)
        {
            if (args.size() != 2)
            {
                return WrongArity(args[0]);
            }

            auto index = ParseInt64(args[1]);
            if (!index.ok())
            {
                return IntegerParseError();
            }
            if (*index != 0)
            {
                return Error("ERR DB index is out of range");
            }
            return Simple("OK");
        }
    } // namespace

    CommandResult ExecuteCommand(RedisDataStruct& redis, const std::vector<std::string>& args)
    {
        if (args.empty())
        {
            return Error("ERR empty command");
        }

        const auto command = ToUpper(args[0]);
        if (command == "PING")
        {
            return Ping(args);
        }
        if (command == "ECHO")
        {
            if (args.size() != 2)
            {
                return WrongArity(command);
            }
            return Bulk(args[1]);
        }
        if (command == "QUIT")
        {
            return Reply(resp::Simple("OK"), true);
        }
        if (command == "COMMAND")
        {
            return Reply(resp::ArrayOf({}));
        }
        if (command == "CLIENT")
        {
            return Client(args);
        }
        if (command == "SELECT")
        {
            return Select(args);
        }
        if (command == "SET")
        {
            return Set(redis, args);
        }
        if (command == "GET")
        {
            return Get(redis, args);
        }
        if (command == "DEL")
        {
            return Del(redis, args);
        }
        if (command == "TYPE")
        {
            return Type(redis, args);
        }
        if (command == "HSET")
        {
            return HSet(redis, args);
        }
        if (command == "HGET")
        {
            return HGet(redis, args);
        }
        if (command == "HDEL")
        {
            return HDel(redis, args);
        }
        if (command == "LPUSH")
        {
            return Push(redis, args, true);
        }
        if (command == "RPUSH")
        {
            return Push(redis, args, false);
        }
        if (command == "LPOP")
        {
            return Pop(redis, args, true);
        }
        if (command == "RPOP")
        {
            return Pop(redis, args, false);
        }
        if (command == "LLEN")
        {
            return LLen(redis, args);
        }
        if (command == "SADD")
        {
            return SAdd(redis, args);
        }
        if (command == "SISMEMBER")
        {
            return SIsMember(redis, args);
        }
        if (command == "SREM")
        {
            return SRem(redis, args);
        }
        if (command == "ZADD")
        {
            return ZAdd(redis, args);
        }
        if (command == "ZSCORE")
        {
            return ZScore(redis, args);
        }
        if (command == "ZREM")
        {
            return ZRem(redis, args);
        }

        return UnknownCommand(command);
    }

} // namespace bitcask::redis
