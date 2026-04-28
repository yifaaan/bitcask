#include "resp/resp.h"

#include <absl/status/status.h>
#include <absl/strings/match.h>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>

#include <algorithm>
#include <format>
#include <memory>
#include <type_traits>
#include <utility>

namespace bitcask::resp
{
    namespace
    {
        constexpr auto kRedisReaderDeleter = [](redisReader* reader) noexcept {
            redisReaderFree(reader);
        };

        constexpr auto kRedisReplyDeleter = [](redisReply* reply) noexcept {
            freeReplyObject(reply);
        };

        using RedisReaderPtr = std::unique_ptr<redisReader, decltype(kRedisReaderDeleter)>;
        using RedisReplyPtr = std::unique_ptr<redisReply, decltype(kRedisReplyDeleter)>;

        absl::StatusOr<std::string> ReplyString(const redisReply& reply)
        {
            if (reply.len == 0)
            {
                return std::string{};
            }
            if (reply.str == nullptr)
            {
                return absl::InvalidArgumentError("RESP string payload is missing");
            }
            return std::string(reply.str, reply.len);
        }

        struct CommandParseResult
        {
            std::vector<std::string> args;
            size_t bytes_consumed = 0;
        };

        size_t ConsumedBytes(const redisReader& reader, size_t input_size)
        {
            if (reader.len == 0)
            {
                return input_size;
            }
            return std::min(reader.pos, input_size);
        }

        bool IsIncompleteFrame(const absl::Status& status)
        {
            return status.code() == absl::StatusCode::kInvalidArgument && absl::StartsWith(status.message(), "Incomplete RESP");
        }

        absl::StatusOr<std::string> CommandArgString(const redisReply& reply)
        {
            switch (reply.type)
            {
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
                return ReplyString(reply);
            default:
                return absl::InvalidArgumentError("RESP command arguments must be strings");
            }
        }

        absl::StatusOr<std::vector<std::string>> CommandArgsFromReply(const redisReply& reply)
        {
            if (reply.type != REDIS_REPLY_ARRAY)
            {
                return absl::InvalidArgumentError("RESP command must be a non-null array");
            }

            std::vector<std::string> args;
            args.reserve(reply.elements);
            for (size_t i = 0; i < reply.elements; ++i)
            {
                if (reply.element == nullptr || reply.element[i] == nullptr)
                {
                    return absl::InvalidArgumentError("RESP command argument is missing");
                }

                auto arg = CommandArgString(*reply.element[i]);
                if (!arg.ok())
                {
                    return arg.status();
                }
                args.push_back(std::move(*arg));
            }

            return args;
        }

        absl::StatusOr<CommandParseResult> ParseCommandOne(absl::string_view input)
        {
            if (input.empty())
            {
                return absl::InvalidArgumentError("Incomplete RESP frame");
            }

            RedisReaderPtr reader(redisReaderCreate(), kRedisReaderDeleter);
            if (reader == nullptr)
            {
                return absl::ResourceExhaustedError("Unable to allocate RESP reader");
            }

            if (redisReaderFeed(reader.get(), input.data(), input.size()) != REDIS_OK)
            {
                return absl::InvalidArgumentError(reader->errstr);
            }

            void* raw_reply = nullptr;
            if (redisReaderGetReply(reader.get(), &raw_reply) != REDIS_OK)
            {
                return absl::InvalidArgumentError(reader->errstr);
            }
            if (raw_reply == nullptr)
            {
                return absl::InvalidArgumentError("Incomplete RESP frame");
            }

            RedisReplyPtr reply(static_cast<redisReply*>(raw_reply), kRedisReplyDeleter);
            auto args = CommandArgsFromReply(*reply);
            if (!args.ok())
            {
                return args.status();
            }

            return CommandParseResult{std::move(*args), ConsumedBytes(*reader, input.size())};
        }

        void AppendSerialized(const Value& value, std::string& out)
        {
            std::visit(
                [&out](const auto& item) {
                    using T = std::decay_t<decltype(item)>;
                    if constexpr (std::is_same_v<T, SimpleString>)
                    {
                        out += std::format("+{}\r\n", item.value);
                    }
                    else if constexpr (std::is_same_v<T, Error>)
                    {
                        out += std::format("-{}\r\n", item.message);
                    }
                    else if constexpr (std::is_same_v<T, Integer>)
                    {
                        out += std::format(":{}\r\n", item.value);
                    }
                    else if constexpr (std::is_same_v<T, BulkString>)
                    {
                        if (!item.value.has_value())
                        {
                            out += "$-1\r\n";
                            return;
                        }
                        out += std::format("${}\r\n{}\r\n", item.value->size(), *item.value);
                    }
                    else if constexpr (std::is_same_v<T, Array>)
                    {
                        out += std::format("*{}\r\n", item.values.size());
                        for (const auto& element : item.values)
                        {
                            AppendSerialized(element, out);
                        }
                    }
                },
                value.data);
        }
    } // namespace

    Value::Value(SimpleString value) : data(std::move(value)) {}
    Value::Value(Error value) : data(std::move(value)) {}
    Value::Value(Integer value) : data(std::move(value)) {}
    Value::Value(BulkString value) : data(std::move(value)) {}
    Value::Value(Array value) : data(std::move(value)) {}

    Value Simple(std::string value)
    {
        return Value(SimpleString{std::move(value)});
    }

    Value Err(std::string message)
    {
        return Value(Error{std::move(message)});
    }

    Value Int(int64_t value)
    {
        return Value(Integer{value});
    }

    Value Bulk(std::string value)
    {
        return Value(BulkString{std::move(value)});
    }

    Value NullBulk()
    {
        return Value(BulkString{std::nullopt});
    }

    Value ArrayOf(std::vector<Value> values)
    {
        return Value(Array{std::move(values)});
    }

    std::string Serialize(const Value& value)
    {
        std::string out;
        AppendSerialized(value, out);
        return out;
    }

    void StreamParser::Append(absl::string_view input)
    {
        buffer_.append(input.data(), input.size());
    }

    absl::StatusOr<std::optional<std::vector<std::string>>> StreamParser::Next()
    {
        auto parsed = ParseCommandOne(buffer_);
        if (!parsed.ok())
        {
            if (IsIncompleteFrame(parsed.status()))
            {
                return std::optional<std::vector<std::string>>{};
            }
            return parsed.status();
        }

        auto args = std::move(parsed->args);
        buffer_.erase(0, parsed->bytes_consumed);
        return std::optional<std::vector<std::string>>{std::move(args)};
    }

    void StreamParser::Clear()
    {
        buffer_.clear();
    }

    size_t StreamParser::BufferedBytes() const
    {
        return buffer_.size();
    }

} // namespace bitcask::resp
