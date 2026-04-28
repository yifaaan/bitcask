#pragma once

#include <absl/strings/string_view.h>
#include <absl/status/statusor.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace bitcask::resp
{

    struct Value;

    struct SimpleString
    {
        std::string value;

        friend bool operator==(const SimpleString&, const SimpleString&) = default;
    };

    struct Error
    {
        std::string message;

        friend bool operator==(const Error&, const Error&) = default;
    };

    struct Integer
    {
        int64_t value = 0;

        friend bool operator==(const Integer&, const Integer&) = default;
    };

    struct BulkString
    {
        std::optional<std::string> value;

        friend bool operator==(const BulkString&, const BulkString&) = default;
    };

    struct Array
    {
        std::vector<Value> values;

        friend bool operator==(const Array&, const Array&) = default;
    };

    struct Value
    {
        using Variant = std::variant<SimpleString, Error, Integer, BulkString, Array>;

        Variant data;

        Value() = default;
        Value(SimpleString value);
        Value(Error value);
        Value(Integer value);
        Value(BulkString value);
        Value(Array value);

        friend bool operator==(const Value&, const Value&) = default;
    };

    Value Simple(std::string value);
    Value Err(std::string message);
    Value Int(int64_t value);
    Value Bulk(std::string value);
    Value NullBulk();
    Value ArrayOf(std::vector<Value> values);

    std::string Serialize(const Value& value);

    class StreamParser
    {
    public:
        void Append(absl::string_view input);
        absl::StatusOr<std::optional<std::vector<std::string>>> Next();
        void Clear();
        size_t BufferedBytes() const;

    private:
        std::string buffer_;
    };

} // namespace bitcask::resp
