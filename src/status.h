#pragma once

#include <string>
#include <string_view>

namespace bitcask
{
    enum class StatusCode : uint8_t
    {
        kOK,
        kNotFound,
        kIOError,
        kKeyIsEmpty,
    };

    class Status
    {
    public:
        Status() = default;

        static Status Ok()
        {
            return {};
        }
        static Status IOError(std::string_view msg)
        {
            return { StatusCode::kIOError, msg };
        }
        static Status NotFound(std::string_view msg)
        {
            return { StatusCode::kNotFound, msg };
        }
        static Status KeyIsEmpty(std::string_view msg)
        {
            return { StatusCode::kKeyIsEmpty, msg };
        }

        [[nodiscard]] bool ok() const
        {
            return code_ == StatusCode::kOK;
        }
        [[nodiscard]] StatusCode code() const
        {
            return code_;
        }
        [[nodiscard]] const std::string& message() const
        {
            return msg_;
        }

        explicit operator bool() const
        {
            return ok();
        }

    private:
        StatusCode code_{ StatusCode::kOK };
        std::string msg_;

        Status(StatusCode code, std::string_view msg) : code_(code), msg_(msg)
        {
        }
    };

} // namespace bitcask
