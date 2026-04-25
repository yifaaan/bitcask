#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include "absl/types/span.h"

namespace bitcask
{

    class IOManager
    {
    public:
        virtual ~IOManager() = default;

        virtual int Read(absl::Span<std::byte> buf, int64_t offset) = 0;
        virtual int Write(absl::Span<const std::byte> data) = 0;
        virtual bool Sync() = 0;
        virtual bool Close() = 0;
        virtual int64_t Size() const = 0;
    };

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path);

} // namespace bitcask
