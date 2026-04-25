#pragma once

#include <absl/types/span.h>

#include <cstdint>
#include <filesystem>
#include <memory>

namespace bitcask
{

    class IOManager
    {
    public:
        virtual ~IOManager() = default;

        // Reads up to buf.size() bytes into buf from the given offset. Returns the
        // number of bytes read, or -1 on error Returns the number of bytes read, or
        // -1 on error
        virtual int Read(absl::Span<std::byte> buf, int64_t offset) = 0;
        //  Writes data to the file. Returns the number of bytes written, or -1 on
        //  error
        virtual int Write(absl::Span<const std::byte> data) = 0;
        //  Flushes any buffered data to the underlying storage. Returns true on success,
        //  false on error
        virtual bool Sync() = 0;

        virtual bool Close() = 0;
        // Returns the size of the file, or -1 on error
        virtual int64_t Size() const = 0;
    };

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path);

} // namespace bitcask
