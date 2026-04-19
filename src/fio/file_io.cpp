#include "file_io.h"

#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#else
#include <sys/stat.h>
#include <unistd.h> // NOLINT
#endif

namespace bitcask
{

    std::unique_ptr<FileIO> FileIO::Open(const std::filesystem::path& path)
    {
        auto* fd = std::fopen(path.string().c_str(), "a+b");
        if (!fd)
        {
            return nullptr;
        }
        return std::unique_ptr<FileIO>(new FileIO(fd));
    }

    FileIO::FileIO(std::FILE* fd) : fd_(fd)
    {
    }

    FileIO::~FileIO()
    {
        Close();
    }

    std::vector<std::byte> FileIO::Read(uint64_t offset, size_t length)
    {
        std::vector<std::byte> buf(length);

#ifdef _WIN32
        if (_fseeki64(fd_, static_cast<__int64>(offset), SEEK_SET) != 0)
            return {};
#else
        if (fseeko(fd_, static_cast<off_t>(offset), SEEK_SET) != 0)
            return {};
#endif

        auto read = std::fread(buf.data(), 1, length, fd_);
        buf.resize(read);
        return buf;
    }

    size_t FileIO::Write(std::span<const std::byte> data)
    {
        return std::fwrite(data.data(), 1, data.size(), fd_);
    }

    bool FileIO::Sync()
    {
        return std::fflush(fd_) == 0
#ifdef _WIN32
               && _commit(_fileno(fd_)) == 0
#else
               && fsync(fileno(fd_)) == 0
#endif
            ;
    }

    void FileIO::Close()
    {
        if (fd_)
        {
            std::fclose(fd_);
            fd_ = nullptr;
        }
    }

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path)
    {
        return FileIO::Open(path);
    }
} // namespace bitcask
