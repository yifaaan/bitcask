#include "FileIO.h"

#include <cerrno>
#include <cstring>
#include <format>
#include <tuple>

#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#define fsync _commit
#define fileno _fileno
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace bitcask
{

	FileIO::~FileIO()
	{
		if (file)
		{
			std::ignore = Close();
		}
	}

	absl::StatusOr<std::unique_ptr<FileIO>> FileIO::Open(const std::string& path)
	{
		// Open file: read/write + binary + append + create
		FILE* file = std::fopen(path.c_str(), "a+b");
		if (!file)
		{
			return absl::InternalError(
				std::format("failed to open file '{}': {}", path, std::strerror(errno)));
		}
		return std::unique_ptr<FileIO>(new FileIO(file, path));
	}

	absl::StatusOr<int64_t> FileIO::Read(std::span<std::byte> buf, int64_t offset)
	{
		if (!file)
		{
			return absl::FailedPreconditionError("file not open");
		}

		// Seek to offset
#ifdef _WIN32
		if (_fseeki64(file, offset, SEEK_SET) != 0)
		{
#else
		if (fseeko(file, offset, SEEK_SET) != 0)
		{
#endif
			return absl::InternalError("fseek failed");
		}

		// Read
		size_t n = std::fread(buf.data(), 1, buf.size(), file);
		if (n == 0 && ferror(file))
		{
			return absl::InternalError("fread failed");
		}
		return static_cast<int64_t>(n);
	}

	absl::StatusOr<int64_t> FileIO::Write(std::span<const std::byte> data)
	{
		if (!file)
		{
			return absl::FailedPreconditionError("file not open");
		}

		// Seek to end (ensure append)
#ifdef _WIN32
		if (_fseeki64(file, 0, SEEK_END) != 0)
		{
#else
		if (fseeko(file, 0, SEEK_END) != 0)
		{
#endif
			return absl::InternalError("fseek to end failed");
		}

		// Write
		size_t n = std::fwrite(data.data(), 1, data.size(), file);
		if (n != data.size())
		{
			return absl::InternalError("fwrite incomplete");
		}
		return static_cast<int64_t>(n);
	}

	absl::Status FileIO::Sync()
	{
		if (!file)
		{
			return absl::FailedPreconditionError("file not open");
		}

		std::fflush(file);

#ifdef _WIN32
		if (_commit(_fileno(file)) != 0)
		{
			return absl::InternalError("_commit failed");
		}
#else
		if (fsync(fileno(file)) != 0)
		{
			return absl::InternalError("fsync failed");
		}
#endif
		return absl::OkStatus();
	}

	absl::Status FileIO::Close()
	{
		if (!file)
		{
			return absl::OkStatus();
		}

		if (std::fclose(file) != 0)
		{
			file = nullptr;
			return absl::InternalError("fclose failed");
		}
		file = nullptr;
		return absl::OkStatus();
	}

	absl::StatusOr<int64_t> FileIO::Size()
	{
		if (!file)
		{
			return absl::FailedPreconditionError("file not open");
		}

		std::fflush(file);

#ifdef _WIN32
		struct _stat64 st;
		if (_fstat64(_fileno(file), &st) != 0)
		{
#else
		struct stat st;
		if (fstat(fileno(file), &st) != 0)
		{
#endif
			return absl::InternalError("fstat failed");
		}
		return st.st_size;
	}

} // namespace bitcask
