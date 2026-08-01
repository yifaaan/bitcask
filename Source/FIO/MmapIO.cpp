#include "MmapIO.h"

#include <absl/status/status.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <format>
#include <mio/mmap.hpp>
#include <span>
#include <system_error>

namespace bitcask
{

	MmapIO::~MmapIO()
	{
		if (mmap.is_mapped())
		{
			mmap.unmap();
		}
	}

	absl::StatusOr<std::unique_ptr<MmapIO>> MmapIO::Open(const std::string& path)
	{
		std::error_code fileEc;
		const auto fileSize = std::filesystem::file_size(path, fileEc);
		if (fileEc)
		{
			return absl::InternalError(
				std::format("failed to stat '{}' for mmap: {}", path, fileEc.message()));
		}

		// mio cannot reliably map empty files on all platforms, so treat it as an empty mapping.
		if (fileSize == 0)
		{
			return std::unique_ptr<MmapIO>(new MmapIO(mio::mmap_source{}, path));
		}

		std::error_code ec;
		auto mmap = mio::make_mmap_source(path, 0, mio::map_entire_file, ec);
		if (ec)
		{
			return absl::InternalError(std::format("failed to mmap '{}': {}", path, ec.message()));
		}

		return std::unique_ptr<MmapIO>(new MmapIO(std::move(mmap), path));
	}

	absl::StatusOr<int64_t> MmapIO::Read(std::span<std::byte> buf, int64_t offset)
	{
		if (closed)
		{
			return absl::FailedPreconditionError("file not open");
		}
		if (offset < 0)
		{
			return absl::InvalidArgumentError("negative offset");
		}

		const auto mappedSize = static_cast<int64_t>(mmap.size());
		if (offset >= mappedSize)
		{
			return absl::OutOfRangeError("offset beyond file end");
		}

		const auto toRead = std::min<int64_t>(static_cast<int64_t>(buf.size()), mappedSize - offset);
		std::memcpy(buf.data(), mmap.data() + offset, static_cast<size_t>(toRead));
		return toRead;
	}

	absl::StatusOr<int64_t> MmapIO::Write(std::span<const std::byte> data)
	{
		return absl::FailedPreconditionError("MmapIO is read-only");
	}

	absl::Status MmapIO::Sync()
	{
		return absl::FailedPreconditionError("MmapIO is read-only");
	}

	absl::Status MmapIO::Close()
	{
		if (closed)
		{
			return absl::OkStatus();
		}

		if (mmap.is_mapped())
		{
			mmap.unmap();
		}

		closed = true;
		return absl::OkStatus();
	}

	absl::StatusOr<int64_t> MmapIO::Size()
	{
		if (closed)
		{
			return absl::FailedPreconditionError("file not open");
		}
		return static_cast<int64_t>(mmap.size());
	}

} // namespace bitcask
