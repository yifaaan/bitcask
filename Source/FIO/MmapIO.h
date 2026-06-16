#pragma once

#include "IOManager.h"
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace bitcask
{

	class MmapIO : public IOManager
	{
	public:
		~MmapIO() override;

		// Open read-only memory-mapped file
		static absl::StatusOr<std::unique_ptr<MmapIO>> Open(const std::string& path);

		absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
		absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override; // Always returns error
		absl::Status Sync() override;											 // No-op (read-only)
		absl::Status Close() override;
		absl::StatusOr<int64_t> Size() override;

	private:
		MmapIO(void* addr, int64_t size
#ifdef _WIN32
			   ,
			   HANDLE handle
#endif
			   )
			: addr(addr), size(size)
#ifdef _WIN32
			  ,
			  handle(handle)
#endif
		{
		}

		void* addr = nullptr; // Mapped address
		int64_t size = 0;	   // File size

#ifdef _WIN32
		HANDLE handle = INVALID_HANDLE_VALUE; // Windows file handle
#else
		int fd_ = -1; // POSIX file descriptor
#endif
	};

} // namespace bitcask
