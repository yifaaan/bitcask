#pragma once

#include "Data/LogRecord.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace bitcask
{

	enum class IOType
	{
		Standard, // FILE* based I/O
		MMap,	  // Memory-mapped read-only I/O
	};

	class IOManager
	{
	public:
		virtual ~IOManager() = default;

		// Read [offset, offset + len) into buf
		// Returns actual bytes read, or error
		virtual absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) = 0;

		// Write data to end of file (append-only)
		// Returns bytes written, or error
		virtual absl::StatusOr<int64_t> Write(std::span<const std::byte> data) = 0;

		// Sync to disk
		virtual absl::Status Sync() = 0;

		// Close file
		virtual absl::Status Close() = 0;

		// Get file size
		virtual absl::StatusOr<int64_t> Size() = 0;

		// Factory function
		static absl::StatusOr<std::unique_ptr<IOManager>> NewIOManager(std::string_view filePath, IOType type);
	};

} // namespace bitcask
