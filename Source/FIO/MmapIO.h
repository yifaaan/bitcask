#pragma once

#include "FIO/IOManager.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mio/mmap.hpp>
#include <span>
#include <string>
#include <string_view>

namespace bitcask
{

	class MmapIO : public IOManager
	{
	public:
		~MmapIO() override;

		static absl::StatusOr<std::unique_ptr<MmapIO>> Open(const std::string& path);

		absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
		absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override;
		absl::Status Sync() override;
		absl::Status Close() override;
		absl::StatusOr<int64_t> Size() override;

	private:
		mio::mmap_source mmap;
		std::string path;
		bool closed = false;

		explicit MmapIO(mio::mmap_source mmap, std::string path)
			: mmap(std::move(mmap)), path(std::move(path)) {}
	};

} // namespace bitcask
