#pragma once

#include "IOManager.h"

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace bitcask
{

	class FileIO : public IOManager
	{
	public:
		~FileIO() override;

		static absl::StatusOr<std::unique_ptr<FileIO>> Open(std::string_view path);

		absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
		absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override;
		absl::Status Sync() override;
		absl::Status Close() override;
		absl::StatusOr<int64_t> Size() override;

	private:
#ifdef _WIN32
		using native_handle = void*;
		static constexpr native_handle invalid_handle_v = nullptr;
#else
		using native_handle = int;
		static constexpr native_handle invalid_handle_v = -1;
#endif

		explicit FileIO(native_handle fd, std::string path)
			: fd(fd), path(std::move(path)) {}

		bool IsValid() const { return fd != invalid_handle_v; }

		native_handle fd = invalid_handle_v;
		std::string path;
	};

} // namespace bitcask
