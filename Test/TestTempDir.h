#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace bitcask::test
{
	inline uint64_t ProcessId()
	{
#ifdef _WIN32
		return static_cast<uint64_t>(GetCurrentProcessId());
#else
		return static_cast<uint64_t>(getpid());
#endif
	}

	inline std::string Hex(uint64_t value)
	{
		std::ostringstream stream;
		stream << std::hex << value;
		return stream.str();
	}

	inline std::filesystem::path MakeUniqueTempDir(std::string_view prefix)
	{
		static std::atomic_uint64_t counter{0};
		const auto root = std::filesystem::temp_directory_path() / "bitcask-tests";

		std::error_code ec;
		std::filesystem::create_directories(root, ec);
		if (ec)
		{
			throw std::runtime_error("failed to create temp test root: " + ec.message());
		}

		const auto tick = static_cast<uint64_t>(
			std::chrono::high_resolution_clock::now().time_since_epoch().count());
		const auto threadHash = static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
		const auto pid = ProcessId();

		for (int attempt = 0; attempt < 128; ++attempt)
		{
			const auto seq = counter.fetch_add(1, std::memory_order_relaxed);
			const auto name = std::string(prefix) + "-" + Hex(pid) + "-" + Hex(tick) + "-" +
							  Hex(threadHash) + "-" + Hex(seq);
			const auto path = root / name;

			ec.clear();
			if (std::filesystem::create_directory(path, ec))
			{
				return path;
			}
			if (ec)
			{
				throw std::runtime_error("failed to create temp test directory: " + ec.message());
			}
		}

		throw std::runtime_error("failed to allocate a unique temp test directory");
	}

	class ScopedTempDir
	{
	public:
		explicit ScopedTempDir(std::string_view prefix)
			: path_(MakeUniqueTempDir(prefix))
		{
		}

		ScopedTempDir(const ScopedTempDir&) = delete;
		ScopedTempDir& operator=(const ScopedTempDir&) = delete;

		ScopedTempDir(ScopedTempDir&& other) noexcept
			: path_(std::move(other.path_))
		{
			other.path_.clear();
		}

		ScopedTempDir& operator=(ScopedTempDir&& other) noexcept
		{
			if (this != &other)
			{
				Cleanup();
				path_ = std::move(other.path_);
				other.path_.clear();
			}
			return *this;
		}

		~ScopedTempDir()
		{
			Cleanup();
		}

		const std::filesystem::path& path() const noexcept
		{
			return path_;
		}

	private:
		void Cleanup() noexcept
		{
			if (path_.empty())
			{
				return;
			}
			std::error_code ec;
			std::filesystem::remove_all(path_, ec);
			path_.clear();
		}

		std::filesystem::path path_;
	};
} // namespace bitcask::test
