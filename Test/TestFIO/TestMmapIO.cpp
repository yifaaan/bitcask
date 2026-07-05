#include "FIO/FileIO.h"
#include "FIO/MmapIO.h"

#include "../TestTempDir.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

namespace bitcask
{
namespace
{
	std::vector<std::byte> ToBytes(std::string_view s)
	{
		return std::vector<std::byte>(
			reinterpret_cast<const std::byte*>(s.data()),
			reinterpret_cast<const std::byte*>(s.data() + s.size()));
	}

	std::string ToString(const std::vector<std::byte>& bytes)
	{
		return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
	}

	TEST(MmapIO, ReadExistingFile)
	{
		test::ScopedTempDir temp("mmapio-test");
		auto path = temp.path() / "read-existing.data";

		const std::string payload = "hello memory mapped io";
		{
			auto fileOr = FileIO::Open(path.string());
			ASSERT_TRUE(fileOr.ok()) << fileOr.status();
			auto file = std::move(*fileOr);
			auto data = ToBytes(payload);
			ASSERT_TRUE(file->Write(data).ok());
			ASSERT_TRUE(file->Sync().ok());
		}

		auto mmapOr = MmapIO::Open(path.string());
		ASSERT_TRUE(mmapOr.ok()) << mmapOr.status();
		auto mmap = std::move(*mmapOr);

		auto sizeOr = mmap->Size();
		ASSERT_TRUE(sizeOr.ok()) << sizeOr.status();
		EXPECT_EQ(*sizeOr, static_cast<int64_t>(payload.size()));

		std::vector<std::byte> buf(payload.size());
		auto readOr = mmap->Read(buf, 0);
		ASSERT_TRUE(readOr.ok()) << readOr.status();
		EXPECT_EQ(*readOr, static_cast<int64_t>(payload.size()));
		EXPECT_EQ(ToString(buf), payload);

		EXPECT_TRUE(mmap->Close().ok());
	}

	TEST(MmapIO, ReadAtOffset)
	{
		test::ScopedTempDir temp("mmapio-test");
		auto path = temp.path() / "read-offset.data";

		const std::string payload = "0123456789abcdef";
		{
			auto fileOr = FileIO::Open(path.string());
			ASSERT_TRUE(fileOr.ok()) << fileOr.status();
			auto file = std::move(*fileOr);
			ASSERT_TRUE(file->Write(ToBytes(payload)).ok());
			ASSERT_TRUE(file->Sync().ok());
		}

		auto mmapOr = MmapIO::Open(path.string());
		ASSERT_TRUE(mmapOr.ok()) << mmapOr.status();
		auto mmap = std::move(*mmapOr);

		std::vector<std::byte> buf(4);
		auto readOr = mmap->Read(buf, 4);
		ASSERT_TRUE(readOr.ok()) << readOr.status();
		EXPECT_EQ(*readOr, 4);
		EXPECT_EQ(ToString(buf), "4567");

		EXPECT_TRUE(mmap->Close().ok());
	}

	TEST(MmapIO, EmptyFileIsValid)
	{
		test::ScopedTempDir temp("mmapio-test");
		auto path = temp.path() / "empty.data";

		// Create an empty file via FileIO
		{
			auto fileOr = FileIO::Open(path.string());
			ASSERT_TRUE(fileOr.ok()) << fileOr.status();
			ASSERT_TRUE((*fileOr)->Sync().ok());
		}

		auto mmapOr = MmapIO::Open(path.string());
		ASSERT_TRUE(mmapOr.ok()) << mmapOr.status();
		auto mmap = std::move(*mmapOr);

		auto sizeOr = mmap->Size();
		ASSERT_TRUE(sizeOr.ok()) << sizeOr.status();
		EXPECT_EQ(*sizeOr, 0);

		std::vector<std::byte> buf(4);
		auto readOr = mmap->Read(buf, 0);
		EXPECT_FALSE(readOr.ok());
		EXPECT_EQ(readOr.status().code(), absl::StatusCode::kOutOfRange);

		EXPECT_TRUE(mmap->Close().ok());
	}

	TEST(MmapIO, WriteAndSyncReturnReadOnlyError)
	{
		test::ScopedTempDir temp("mmapio-test");
		auto path = temp.path() / "readonly.data";

		const std::string payload = "readonly";
		{
			auto fileOr = FileIO::Open(path.string());
			ASSERT_TRUE(fileOr.ok()) << fileOr.status();
			auto file = std::move(*fileOr);
			ASSERT_TRUE(file->Write(ToBytes(payload)).ok());
			ASSERT_TRUE(file->Sync().ok());
		}

		auto mmapOr = MmapIO::Open(path.string());
		ASSERT_TRUE(mmapOr.ok()) << mmapOr.status();
		auto mmap = std::move(*mmapOr);

		std::vector<std::byte> data{std::byte{'x'}};
		auto writeOr = mmap->Write(data);
		EXPECT_FALSE(writeOr.ok());
		EXPECT_EQ(writeOr.status().code(), absl::StatusCode::kFailedPrecondition);

		auto syncStatus = mmap->Sync();
		EXPECT_FALSE(syncStatus.ok());
		EXPECT_EQ(syncStatus.code(), absl::StatusCode::kFailedPrecondition);

		EXPECT_TRUE(mmap->Close().ok());
	}

} // namespace
} // namespace bitcask
