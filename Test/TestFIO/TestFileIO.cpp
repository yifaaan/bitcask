#include "FIO/FileIO.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

namespace bitcask
{
namespace
{
	std::filesystem::path TempFilePath(const std::string& name)
	{
		auto dir = std::filesystem::temp_directory_path() / "bitcask-fileio-tests";
		std::filesystem::create_directories(dir);
		return dir / name;
	}

	TEST(FileIO, WriteReadSyncAndSize)
	{
		auto path = TempFilePath("write-read-sync.data");
		std::error_code ec;
		std::filesystem::remove(path, ec);

		auto fileOr = FileIO::Open(path.string());
		ASSERT_TRUE(fileOr.ok()) << fileOr.status();

		auto file = std::move(*fileOr);
		const std::vector<std::byte> data{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}, std::byte{'d'}};

		auto writeOr = file->Write(data);
		ASSERT_TRUE(writeOr.ok()) << writeOr.status();
		EXPECT_EQ(*writeOr, static_cast<int64_t>(data.size()));

		auto syncStatus = file->Sync();
		EXPECT_TRUE(syncStatus.ok()) << syncStatus;

		auto sizeOr = file->Size();
		ASSERT_TRUE(sizeOr.ok()) << sizeOr.status();
		EXPECT_EQ(*sizeOr, static_cast<int64_t>(data.size()));

		std::vector<std::byte> readBuf(data.size());
		auto readOr = file->Read(readBuf, 0);
		ASSERT_TRUE(readOr.ok()) << readOr.status();
		EXPECT_EQ(*readOr, static_cast<int64_t>(data.size()));
		EXPECT_EQ(readBuf, data);

		auto closeStatus = file->Close();
		EXPECT_TRUE(closeStatus.ok()) << closeStatus;

		std::filesystem::remove(path, ec);
	}

	TEST(FileIO, AppendWritesToEnd)
	{
		auto path = TempFilePath("append.data");
		std::error_code ec;
		std::filesystem::remove(path, ec);

		auto fileOr = FileIO::Open(path.string());
		ASSERT_TRUE(fileOr.ok()) << fileOr.status();

		auto file = std::move(*fileOr);
		const std::vector<std::byte> first{std::byte{'x'}, std::byte{'y'}};
		const std::vector<std::byte> second{std::byte{'z'}};

		ASSERT_TRUE(file->Write(first).ok());
		ASSERT_TRUE(file->Write(second).ok());

		auto sizeOr = file->Size();
		ASSERT_TRUE(sizeOr.ok()) << sizeOr.status();
		EXPECT_EQ(*sizeOr, 3);

		std::vector<std::byte> readBuf(3);
		auto readOr = file->Read(readBuf, 0);
		ASSERT_TRUE(readOr.ok()) << readOr.status();
		EXPECT_EQ(readBuf[0], std::byte{'x'});
		EXPECT_EQ(readBuf[1], std::byte{'y'});
		EXPECT_EQ(readBuf[2], std::byte{'z'});

		ASSERT_TRUE(file->Close().ok());
		std::filesystem::remove(path, ec);
	}

} // namespace
} // namespace bitcask
