#include "Data/DataFile.h"

#include <absl/status/status.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>

namespace bitcask
{
namespace
{

	class TempDir
	{
	public:
		TempDir()
			: path_(std::filesystem::temp_directory_path() /
					("bitcask-data-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())))
		{
			std::filesystem::create_directories(path_);
		}

		~TempDir()
		{
			std::error_code ec;
			std::filesystem::remove_all(path_, ec);
		}

		const std::filesystem::path& path() const noexcept
		{
			return path_;
		}

	private:
		std::filesystem::path path_;
	};

	absl::StatusOr<int64_t> WriteRecord(DataFile& file, const LogRecord& record)
	{
		auto [encoded, size] = EncodeLogRecord(record);
		auto written = file.Write(encoded);
		if (!written.ok())
		{
			return written.status();
		}
		return size;
	}

	TEST(DataFile, OpensExpectedFileNameAndTracksWriteOffset)
	{
		TempDir dir;
		auto file = DataFile::Open(dir.path().string(), 1, IOType::Standard);
		ASSERT_TRUE(file.ok()) << file.status();

		EXPECT_TRUE(std::filesystem::exists(dir.path() / DataFileName(1)));
		EXPECT_EQ((*file)->fid, 1);
		EXPECT_EQ((*file)->writeOffset, 0);

		const LogRecord record{.key = "a", .value = "b"};
		auto size = WriteRecord(**file, record);
		ASSERT_TRUE(size.ok()) << size.status();
		EXPECT_EQ((*file)->writeOffset, *size);
	}

	TEST(DataFile, ReadsWrittenRecord)
	{
		TempDir dir;
		auto file = DataFile::Open(dir.path().string(), 2, IOType::Standard);
		ASSERT_TRUE(file.ok()) << file.status();

		const LogRecord record{.key = "hello", .value = "world"};
		auto size = WriteRecord(**file, record);
		ASSERT_TRUE(size.ok()) << size.status();

		auto read = (*file)->ReadLogRecord(0);
		ASSERT_TRUE(read.ok()) << read.status();
		EXPECT_FALSE(read->isEof);
		EXPECT_EQ(read->size, *size);
		EXPECT_EQ(read->record.key, record.key);
		EXPECT_EQ(read->record.value, record.value);
		EXPECT_EQ(read->record.type, record.type);
	}

	TEST(DataFile, ScansSequentialRecordsAndDetectsEof)
	{
		TempDir dir;
		auto file = DataFile::Open(dir.path().string(), 3, IOType::Standard);
		ASSERT_TRUE(file.ok()) << file.status();

		auto firstSize = WriteRecord(**file, LogRecord{.key = "k1", .value = "v1"});
		ASSERT_TRUE(firstSize.ok()) << firstSize.status();
		auto secondSize = WriteRecord(**file, LogRecord{.key = "k2", .value = "v2"});
		ASSERT_TRUE(secondSize.ok()) << secondSize.status();

		auto first = (*file)->ReadLogRecord(0);
		ASSERT_TRUE(first.ok()) << first.status();
		EXPECT_EQ(first->record.key, "k1");

		auto second = (*file)->ReadLogRecord(first->size);
		ASSERT_TRUE(second.ok()) << second.status();
		EXPECT_EQ(second->record.key, "k2");
		EXPECT_EQ(second->size, *secondSize);

		auto eof = (*file)->ReadLogRecord(first->size + second->size);
		ASSERT_TRUE(eof.ok()) << eof.status();
		EXPECT_TRUE(eof->isEof);
		EXPECT_EQ(eof->size, 0);
	}

	TEST(DataFile, DetectsCorruptedCRC)
	{
		TempDir dir;
		const auto path = dir.path() / DataFileName(4);
		{
			auto file = DataFile::Open(dir.path().string(), 4, IOType::Standard);
			ASSERT_TRUE(file.ok()) << file.status();
			auto size = WriteRecord(**file, LogRecord{.key = "bad", .value = "crc"});
			ASSERT_TRUE(size.ok()) << size.status();
			ASSERT_TRUE((*file)->Sync().ok());
		}

		std::fstream stream(path, std::ios::in | std::ios::out | std::ios::binary);
		ASSERT_TRUE(stream.is_open());
		stream.seekp(0);
		stream.put(static_cast<char>(0xFF));
		stream.close();

		auto file = DataFile::Open(dir.path().string(), 4, IOType::Standard);
		ASSERT_TRUE(file.ok()) << file.status();
		auto read = (*file)->ReadLogRecord(0);
		ASSERT_FALSE(read.ok());
		EXPECT_EQ(read.status().code(), absl::StatusCode::kDataLoss);
	}

	TEST(DataFile, AppendsHintRecord)
	{
		TempDir dir;
		auto file = OpenHintFile(dir.path().string(), IOType::Standard);
		ASSERT_TRUE(file.ok()) << file.status();

		const LogRecordPos pos{.fid = 7, .offset = 99, .size = 33};
		auto written = (*file)->AppendHintRecord("hint-key", pos);
		ASSERT_TRUE(written.ok()) << written.status();

		auto read = (*file)->ReadLogRecord(0);
		ASSERT_TRUE(read.ok()) << read.status();
		EXPECT_EQ(read->record.key, "hint-key");

		const auto valueBytes = std::as_bytes(std::span<const char>(read->record.value.data(), read->record.value.size()));
		auto [decodedPos, decodedSize] = DecodeLogRecordPos(valueBytes);
		ASSERT_TRUE(decodedPos.has_value());
		EXPECT_EQ(decodedPos->fid, pos.fid);
		EXPECT_EQ(decodedPos->offset, pos.offset);
		EXPECT_EQ(decodedPos->size, pos.size);
		EXPECT_EQ(decodedSize, static_cast<int64_t>(read->record.value.size()));
	}

} // namespace
} // namespace bitcask
