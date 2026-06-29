#include "Data/LogRecord.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace bitcask
{
namespace
{

	TEST(LogRecord, EncodesAndDecodesHeader)
	{
		const LogRecord record{
			.key = "test-key",
			.value = "test-value",
			.type = LogRecordType::Normal,
		};

		auto [encoded, size] = EncodeLogRecord(record);
		auto [headerOpt, headerSize] = DecodeLogRecordHeader(encoded);

		ASSERT_TRUE(headerOpt.has_value());
		EXPECT_EQ(size, static_cast<int64_t>(encoded.size()));
		EXPECT_EQ(headerOpt->type, LogRecordType::Normal);
		EXPECT_EQ(headerOpt->keySize, static_cast<int64_t>(record.key.size()));
		EXPECT_EQ(headerOpt->valueSize, static_cast<int64_t>(record.value.size()));

		const auto headerWithoutCRC = std::span<const std::byte>(encoded).subspan(4, static_cast<size_t>(headerSize - 4));
		EXPECT_EQ(CalcLogRecordCRC(record, headerWithoutCRC), headerOpt->crc);
	}

	TEST(LogRecord, SupportsAllRecordTypesAndEmptyFields)
	{
		for (const auto type : {LogRecordType::Normal, LogRecordType::Deleted, LogRecordType::TxnFinished})
		{
			const LogRecord record{.type = type};
			auto [encoded, size] = EncodeLogRecord(record);
			auto [headerOpt, headerSize] = DecodeLogRecordHeader(encoded);

			ASSERT_TRUE(headerOpt.has_value());
			EXPECT_EQ(headerOpt->type, type);
			EXPECT_EQ(headerOpt->keySize, 0);
			EXPECT_EQ(headerOpt->valueSize, 0);
			EXPECT_EQ(size, headerSize);
		}
	}

	TEST(LogRecord, SupportsLargeVarintSizes)
	{
		LogRecord record;
		record.key = std::string(300, 'k');
		record.value = std::string(16 * 1024, 'v');

		auto [encoded, size] = EncodeLogRecord(record);
		auto [headerOpt, headerSize] = DecodeLogRecordHeader(encoded);

		ASSERT_TRUE(headerOpt.has_value());
		EXPECT_EQ(headerOpt->keySize, static_cast<int64_t>(record.key.size()));
		EXPECT_EQ(headerOpt->valueSize, static_cast<int64_t>(record.value.size()));
		EXPECT_EQ(size, static_cast<int64_t>(encoded.size()));
		EXPECT_GT(headerSize, 6);
	}

	TEST(LogRecord, RejectsInvalidHeader)
	{
		const std::array<std::byte, 5> invalidType = {
			std::byte{0x00},
			std::byte{0x00},
			std::byte{0x00},
			std::byte{0x00},
			std::byte{0xFF},
		};

		auto [headerOpt, headerSize] = DecodeLogRecordHeader(invalidType);
		EXPECT_FALSE(headerOpt.has_value());
		EXPECT_EQ(headerSize, 0);
	}

	TEST(LogRecordPos, EncodesAndDecodesRoundTrip)
	{
		const LogRecordPos pos{.fid = 42, .offset = 123456, .size = 789};

		auto [encoded, size] = EncodeLogRecordPos(pos);
		auto [decodedOpt, decodedSize] = DecodeLogRecordPos(encoded);

		ASSERT_TRUE(decodedOpt.has_value());
		EXPECT_EQ(decodedOpt->fid, pos.fid);
		EXPECT_EQ(decodedOpt->offset, pos.offset);
		EXPECT_EQ(decodedOpt->size, pos.size);
		EXPECT_EQ(decodedSize, size);
	}

} // namespace
} // namespace bitcask
