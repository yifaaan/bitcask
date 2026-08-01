#include "Data/LogRecord.h"

#include <gtest/gtest.h>

#include <string>

namespace bitcask
{
namespace
{

	TEST(LogRecord, EncodeDecodeRoundTrip)
	{
		const LogRecord record
		{
			.key = "hello",
			.value = "world",
			.type = LogRecordType::Normal,
		};

		auto encoded = EncodeLogRecord(record);
		ASSERT_FALSE(encoded.empty());

		auto headerOr = DecodeLogRecordHeader(encoded);
		ASSERT_TRUE(headerOr.first.has_value());

		const auto& header = *headerOr.first;
		EXPECT_EQ(header.type, record.type);
		EXPECT_EQ(header.keySize, record.key.size());
		EXPECT_EQ(header.valueSize, record.value.size());

		EXPECT_EQ(CalcLogRecordCRC(record, header), header.crc);
	}

	TEST(LogRecord, EncodeEmptyKeyValue)
	{
		const LogRecord record
		{
			.key = "",
			.value = "",
			.type = LogRecordType::Deleted,
		};

		auto encoded = EncodeLogRecord(record);
		ASSERT_FALSE(encoded.empty());

		auto headerOr = DecodeLogRecordHeader(encoded);
		ASSERT_TRUE(headerOr.first.has_value());
		EXPECT_EQ(headerOr.first->type, LogRecordType::Deleted);
		EXPECT_EQ(headerOr.first->keySize, 0u);
		EXPECT_EQ(headerOr.first->valueSize, 0u);
	}

	TEST(LogRecord, DecodeTruncatedHeaderFails)
	{
		std::vector<std::byte> buf(3, std::byte{0});
		auto headerOr = DecodeLogRecordHeader(buf);
		EXPECT_FALSE(headerOr.first.has_value());
		EXPECT_EQ(headerOr.second, 0);
	}

} // namespace
} // namespace bitcask
