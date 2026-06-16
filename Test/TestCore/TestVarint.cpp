#include "Core/Varint.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <vector>

namespace bitcask
{
namespace
{

	TEST(Varint, EncodesAndDecodesBoundaryValues)
	{
		const std::vector<uint64_t> values = {
			0,
			1,
			127,
			128,
			300,
			(std::numeric_limits<uint32_t>::max)(),
			(std::numeric_limits<uint64_t>::max)(),
		};

		for (const auto value : values)
		{
			std::array<std::byte, kMaxVarintLength> buf{};
			const int len = PutVarint(buf, value);
			EXPECT_EQ(len, VarintLength(value));

			auto [decoded, decodedLen] = GetVarint(std::span<const std::byte>(buf.data(), static_cast<size_t>(len)));
			EXPECT_EQ(decoded, value);
			EXPECT_EQ(decodedLen, len);
		}
	}

	TEST(Varint, MatchesKnownEncoding)
	{
		std::array<std::byte, kMaxVarintLength> buf{};
		const int len = PutVarint(buf, 300);

		ASSERT_EQ(len, 2);
		EXPECT_EQ(buf[0], std::byte{0xAC});
		EXPECT_EQ(buf[1], std::byte{0x02});
	}

	TEST(Varint, RejectsInvalidInput)
	{
		const std::array<std::byte, 1> truncated = {std::byte{0x80}};
		auto [value, len] = GetVarint(truncated);
		EXPECT_EQ(value, 0);
		EXPECT_EQ(len, 0);

		const std::array<std::byte, 10> overflow = {
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0xFF},
			std::byte{0x02},
		};
		auto [overflowValue, overflowLen] = GetVarint(overflow);
		EXPECT_EQ(overflowValue, 0);
		EXPECT_EQ(overflowLen, 0);
	}

	TEST(Varint, RejectsTooSmallOutputBuffer)
	{
		std::array<std::byte, 1> buf{};
		EXPECT_EQ(PutVarint(buf, 128), 0);
	}

} // namespace
} // namespace bitcask
