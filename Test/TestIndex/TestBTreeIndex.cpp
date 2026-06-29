#include "Index/BTreeIndex.h"

#include <absl/status/status.h>
#include <gtest/gtest.h>

#include <string>

namespace bitcask
{
namespace
{

	TEST(BTreeIndex, PutGetDeleteRoundTrip)
	{
		BTreeIndex index;
		const LogRecordPos pos{.fid = 7, .offset = 123, .size = 45};

		EXPECT_TRUE(index.Put("hello", pos).ok());
		EXPECT_EQ(index.Size(), 1u);

		auto got = index.Get("hello");
		ASSERT_TRUE(got.ok()) << got.status();
		EXPECT_EQ(got->fid, pos.fid);
		EXPECT_EQ(got->offset, pos.offset);
		EXPECT_EQ(got->size, pos.size);

		EXPECT_TRUE(index.Delete("hello").ok());
		EXPECT_EQ(index.Size(), 0u);

		auto missing = index.Get("hello");
		EXPECT_FALSE(missing.ok());
		EXPECT_EQ(missing.status().code(), absl::StatusCode::kNotFound);
	}

	TEST(BTreeIndex, OverwritesExistingKey)
	{
		BTreeIndex index;

		EXPECT_TRUE(index.Put("same-key", LogRecordPos{.fid = 1, .offset = 10, .size = 20}).ok());
		EXPECT_TRUE(index.Put("same-key", LogRecordPos{.fid = 2, .offset = 30, .size = 40}).ok());

		EXPECT_EQ(index.Size(), 1u);

		auto got = index.Get("same-key");
		ASSERT_TRUE(got.ok()) << got.status();
		EXPECT_EQ(got->fid, 2u);
		EXPECT_EQ(got->offset, 30);
		EXPECT_EQ(got->size, 40);
	}

	TEST(BTreeIndex, ClearRemovesAllEntries)
	{
		BTreeIndex index;

		EXPECT_TRUE(index.Put("a", LogRecordPos{.fid = 1, .offset = 1, .size = 1}).ok());
		EXPECT_TRUE(index.Put("b", LogRecordPos{.fid = 2, .offset = 2, .size = 2}).ok());
		EXPECT_EQ(index.Size(), 2u);

		index.Clear();

		EXPECT_EQ(index.Size(), 0u);
		EXPECT_FALSE(index.Get("a").ok());
		EXPECT_FALSE(index.Get("b").ok());
	}

	TEST(BTreeIndex, DeleteMissingKeyReturnsNotFound)
	{
		BTreeIndex index;

		auto status = index.Delete("missing");
		EXPECT_FALSE(status.ok());
		EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
	}

} // namespace
} // namespace bitcask
