#include "DB/DB.h"
#include "Index/BTreeIndex.h"

#include <absl/status/status.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

namespace bitcask
{
namespace
{

	TEST(BTreeIndex, PutGetDeleteRoundTrip)
	{
		BTreeIndex index;
		const LogRecordPos pos{.fid = 7, .offset = 123, .size = 45};

		EXPECT_FALSE(index.Put("hello", pos).has_value());
		EXPECT_EQ(index.Size(), 1u);

		auto got = index.Get("hello");
		ASSERT_TRUE(got.ok()) << got.status();
		EXPECT_EQ(got->fid, pos.fid);
		EXPECT_EQ(got->offset, pos.offset);
		EXPECT_EQ(got->size, pos.size);

		EXPECT_TRUE(index.Delete("hello").has_value());
		EXPECT_EQ(index.Size(), 0u);

		auto missing = index.Get("hello");
		EXPECT_FALSE(missing.ok());
		EXPECT_EQ(missing.status().code(), absl::StatusCode::kNotFound);
	}

	TEST(BTreeIndex, OverwritesExistingKey)
	{
		BTreeIndex index;

		EXPECT_FALSE(index.Put("same-key", LogRecordPos{.fid = 1, .offset = 10, .size = 20}).has_value());
		auto oldPos = index.Put("same-key", LogRecordPos{.fid = 2, .offset = 30, .size = 40});
		ASSERT_TRUE(oldPos.has_value());
		EXPECT_EQ(oldPos->fid, 1);
		EXPECT_EQ(oldPos->offset, 10);
		EXPECT_EQ(oldPos->size, 20);

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

		EXPECT_FALSE(index.Put("a", LogRecordPos{.fid = 1, .offset = 1, .size = 1}).has_value());
		EXPECT_FALSE(index.Put("b", LogRecordPos{.fid = 2, .offset = 2, .size = 2}).has_value());
		EXPECT_EQ(index.Size(), 2u);

		index.Clear();

		EXPECT_EQ(index.Size(), 0u);
		EXPECT_FALSE(index.Get("a").ok());
		EXPECT_FALSE(index.Get("b").ok());
	}

	TEST(BTreeIndex, DeleteMissingKeyReturnsNullopt)
	{
		BTreeIndex index;

		EXPECT_FALSE(index.Delete("missing").has_value());
	}

	TEST(BTreeIndexIterator, EmptyIterator)
	{
		BTreeIndexIterator it({});
		EXPECT_FALSE(it.Valid());

		it.Seek("anything");
		EXPECT_FALSE(it.Valid());
	}

	TEST(BTreeIndexIterator, SeekExactMatch)
	{
		BTreeIndexIterator it(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});

		it.Seek("b");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "b");
		EXPECT_EQ(it.Value().fid, 2);
		EXPECT_EQ(it.Value().offset, 20);
		EXPECT_EQ(it.Value().size, 200);
	}

	TEST(BTreeIndexIterator, SeekBetweenKeys)
	{
		BTreeIndexIterator it(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"c", {3, 30, 300}},
		});

		it.Seek("b");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");
	}

	TEST(BTreeIndexIterator, SeekBeforeFirst)
	{
		BTreeIndexIterator it(std::vector<IndexEntry>{
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});

		it.Seek("a");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "b");
	}

	TEST(BTreeIndexIterator, SeekBeyondLast)
	{
		BTreeIndexIterator it(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
		});

		it.Seek("z");
		EXPECT_FALSE(it.Valid());
	}

	TEST(BTreeIndexIterator, NextAndPrev)
	{
		BTreeIndexIterator it(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "a");

		it.Next();
		EXPECT_EQ(it.Key(), "b");

		it.Next();
		EXPECT_EQ(it.Key(), "c");

		it.Next();
		EXPECT_FALSE(it.Valid());

		// Prev from past-end goes back to last
		it.Prev();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Prev();
		EXPECT_EQ(it.Key(), "b");

		it.Prev();
		EXPECT_EQ(it.Key(), "a");

		// Prev from first element wraps to invalid
		it.Prev();
		EXPECT_FALSE(it.Valid());
	}

	TEST(BTreeIndexIterator, RewindResetsToBeginning)
	{
		BTreeIndexIterator it(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
		});

		it.Seek("b");
		EXPECT_EQ(it.Key(), "b");

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "a");
	}

	// === Iterator (user-facing) tests ===

	TEST(Iterator, ForwardRewindAll)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)), {});

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "a");

		it.Next();
		EXPECT_EQ(it.Key(), "b");
		it.Next();
		EXPECT_EQ(it.Key(), "c");
		it.Next();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ForwardSeek)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"c", {3, 30, 300}},
			{"e", {5, 50, 500}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)), {});

		it.Seek("c");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Seek("d");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "e");

		it.Seek("z");
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, PrefixFilterForward)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"foo.a", {1, 10, 100}},
			{"foo.b", {2, 20, 200}},
			{"bar.c", {3, 30, 300}},
			{"foo.d", {4, 40, 400}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.prefix = "foo."});

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "foo.a");

		it.Next();
		EXPECT_EQ(it.Key(), "foo.b");
		it.Next();
		EXPECT_EQ(it.Key(), "foo.d");
		it.Next();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, PrefixFilterSeek)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"foo.a", {1, 10, 100}},
			{"foo.c", {3, 30, 300}},
			{"foo.e", {5, 50, 500}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.prefix = "foo."});

		it.Seek("foo.c");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "foo.c");

		it.Seek("foo.b");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "foo.c");

		it.Seek("foo.z");
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, PrefixNoMatch)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.prefix = "z"});

		it.Rewind();
		EXPECT_FALSE(it.Valid());

		it.Seek("a");
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ReverseAll)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.reverse = true});

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Next();
		EXPECT_EQ(it.Key(), "b");
		it.Next();
		EXPECT_EQ(it.Key(), "a");
		it.Next();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ReverseSeek)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"c", {3, 30, 300}},
			{"e", {5, 50, 500}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.reverse = true});

		it.Seek("c");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Seek("d");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Seek("a");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "a");

		it.Seek("`"); // before 'a'
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ReversePrefix)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"foo.a", {1, 10, 100}},
			{"foo.b", {2, 20, 200}},
			{"bar.c", {3, 30, 300}},
			{"foo.d", {4, 40, 400}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.prefix = "foo.", .reverse = true});

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "foo.d");

		it.Next();
		EXPECT_EQ(it.Key(), "foo.b");
		it.Next();
		EXPECT_EQ(it.Key(), "foo.a");
		it.Next();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ForwardPrev)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)), {});

		it.Seek("c");
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Prev();
		EXPECT_EQ(it.Key(), "b");
		it.Prev();
		EXPECT_EQ(it.Key(), "a");
		it.Prev();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ReversePrev)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"a", {1, 10, 100}},
			{"b", {2, 20, 200}},
			{"c", {3, 30, 300}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.reverse = true});

		it.Rewind();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "c");

		it.Next();
		EXPECT_EQ(it.Key(), "b");

		it.Prev();
		EXPECT_EQ(it.Key(), "c");

		it.Prev();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, PrefixFilterPrev)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"foo.a", {1, 10, 100}},
			{"foo.b", {2, 20, 200}},
			{"bar.c", {3, 30, 300}},
			{"foo.d", {4, 40, 400}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)),
			{.prefix = "foo."});

		it.Rewind();
		it.Next();
		it.Next();
		ASSERT_TRUE(it.Valid());
		EXPECT_EQ(it.Key(), "foo.d");

		it.Prev();
		EXPECT_EQ(it.Key(), "foo.b");
		it.Prev();
		EXPECT_EQ(it.Key(), "foo.a");
		it.Prev();
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, EmptyUnderlyingIterator)
	{
		Iterator it(std::make_unique<BTreeIndexIterator>(std::vector<IndexEntry>{}), {});

		EXPECT_FALSE(it.Valid());

		it.Rewind();
		EXPECT_FALSE(it.Valid());

		it.Seek("anything");
		EXPECT_FALSE(it.Valid());
	}

	TEST(Iterator, ValueWithoutReaderReturnsError)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"k", {1, 10, 100}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)), {});
		it.Rewind();
		ASSERT_TRUE(it.Valid());
		auto val = it.Value();
		EXPECT_FALSE(val.ok());
		EXPECT_EQ(val.status().code(), absl::StatusCode::kInternal);
	}

	TEST(Iterator, ValueWithReader)
	{
		BTreeIndexIterator inner(std::vector<IndexEntry>{
			{"k", {1, 10, 100}},
		});
		Iterator it(std::make_unique<BTreeIndexIterator>(std::move(inner)), {},
			[](const LogRecordPos&) -> absl::StatusOr<std::string> {
				return "read-value";
			});
		it.Rewind();
		ASSERT_TRUE(it.Valid());
		auto val = it.Value();
		ASSERT_TRUE(val.ok()) << val.status();
		EXPECT_EQ(*val, "read-value");
	}

	TEST(Iterator, ValueFromDB)
	{
		std::error_code ec;
		auto dir = std::filesystem::temp_directory_path() / "bitcask-iter-value-test";
		std::filesystem::remove_all(dir, ec);

		auto dbOr = DB::Open(Options{.dataDir = dir.string(), .indexType = IndexType::BTree});
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto& db = **dbOr;

		ASSERT_TRUE(db.Put("a", "val-a").ok());
		ASSERT_TRUE(db.Put("b", "val-b").ok());
		ASSERT_TRUE(db.Put("c", "val-c").ok());

		auto iter = db.NewIterator({});
		ASSERT_NE(iter, nullptr);

		iter->Rewind();
		ASSERT_TRUE(iter->Valid());
		EXPECT_EQ(iter->Key(), "a");
		{
			auto v = iter->Value();
			ASSERT_TRUE(v.ok()) << v.status();
			EXPECT_EQ(*v, "val-a");
		}

		iter->Next();
		{
			auto v = iter->Value();
			ASSERT_TRUE(v.ok()) << v.status();
			EXPECT_EQ(*v, "val-b");
		}

		iter->Seek("c");
		{
			auto v = iter->Value();
			ASSERT_TRUE(v.ok()) << v.status();
			EXPECT_EQ(*v, "val-c");
		}

		iter->Seek("z");
		EXPECT_FALSE(iter->Valid());

		std::filesystem::remove_all(dir, ec);
	}

	TEST(Iterator, IteratorFromBTreeIndex)
	{
		BTreeIndex index;
		EXPECT_FALSE(index.Put("b", LogRecordPos{2, 20, 200}).has_value());
		EXPECT_FALSE(index.Put("a", LogRecordPos{1, 10, 100}).has_value());
		EXPECT_FALSE(index.Put("c", LogRecordPos{3, 30, 300}).has_value());

		auto it = index.NewIterator();
		ASSERT_NE(it, nullptr);

		it->Rewind();
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->Key(), "a");

		it->Seek("b");
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->Key(), "b");

		it->Seek("z");
		EXPECT_FALSE(it->Valid());
	}

} // namespace
} // namespace bitcask
