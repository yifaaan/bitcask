#include "DB/DB.h"

#include <absl/status/status.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace bitcask
{
namespace
{

	// Each test gets its own throwaway directory under the system temp path so
	// the on-disk data files never collide with another test or a prior run.
	class DBTest : public ::testing::Test
	{
	protected:
		static std::filesystem::path MakeUniqueDir()
		{
			static std::atomic<uint64_t> counter{0};
			auto name = std::string{"db-test-"} + std::to_string(counter.fetch_add(1));
			return std::filesystem::temp_directory_path() / "bitcask-db-tests" / name;
		}

		void SetUp() override
		{
			dir_ = MakeUniqueDir();
			std::error_code ec;
			std::filesystem::remove_all(dir_, ec); // clean slate in case a prior run left it behind
		}

		void TearDown() override
		{
			std::error_code ec;
			std::filesystem::remove_all(dir_, ec);
		}

		Options MakeOptions(uint64_t maxDataFileSize = 10 * 1024 * 1024, bool syncOnWrite = false) const
		{
			Options opt;
			opt.dataDir = dir_.string();
			opt.maxDataFileSize = maxDataFileSize;
			opt.syncOnWrite = syncOnWrite;
			opt.indexType = IndexType::BTree;
			return opt;
		}

		std::filesystem::path dir_;
	};

	TEST_F(DBTest, OpenCreatesDatabaseOnEmptyDirectory)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		EXPECT_NE(*dbOr, nullptr);
	}

	TEST_F(DBTest, OpenFailsWithEmptyDataDir)
	{
		Options opt;
		opt.dataDir = "";

		auto dbOr = DB::Open(opt);
		EXPECT_FALSE(dbOr.ok());
		EXPECT_EQ(dbOr.status().code(), absl::StatusCode::kInvalidArgument);
	}

	TEST_F(DBTest, PutAndGetRoundTrip)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("name", "bitcask").ok());
		auto valOr = db->Get("name");
		ASSERT_TRUE(valOr.ok()) << valOr.status();
		EXPECT_EQ(*valOr, "bitcask");
	}

	TEST_F(DBTest, GetMissingKeyReturnsNotFound)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto valOr = db->Get("missing");
		EXPECT_FALSE(valOr.ok());
		EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
	}

	TEST_F(DBTest, PutEmptyKeyIsInvalid)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto status = db->Put("", "value");
		EXPECT_FALSE(status.ok());
		EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
	}

	TEST_F(DBTest, GetEmptyKeyIsInvalid)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto valOr = db->Get("");
		EXPECT_FALSE(valOr.ok());
		EXPECT_EQ(valOr.status().code(), absl::StatusCode::kInvalidArgument);
	}

	TEST_F(DBTest, DeleteEmptyKeyIsInvalid)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto status = db->Delete("");
		EXPECT_FALSE(status.ok());
		EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
	}

	TEST_F(DBTest, PutAcceptsEmptyValue)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("empty", "").ok());
		auto valOr = db->Get("empty");
		ASSERT_TRUE(valOr.ok()) << valOr.status();
		EXPECT_EQ(*valOr, "");
	}

	TEST_F(DBTest, PutOverwritesExistingKey)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("k", "v1").ok());
		ASSERT_TRUE(db->Put("k", "v2").ok());

		auto valOr = db->Get("k");
		ASSERT_TRUE(valOr.ok()) << valOr.status();
		EXPECT_EQ(*valOr, "v2");
	}

	TEST_F(DBTest, StoresMultipleKeysIndependently)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("a", "1").ok());
		ASSERT_TRUE(db->Put("b", "2").ok());
		ASSERT_TRUE(db->Put("c", "3").ok());

		auto a = db->Get("a");
		ASSERT_TRUE(a.ok()) << a.status();
		EXPECT_EQ(*a, "1");

		auto b = db->Get("b");
		ASSERT_TRUE(b.ok()) << b.status();
		EXPECT_EQ(*b, "2");

		auto c = db->Get("c");
		ASSERT_TRUE(c.ok()) << c.status();
		EXPECT_EQ(*c, "3");
	}

	TEST_F(DBTest, DeleteRemovesKey)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("k", "v").ok());
		ASSERT_TRUE(db->Delete("k").ok());

		auto valOr = db->Get("k");
		EXPECT_FALSE(valOr.ok());
		EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
	}

	TEST_F(DBTest, DeleteMissingKeyIsOk)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		// Deleting a key that was never written is a no-op and must not fail.
		EXPECT_TRUE(db->Delete("nope").ok());
	}

	TEST_F(DBTest, PutPersistsAcrossReopen)
	{
		{
			auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			ASSERT_TRUE(db->Put("k1", "v1").ok());
			ASSERT_TRUE(db->Put("k2", "v2").ok());
		}

		// Reopen on the same directory: data files are rescanned and the index rebuilt.
		auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto k1 = db->Get("k1");
		ASSERT_TRUE(k1.ok()) << k1.status();
		EXPECT_EQ(*k1, "v1");

		auto k2 = db->Get("k2");
		ASSERT_TRUE(k2.ok()) << k2.status();
		EXPECT_EQ(*k2, "v2");
	}

	TEST_F(DBTest, DeletePersistsAcrossReopen)
	{
		{
			auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			ASSERT_TRUE(db->Put("k", "v").ok());
			ASSERT_TRUE(db->Delete("k").ok());
		}

		auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto valOr = db->Get("k");
		EXPECT_FALSE(valOr.ok());
		EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
	}

	TEST_F(DBTest, RotatesDataFileAndReadsAcrossFiles)
	{
		// A "keyN"/"valueN" record encodes to 17 bytes (7-byte header + 4 + 6).
		// A 64-byte cap holds exactly three records, so the fourth write rotates
		// the active file into olderFiles and opens a new one.
		auto dbOr = DB::Open(MakeOptions(64, /*syncOnWrite=*/true));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("key1", "value1").ok());
		ASSERT_TRUE(db->Put("key2", "value2").ok());
		ASSERT_TRUE(db->Put("key3", "value3").ok());
		ASSERT_TRUE(db->Put("key4", "value4").ok()); // triggers rotation

		// key1..3 now live in an older (read-only) file; key4 in the active file.
		auto k1 = db->Get("key1");
		ASSERT_TRUE(k1.ok()) << k1.status();
		EXPECT_EQ(*k1, "value1");

		auto k2 = db->Get("key2");
		ASSERT_TRUE(k2.ok()) << k2.status();
		EXPECT_EQ(*k2, "value2");

		auto k3 = db->Get("key3");
		ASSERT_TRUE(k3.ok()) << k3.status();
		EXPECT_EQ(*k3, "value3");

		auto k4 = db->Get("key4");
		ASSERT_TRUE(k4.ok()) << k4.status();
		EXPECT_EQ(*k4, "value4");
	}

} // namespace
} // namespace bitcask
