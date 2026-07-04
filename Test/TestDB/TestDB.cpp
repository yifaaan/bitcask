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
			dir = MakeUniqueDir();
			std::error_code ec;
			std::filesystem::remove_all(dir, ec); // clean slate in case a prior run left it behind
		}

		void TearDown() override
		{
			std::error_code ec;
			std::filesystem::remove_all(dir, ec);
			std::filesystem::remove_all(MergeDir(), ec);
		}

		Options MakeOptions(uint64_t maxDataFileSize = 10 * 1024 * 1024, bool syncOnWrite = false) const
		{
			Options opt;
			opt.dataDir = dir.string();
			opt.maxDataFileSize = maxDataFileSize;
			opt.syncOnWrite = syncOnWrite;
			opt.indexType = IndexType::BTree;
			return opt;
		}

		std::filesystem::path MergeDir() const
		{
			return dir.parent_path() / (dir.filename().string() + std::string(MergeDirSuffix));
		}

		std::filesystem::path dir;
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

	TEST_F(DBTest, SyncSucceedsAfterWrite)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("k", "v").ok());
		EXPECT_TRUE(db->Sync().ok());
	}

	TEST_F(DBTest, SyncOnEmptyDatabaseIsOk)
	{
		// No writes yet, so there is no active file; Sync() must still succeed.
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		EXPECT_TRUE(db->Sync().ok());
	}

	TEST_F(DBTest, CloseSucceedsAndIsIdempotent)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("k", "v").ok());
		EXPECT_TRUE(db->Close().ok());
		// Calling Close() again must be a no-op, not an error.
		EXPECT_TRUE(db->Close().ok());
	}

	TEST_F(DBTest, OperationsFailAfterClose)
	{
		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("k", "v").ok());
		ASSERT_TRUE(db->Close().ok());

		auto putStatus = db->Put("k2", "v2");
		EXPECT_FALSE(putStatus.ok());
		EXPECT_EQ(putStatus.code(), absl::StatusCode::kFailedPrecondition);

		auto getStatus = db->Get("k");
		EXPECT_FALSE(getStatus.ok());
		EXPECT_EQ(getStatus.status().code(), absl::StatusCode::kFailedPrecondition);

		auto deleteStatus = db->Delete("k");
		EXPECT_FALSE(deleteStatus.ok());
		EXPECT_EQ(deleteStatus.code(), absl::StatusCode::kFailedPrecondition);

		auto syncStatus = db->Sync();
		EXPECT_FALSE(syncStatus.ok());
		EXPECT_EQ(syncStatus.code(), absl::StatusCode::kFailedPrecondition);
	}

	TEST_F(DBTest, DataPersistsAfterExplicitCloseAndReopen)
	{
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			ASSERT_TRUE(db->Put("k1", "v1").ok());
			ASSERT_TRUE(db->Put("k2", "v2").ok());
			ASSERT_TRUE(db->Sync().ok());
			ASSERT_TRUE(db->Close().ok());
		}

		auto dbOr = DB::Open(MakeOptions());
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		auto k1 = db->Get("k1");
		ASSERT_TRUE(k1.ok()) << k1.status();
		EXPECT_EQ(*k1, "v1");

		auto k2 = db->Get("k2");
		ASSERT_TRUE(k2.ok()) << k2.status();
		EXPECT_EQ(*k2, "v2");
	}

	TEST_F(DBTest, MergeKeepsDatabaseUsableInCurrentInstance)
	{
		auto dbOr = DB::Open(MakeOptions(128, /*syncOnWrite=*/true));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		ASSERT_TRUE(db->Put("stale", "v1").ok());
		ASSERT_TRUE(db->Put("stale", "v2").ok());
		ASSERT_TRUE(db->Put("keep", "live").ok());
		ASSERT_TRUE(db->Put("gone", "value").ok());
		ASSERT_TRUE(db->Delete("gone").ok());

		ASSERT_TRUE(db->Merge().ok());
		EXPECT_TRUE(std::filesystem::exists(MergeDir()));

		auto stale = db->Get("stale");
		ASSERT_TRUE(stale.ok()) << stale.status();
		EXPECT_EQ(*stale, "v2");

		auto keep = db->Get("keep");
		ASSERT_TRUE(keep.ok()) << keep.status();
		EXPECT_EQ(*keep, "live");

		auto gone = db->Get("gone");
		EXPECT_FALSE(gone.ok());
		EXPECT_EQ(gone.status().code(), absl::StatusCode::kNotFound);

		ASSERT_TRUE(db->Put("after-merge", "ok").ok());
		auto afterMerge = db->Get("after-merge");
		ASSERT_TRUE(afterMerge.ok()) << afterMerge.status();
		EXPECT_EQ(*afterMerge, "ok");
	}

	TEST_F(DBTest, MergeIsAppliedOnReopen)
	{
		{
			auto dbOr = DB::Open(MakeOptions(160, /*syncOnWrite=*/true));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			for (int i = 0; i < 12; ++i)
			{
				ASSERT_TRUE(db->Put("key-" + std::to_string(i), "old-" + std::to_string(i)).ok());
			}
			for (int i = 0; i < 12; ++i)
			{
				ASSERT_TRUE(db->Put("key-" + std::to_string(i), "new-" + std::to_string(i)).ok());
			}
			ASSERT_TRUE(db->Put("deleted", "value").ok());
			ASSERT_TRUE(db->Delete("deleted").ok());
			ASSERT_TRUE(db->Put("survivor", "live").ok());

			ASSERT_TRUE(db->Merge().ok());
			EXPECT_TRUE(std::filesystem::exists(MergeDir()));
		}

		auto dbOr = DB::Open(MakeOptions(160, /*syncOnWrite=*/true));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		EXPECT_FALSE(std::filesystem::exists(MergeDir()));
		EXPECT_TRUE(std::filesystem::exists(dir / std::string(HintFileName)));

		for (int i = 0; i < 12; ++i)
		{
			SCOPED_TRACE(testing::Message() << "i=" << i);
			auto valOr = db->Get("key-" + std::to_string(i));
			ASSERT_TRUE(valOr.ok()) << valOr.status();
			EXPECT_EQ(*valOr, "new-" + std::to_string(i));
		}

		auto deleted = db->Get("deleted");
		EXPECT_FALSE(deleted.ok());
		EXPECT_EQ(deleted.status().code(), absl::StatusCode::kNotFound);

		auto survivor = db->Get("survivor");
		ASSERT_TRUE(survivor.ok()) << survivor.status();
		EXPECT_EQ(*survivor, "live");

		ASSERT_TRUE(db->Put("post-reopen", "ok").ok());
		auto postReopen = db->Get("post-reopen");
		ASSERT_TRUE(postReopen.ok()) << postReopen.status();
		EXPECT_EQ(*postReopen, "ok");
	}

	TEST_F(DBTest, MergeWithOnlyDeletedKeysReopensEmpty)
	{
		{
			auto dbOr = DB::Open(MakeOptions(128, /*syncOnWrite=*/true));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			ASSERT_TRUE(db->Put("deleted", "value").ok());
			ASSERT_TRUE(db->Delete("deleted").ok());
			ASSERT_TRUE(db->Merge().ok());
			EXPECT_TRUE(std::filesystem::exists(MergeDir()));
		}

		auto dbOr = DB::Open(MakeOptions(128, /*syncOnWrite=*/true));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		EXPECT_FALSE(std::filesystem::exists(MergeDir()));
		auto deleted = db->Get("deleted");
		EXPECT_FALSE(deleted.ok());
		EXPECT_EQ(deleted.status().code(), absl::StatusCode::kNotFound);

		ASSERT_TRUE(db->Put("fresh", "value").ok());
		auto fresh = db->Get("fresh");
		ASSERT_TRUE(fresh.ok()) << fresh.status();
		EXPECT_EQ(*fresh, "value");
	}

	TEST_F(DBTest, MergeLargeDataSetPreservesLatestValuesAcrossReopen)
	{
		constexpr int kCount = 2500;
		constexpr int kDeletedModulo = 5;
		auto makeKey = [](int i) {
			return "large-key-" + std::to_string(i);
		};
		auto makeValue = [](int i, int generation) {
			return "large-value-" + std::to_string(generation) + "-" + std::to_string(i) + "-" +
				   std::string(64, static_cast<char>('a' + (i % 26)));
		};

		{
			auto dbOr = DB::Open(MakeOptions(4096, /*syncOnWrite=*/false));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			for (int i = 0; i < kCount; ++i)
			{
				ASSERT_TRUE(db->Put(makeKey(i), makeValue(i, 0)).ok());
			}
			for (int i = 0; i < kCount; ++i)
			{
				ASSERT_TRUE(db->Put(makeKey(i), makeValue(i, 1)).ok());
			}
			for (int i = 0; i < kCount; ++i)
			{
				if (i % kDeletedModulo == 0)
				{
					ASSERT_TRUE(db->Delete(makeKey(i)).ok());
				}
				else
				{
					ASSERT_TRUE(db->Put(makeKey(i), makeValue(i, 2)).ok());
				}
			}

			ASSERT_TRUE(db->Merge().ok());
			ASSERT_TRUE(db->Put("post-large-merge", "still-here").ok());

			for (int i = 0; i < kCount; ++i)
			{
				SCOPED_TRACE(testing::Message() << "current instance i=" << i);
				auto valOr = db->Get(makeKey(i));
				if (i % kDeletedModulo == 0)
				{
					EXPECT_FALSE(valOr.ok());
					EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
				}
				else
				{
					ASSERT_TRUE(valOr.ok()) << valOr.status();
					EXPECT_EQ(*valOr, makeValue(i, 2));
				}
			}
		}

		auto dbOr = DB::Open(MakeOptions(4096, /*syncOnWrite=*/false));
		ASSERT_TRUE(dbOr.ok()) << dbOr.status();
		auto db = std::move(*dbOr);

		for (int i = 0; i < kCount; ++i)
		{
			SCOPED_TRACE(testing::Message() << "reopened i=" << i);
			auto valOr = db->Get(makeKey(i));
			if (i % kDeletedModulo == 0)
			{
				EXPECT_FALSE(valOr.ok());
				EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
			}
			else
			{
				ASSERT_TRUE(valOr.ok()) << valOr.status();
				EXPECT_EQ(*valOr, makeValue(i, 2));
			}
		}

		auto postMerge = db->Get("post-large-merge");
		ASSERT_TRUE(postMerge.ok()) << postMerge.status();
		EXPECT_EQ(*postMerge, "still-here");
		EXPECT_EQ(db->ListKeys().size(), static_cast<size_t>(kCount - (kCount / kDeletedModulo)) + 1);
	}

} // namespace
} // namespace bitcask
