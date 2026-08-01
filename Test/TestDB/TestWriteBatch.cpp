#include "DB/DB.h"
#include "DB/WriteBatch.h"

#include "../TestTempDir.h"

#include <absl/status/status.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <system_error>
#include <utility>

namespace bitcask
{
	namespace
	{
		class WriteBatchTest : public ::testing::Test
		{
		protected:
			static std::filesystem::path MakeUniqueDir()
			{
				return test::MakeUniqueTempDir("wb-test");
			}

			void SetUp() override
			{
				dir = MakeUniqueDir();
			}

			void TearDown() override
			{
				std::error_code ec;
				std::filesystem::remove_all(dir, ec);
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

			std::filesystem::path dir;
		};

		constexpr uint64_t kLargeBatchSize = 10000;

		std::string MakeKey(uint64_t i)
		{
			return "key-" + std::to_string(i);
		}

		std::string MakeValue(uint64_t i)
		{
			return "value-" + std::to_string(i);
		}

		TEST_F(WriteBatchTest, CommitEmptyBatchIsOK)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			auto wb = db->NewWriteBatch();
			EXPECT_TRUE(wb->Commit().ok());
		}

		TEST_F(WriteBatchTest, PutEmptyKeyIsInvalid)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			auto wb = db->NewWriteBatch();
			auto status = wb->Put("", "value");
			EXPECT_FALSE(status.ok());
			EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
		}

		TEST_F(WriteBatchTest, DeleteEmptyKeyIsInvalid)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			auto wb = db->NewWriteBatch();
			auto status = wb->Delete("");
			EXPECT_FALSE(status.ok());
			EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
		}

		TEST_F(WriteBatchTest, PutAndCommitWritesValue)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);
			auto wb = db->NewWriteBatch();

			ASSERT_TRUE(wb->Put("k", "v").ok());
			ASSERT_TRUE(wb->Commit().ok());

			auto valOr = db->Get("k");
			ASSERT_TRUE(valOr.ok()) << valOr.status();
			EXPECT_EQ(*valOr, "v");
		}

		TEST_F(WriteBatchTest, BatchWritesMultipleKeysIndependently)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			auto wb = db->NewWriteBatch();
			ASSERT_TRUE(wb->Put("a", "1").ok());
			ASSERT_TRUE(wb->Put("b", "2").ok());
			ASSERT_TRUE(wb->Put("c", "3").ok());
			ASSERT_TRUE(wb->Commit().ok());

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

		TEST_F(WriteBatchTest, OverwriteWithinBatchKeepsLastValue)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			// pendingWrites 按 key 去重,后写覆盖先写。
			auto wb = db->NewWriteBatch();
			ASSERT_TRUE(wb->Put("k", "v1").ok());
			ASSERT_TRUE(wb->Put("k", "v2").ok());
			ASSERT_TRUE(wb->Commit().ok());

			auto valOr = db->Get("k");
			ASSERT_TRUE(valOr.ok()) << valOr.status();
			EXPECT_EQ(*valOr, "v2");
		}

		TEST_F(WriteBatchTest, CommitPersistsAcrossReopen)
		{
			{
				auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
				ASSERT_TRUE(dbOr.ok()) << dbOr.status();
				auto db = std::move(*dbOr);

				WriteBatchOptions opt;
				opt.syncWrites = true;
				auto wb = db->NewWriteBatch(opt);
				ASSERT_TRUE(wb->Put("k1", "v1").ok());
				ASSERT_TRUE(wb->Put("k2", "v2").ok());
				ASSERT_TRUE(wb->Commit().ok());
			}

			// 重开:扫描数据文件,看到 TxnFinished 标记后把事务记录回放到索引。
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

		TEST_F(WriteBatchTest, CommitFailsWhenBatchExceedsMaxBatchNum)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			WriteBatchOptions opt;
			opt.maxBatchNum = 2;
			auto wb = db->NewWriteBatch(opt);
			ASSERT_TRUE(wb->Put("a", "1").ok());
			ASSERT_TRUE(wb->Put("b", "2").ok());
			ASSERT_TRUE(wb->Put("c", "3").ok()); // 超过上限

			auto status = wb->Commit();
			EXPECT_FALSE(status.ok());
			EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);

			// 超限检查发生在写任何记录之前,索引应当为空。
			EXPECT_FALSE(db->Get("a").ok());
		}

		TEST_F(WriteBatchTest, DeleteOnPendingPutStagesTombstone)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			auto wb = db->NewWriteBatch();
			ASSERT_TRUE(wb->Put("k", "v").ok());
			// Delete 覆盖同 key 的 pending Put, 暂存为墓碑并返回 Ok。
			EXPECT_TRUE(wb->Delete("k").ok());

			ASSERT_TRUE(wb->Commit().ok()); // 写入墓碑; "k" 从未真正进入索引
			EXPECT_FALSE(db->Get("k").ok());
		}

		TEST_F(WriteBatchTest, DeleteStagesTombstoneForExistingKey)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);
			ASSERT_TRUE(db->Put("k", "v").ok());

			auto wb = db->NewWriteBatch();
			ASSERT_TRUE(wb->Delete("k").ok());
			ASSERT_TRUE(wb->Commit().ok());

			auto valOr = db->Get("k");
			EXPECT_FALSE(valOr.ok());
			EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
		}

		TEST_F(WriteBatchTest, LargeBatchCommitRoundTrip)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			WriteBatchOptions opt;
			opt.maxBatchNum = kLargeBatchSize + 1; // 默认上限是 10000, 这里放开
			auto wb = db->NewWriteBatch(opt);

			for (uint64_t i = 0; i < kLargeBatchSize; ++i)
			{
				ASSERT_TRUE(wb->Put(MakeKey(i), MakeValue(i)).ok());
			}
			ASSERT_TRUE(wb->Commit().ok());

			for (uint64_t i = 0; i < kLargeBatchSize; ++i)
			{
				SCOPED_TRACE(testing::Message() << "i=" << i);
				auto valOr = db->Get(MakeKey(i));
				ASSERT_TRUE(valOr.ok()) << valOr.status();
				EXPECT_EQ(*valOr, MakeValue(i));
			}
		}

		TEST_F(WriteBatchTest, LargeBatchRotatesActiveFile)
		{
			// 每条记录约 20~25 字节, 1024 字节的文件上限会频繁滚动 active 文件。
			constexpr uint64_t kCount = 2000;
			auto dbOr = DB::Open(MakeOptions(/*maxDataFileSize=*/1024, /*syncOnWrite=*/false));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			WriteBatchOptions opt;
			opt.maxBatchNum = kCount + 1;
			auto wb = db->NewWriteBatch(opt);
			for (uint64_t i = 0; i < kCount; ++i)
			{
				ASSERT_TRUE(wb->Put(MakeKey(i), MakeValue(i)).ok());
			}
			ASSERT_TRUE(wb->Commit().ok());

			// 全量读回: 确认滚动后 olderFiles 与 activeFile 中的记录都能定位。
			for (uint64_t i = 0; i < kCount; ++i)
			{
				SCOPED_TRACE(testing::Message() << "i=" << i);
				auto valOr = db->Get(MakeKey(i));
				ASSERT_TRUE(valOr.ok()) << valOr.status();
				EXPECT_EQ(*valOr, MakeValue(i));
			}
		}

		TEST_F(WriteBatchTest, LargeValueCommit)
		{
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			constexpr size_t kValueSize = 1024 * 1024; // 1 MiB
			std::string big(kValueSize, '\0');
			for (size_t i = 0; i < kValueSize; ++i)
			{
				big[i] = static_cast<char>(i % 251); // 用素数周期填充, 便于逐字节校验
			}

			auto wb = db->NewWriteBatch();
			ASSERT_TRUE(wb->Put("big", big).ok());
			ASSERT_TRUE(wb->Commit().ok());

			auto valOr = db->Get("big");
			ASSERT_TRUE(valOr.ok()) << valOr.status();
			EXPECT_EQ(valOr->size(), kValueSize);
			EXPECT_EQ(*valOr, big);
		}

		TEST_F(WriteBatchTest, LargeBatchPersistsAcrossReopen)
		{
			constexpr uint64_t kCount = 5000;
			{
				auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
				ASSERT_TRUE(dbOr.ok()) << dbOr.status();
				auto db = std::move(*dbOr);

				WriteBatchOptions opt;
				opt.maxBatchNum = kCount + 1;
				opt.syncWrites = true; // 提交时 fsync
				auto wb = db->NewWriteBatch(opt);
				for (uint64_t i = 0; i < kCount; ++i)
				{
					ASSERT_TRUE(wb->Put(MakeKey(i), MakeValue(i)).ok());
				}
				ASSERT_TRUE(wb->Commit().ok());
			}

			// 重开: 回放日志, 遇到 TxnFinished 标记后把整个事务回放进索引。
			auto dbOr = DB::Open(MakeOptions(10 * 1024 * 1024, /*syncOnWrite=*/true));
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			for (uint64_t i = 0; i < kCount; ++i)
			{
				SCOPED_TRACE(testing::Message() << "i=" << i);
				auto valOr = db->Get(MakeKey(i));
				ASSERT_TRUE(valOr.ok()) << valOr.status();
				EXPECT_EQ(*valOr, MakeValue(i));
			}
		}

		TEST_F(WriteBatchTest, ManySequentialBatches)
		{
			constexpr uint64_t kBatches = 100;
			constexpr uint64_t kPerBatch = 100;

			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			for (uint64_t b = 0; b < kBatches; ++b)
			{
				auto wb = db->NewWriteBatch();
				for (uint64_t j = 0; j < kPerBatch; ++j)
				{
					uint64_t id = b * kPerBatch + j;
					ASSERT_TRUE(wb->Put(MakeKey(id), MakeValue(id)).ok());
				}
				ASSERT_TRUE(wb->Commit().ok());
			}

			const uint64_t total = kBatches * kPerBatch;
			for (uint64_t i = 0; i < total; ++i)
			{
				SCOPED_TRACE(testing::Message() << "i=" << i);
				auto valOr = db->Get(MakeKey(i));
				ASSERT_TRUE(valOr.ok()) << valOr.status();
				EXPECT_EQ(*valOr, MakeValue(i));
			}
			EXPECT_EQ(db->ListKeys().size(), total);
		}

		TEST_F(WriteBatchTest, MixedPutDeleteInLargeBatch)
		{
			constexpr uint64_t kCount = 2000;
			auto dbOr = DB::Open(MakeOptions());
			ASSERT_TRUE(dbOr.ok()) << dbOr.status();
			auto db = std::move(*dbOr);

			// 先用普通 Put 写满 kCount 个 key
			for (uint64_t i = 0; i < kCount; ++i)
			{
				ASSERT_TRUE(db->Put(MakeKey(i), MakeValue(i)).ok());
			}

			// 同一批次内: 偶数 key 删除, 奇数 key 覆盖
			WriteBatchOptions opt;
			opt.maxBatchNum = kCount + 1;
			auto wb = db->NewWriteBatch(opt);
			for (uint64_t i = 0; i < kCount; ++i)
			{
				if (i % 2 == 0)
				{
					ASSERT_TRUE(wb->Delete(MakeKey(i)).ok());
				}
				else
				{
					ASSERT_TRUE(wb->Put(MakeKey(i), MakeValue(i) + "-x").ok());
				}
			}
			ASSERT_TRUE(wb->Commit().ok());

			for (uint64_t i = 0; i < kCount; ++i)
			{
				SCOPED_TRACE(testing::Message() << "i=" << i);
				auto valOr = db->Get(MakeKey(i));
				if (i % 2 == 0)
				{
					EXPECT_FALSE(valOr.ok());
					EXPECT_EQ(valOr.status().code(), absl::StatusCode::kNotFound);
				}
				else
				{
					ASSERT_TRUE(valOr.ok()) << valOr.status();
					EXPECT_EQ(*valOr, MakeValue(i) + "-x");
				}
			}
		}
	} // namespace

} // namespace bitcask
