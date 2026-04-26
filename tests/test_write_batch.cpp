#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>

#include "batch.h"
#include "db.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_write_batch";

    struct WriteBatchFixture
    {
        WriteBatchFixture()
        {
            std::filesystem::remove_all(kTestDir);
            std::filesystem::create_directories(kTestDir);
        }

        ~WriteBatchFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }
    };

    bitcask::WriteBatch MakeBatch(bitcask::DB& db, bitcask::WriteBatchOptions options = {})
    {
        return bitcask::WriteBatch(&db, options);
    }
} // namespace

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch Commit writes pending puts", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);

    auto batch = MakeBatch(*db);
    REQUIRE(batch.Put("alpha", "1").ok());
    REQUIRE(batch.Put("beta", "2").ok());
    REQUIRE_FALSE(db->Get("alpha").has_value());

    REQUIRE(batch.Commit().ok());

    auto alpha = db->Get("alpha");
    auto beta = db->Get("beta");
    REQUIRE(alpha.has_value());
    REQUIRE(beta.has_value());
    REQUIRE(*alpha == "1");
    REQUIRE(*beta == "2");
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch keeps the latest put for a key", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);

    auto batch = MakeBatch(*db);
    REQUIRE(batch.Put("key", "old").ok());
    REQUIRE(batch.Put("key", "new").ok());

    REQUIRE(batch.Commit().ok());

    auto value = db->Get("key");
    REQUIRE(value.has_value());
    REQUIRE(*value == "new");
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch can update and delete existing keys", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);
    REQUIRE(db->Put("keep", "old").ok());
    REQUIRE(db->Put("gone", "value").ok());

    auto batch = MakeBatch(*db);
    REQUIRE(batch.Put("keep", "new").ok());
    REQUIRE(batch.Delete("gone").ok());

    REQUIRE(batch.Commit().ok());

    auto keep = db->Get("keep");
    REQUIRE(keep.has_value());
    REQUIRE(*keep == "new");
    REQUIRE_FALSE(db->Get("gone").has_value());
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch delete of absent key is a no-op", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);

    auto batch = MakeBatch(*db);
    REQUIRE(batch.Delete("missing").ok());

    REQUIRE(batch.Commit().ok());
    REQUIRE_FALSE(db->Get("missing").has_value());
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch put then delete before commit leaves no key", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);

    auto batch = MakeBatch(*db);
    REQUIRE(batch.Put("transient", "value").ok());
    REQUIRE(batch.Delete("transient").ok());

    REQUIRE(batch.Commit().ok());
    REQUIRE_FALSE(db->Get("transient").has_value());
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch rejects empty keys", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);

    auto batch = MakeBatch(*db);
    REQUIRE_FALSE(batch.Put("", "value").ok());
    REQUIRE_FALSE(batch.Delete("").ok());

    REQUIRE(batch.Commit().ok());
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch enforces max batch size before writing", "[write_batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
    REQUIRE(db != nullptr);

    bitcask::WriteBatchOptions options;
    options.max_batch_size = 1;
    auto batch = MakeBatch(*db, options);
    REQUIRE(batch.Put("alpha", "1").ok());
    REQUIRE(batch.Put("beta", "2").ok());

    REQUIRE_FALSE(batch.Commit().ok());
    REQUIRE_FALSE(db->Get("alpha").has_value());
    REQUIRE_FALSE(db->Get("beta").has_value());
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch commit is durable after reopen", "[write_batch]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
        REQUIRE(db != nullptr);

        auto batch = MakeBatch(*db);
        REQUIRE(batch.Put("alpha", "1").ok());
        REQUIRE(batch.Put("beta", "2").ok());
        REQUIRE(batch.Commit().ok());
        db->Close();
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{ .data_dir = kTestDir });
        REQUIRE(db != nullptr);

        auto alpha = db->Get("alpha");
        auto beta = db->Get("beta");
        REQUIRE(alpha.has_value());
        REQUIRE(beta.has_value());
        REQUIRE(*alpha == "1");
        REQUIRE(*beta == "2");
    }
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch honors DB bytes_per_sync when commit sync is disabled", "[write_batch]")
{
    auto options = bitcask::Options{ .data_dir = kTestDir };
    options.bytes_per_sync = 1;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    bitcask::WriteBatchOptions batch_options;
    batch_options.sync_on_commit = false;
    auto batch = MakeBatch(*db, batch_options);
    REQUIRE(batch.Put("alpha", "1").ok());
    REQUIRE(batch.Commit().ok());

    const auto data_file = kTestDir / "000000000.data";
    REQUIRE(std::filesystem::exists(data_file));
    REQUIRE(std::filesystem::file_size(data_file) > 0);
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch put then delete same key tracks reclaimable size", "[write_batch][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);
    REQUIRE(db->Put("existing", "value").ok());

    auto before = db->Stat();
    REQUIRE(before.reclaimable_size == 0);

    bitcask::WriteBatch batch(db.get(), {});
    REQUIRE(batch.Put("existing", "updated").ok());
    REQUIRE(batch.Delete("existing").ok());
    REQUIRE(batch.Commit().ok());

    auto after = db->Stat();
    REQUIRE(after.key_num == 0);
    REQUIRE(after.reclaimable_size > before.reclaimable_size);
    REQUIRE_FALSE(db->Get("existing").has_value());
}

TEST_CASE_METHOD(WriteBatchFixture, "WriteBatch overwrite of existing key tracks reclaimable", "[write_batch][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);
    REQUIRE(db->Put("key", "old").ok());

    bitcask::WriteBatch batch(db.get(), {});
    REQUIRE(batch.Put("key", "new").ok());
    REQUIRE(batch.Commit().ok());

    auto stat = db->Stat();
    REQUIRE(stat.key_num == 1);
    REQUIRE(stat.reclaimable_size > 0);
    auto value = db->Get("key");
    REQUIRE(value.has_value());
    REQUIRE(*value == "new");
}
