#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "batch.h"
#include "db.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_merge";
    const auto kMergeDir = kTestDir / "-merge";

    struct MergeFixture
    {
        MergeFixture()
        {
            std::filesystem::remove_all(kTestDir);
            std::filesystem::create_directories(kTestDir);
        }

        ~MergeFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }
    };

    bitcask::Options MakeOptions(bitcask::IndexType index_type, uint64_t max_data_file_size = 1024 * 1024 * 10)
    {
        auto options = bitcask::Options{.data_dir = kTestDir, .max_data_file_size = max_data_file_size};
        options.index_type = index_type;
        return options;
    }

    bitcask::Options SectionOptions(uint64_t max_data_file_size = 1024 * 1024 * 10)
    {
        auto index_type = bitcask::IndexType::BTree;
        SECTION("BTree")
        {
            index_type = bitcask::IndexType::BTree;
        }
        SECTION("ART")
        {
            index_type = bitcask::IndexType::ART;
        }
        return MakeOptions(index_type, max_data_file_size);
    }

    void RequireValue(bitcask::DB& db, std::string_view key, std::string_view expected)
    {
        auto value = db.Get(key);
        REQUIRE(value.has_value());
        REQUIRE(*value == std::string(expected));
    }

    size_t CountDataFiles()
    {
        size_t count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(kTestDir))
        {
            if (entry.is_regular_file() && entry.path().filename().string().ends_with(bitcask::kDataFileNameSuffix))
            {
                ++count;
            }
        }
        return count;
    }

    void WriteDummyFile(const std::filesystem::path& path)
    {
        std::ofstream file(path, std::ios::binary);
        file << 'x';
        REQUIRE(file.good());
    }
} // namespace

TEST_CASE_METHOD(MergeFixture, "DB Merge keeps latest live records after reopen", "[db][merge]")
{
    auto options = SectionOptions();
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("alpha", std::string(40, 'a')).ok());
    REQUIRE(db->Put("beta", std::string(40, 'b')).ok());
    REQUIRE(db->Put("stale", "old").ok());
    REQUIRE(db->Put("removed", std::string(40, 'r')).ok());
    REQUIRE(db->Put("stale", "new").ok());
    REQUIRE(db->Delete("removed").ok());

    REQUIRE(db->Merge().ok());
    REQUIRE(std::filesystem::exists(kMergeDir));

    REQUIRE(db->Put("after_merge", "still here").ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));

    RequireValue(*db, "alpha", std::string(40, 'a'));
    RequireValue(*db, "beta", std::string(40, 'b'));
    RequireValue(*db, "stale", "new");
    RequireValue(*db, "after_merge", "still here");
    REQUIRE_FALSE(db->Get("removed").has_value());

    const auto expected = std::vector<std::string>{"after_merge", "alpha", "beta", "stale"};
    REQUIRE(db->ListKeys() == expected);
}

TEST_CASE_METHOD(MergeFixture, "DB Merge on empty database is a no-op", "[db][merge]")
{
    auto db = bitcask::DB::Open(SectionOptions());
    REQUIRE(db != nullptr);

    REQUIRE(db->Merge().ok());

    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    REQUIRE(db->ListKeys().empty());
}

TEST_CASE_METHOD(MergeFixture, "DB Open removes incomplete merge directory", "[db][merge]")
{
    auto options = SectionOptions();
    {
        auto db = bitcask::DB::Open(options);
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("survivor", "value").ok());
        db->Close();
    }

    std::filesystem::create_directories(kMergeDir);
    WriteDummyFile(kMergeDir / "000000000.data");

    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    RequireValue(*db, "survivor", "value");
}

TEST_CASE_METHOD(MergeFixture, "DB Open installs completed merge files", "[db][merge]")
{
    auto options = SectionOptions();
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(db->Put("key", "old").ok());
    REQUIRE(db->Put("key", "new").ok());

    REQUIRE(db->Merge().ok());
    REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));
    REQUIRE(std::filesystem::exists(kMergeDir / "hint-index"));
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    REQUIRE(std::filesystem::exists(kTestDir / "merge-finished"));
    REQUIRE(std::filesystem::exists(kTestDir / "hint-index"));
    REQUIRE(CountDataFiles() == 2);
    RequireValue(*db, "key", "new");
}

TEST_CASE_METHOD(MergeFixture, "DB Merge compacts multiple data files", "[db][merge]")
{
    auto options = SectionOptions(128);
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    for (int i = 0; i < 12; ++i)
    {
        REQUIRE(db->Put("key" + std::to_string(i), std::string(24, static_cast<char>('a' + i))).ok());
    }
    REQUIRE(db->Put("key3", "latest").ok());
    REQUIRE(db->Delete("key5").ok());
    REQUIRE(CountDataFiles() > 1);

    REQUIRE(db->Merge().ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));

    for (int i = 0; i < 12; ++i)
    {
        const auto key = "key" + std::to_string(i);
        if (i == 5)
        {
            REQUIRE_FALSE(db->Get(key).has_value());
        }
        else if (i == 3)
        {
            RequireValue(*db, key, "latest");
        }
        else
        {
            RequireValue(*db, key, std::string(24, static_cast<char>('a' + i)));
        }
    }
}

TEST_CASE_METHOD(MergeFixture, "DB Merge preserves committed write batch records", "[db][merge][write_batch]")
{
    auto options = SectionOptions();
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    bitcask::WriteBatch batch(db.get(), {});
    REQUIRE(batch.Put("batch_alive", "old").ok());
    REQUIRE(batch.Put("batch_removed", "gone").ok());
    REQUIRE(batch.Commit().ok());

    bitcask::WriteBatch update(db.get(), {});
    REQUIRE(update.Put("batch_alive", "new").ok());
    REQUIRE(update.Delete("batch_removed").ok());
    REQUIRE(update.Commit().ok());

    REQUIRE(db->Merge().ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    RequireValue(*db, "batch_alive", "new");
    REQUIRE_FALSE(db->Get("batch_removed").has_value());
}

TEST_CASE_METHOD(MergeFixture, "DB Merge can run repeatedly", "[db][merge]")
{
    auto options = SectionOptions();
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Merge().ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(db->Put("alpha", "2").ok());
    REQUIRE(db->Put("beta", "3").ok());
    REQUIRE(db->Merge().ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    RequireValue(*db, "alpha", "2");
    RequireValue(*db, "beta", "3");
}

TEST_CASE_METHOD(MergeFixture, "DB auto merge runs when reclaimable ratio reaches threshold", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::BTree);
    options.auto_merge_reclaim_ratio = 0.01;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key", std::string(64, 'a')).ok());
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir / "merge-finished"));

    REQUIRE(db->Put("key", std::string(64, 'b')).ok());
    REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));
}

TEST_CASE_METHOD(MergeFixture, "DB auto merge with ART runs when reclaimable ratio reaches threshold", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::ART);
    options.auto_merge_reclaim_ratio = 0.01;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key", std::string(64, 'a')).ok());
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir / "merge-finished"));

    REQUIRE(db->Put("key", std::string(64, 'b')).ok());
    REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));
}

TEST_CASE_METHOD(MergeFixture, "DB auto merge does not trigger when disabled", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::BTree);
    options.auto_merge_reclaim_ratio = 0.0;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key", std::string(64, 'a')).ok());
    REQUIRE(db->Put("key", std::string(64, 'b')).ok());
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir / "merge-finished"));
}

TEST_CASE_METHOD(MergeFixture, "DB auto merge skips when merge directory exists after completed merge", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::BTree);
    options.auto_merge_reclaim_ratio = 0.01;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    // First write + overwrite triggers auto-merge
    REQUIRE(db->Put("key", std::string(64, 'a')).ok());
    REQUIRE(db->Put("key", std::string(64, 'b')).ok());
    REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));

    // Second overwrite should NOT trigger another merge because merge dir still exists
    REQUIRE(db->Put("key", std::string(64, 'c')).ok());
    // The merge dir still has the original merge-finished, confirming no new merge started
    REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));
}

TEST_CASE_METHOD(MergeFixture, "DB Merge estimates required space for data files", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::BTree);
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("persist", std::string(256, 'x')).ok());
    REQUIRE(db->Put("discard", std::string(256, 'y')).ok());
    REQUIRE(db->Put("discard", std::string(256, 'z')).ok());

    REQUIRE(db->Merge().ok());
    REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));
    REQUIRE(std::filesystem::exists(kMergeDir / "hint-index"));
    // Merged data file exists in the merge directory
    REQUIRE(std::filesystem::exists(kMergeDir / "000000000.data"));
}

TEST_CASE_METHOD(MergeFixture, "DB Merge handles hint file after auto-merge reopen", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::BTree);
    {
        auto db = bitcask::DB::Open(options);
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key", std::string(80, 'k')).ok());
        REQUIRE(db->Put("key", std::string(80, 'm')).ok());
        REQUIRE(db->Merge().ok());
        REQUIRE(std::filesystem::exists(kMergeDir / "hint-index"));
    }

    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(std::filesystem::exists(kTestDir / "hint-index"));
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    RequireValue(*db, "key", std::string(80, 'm'));
}

TEST_CASE_METHOD(MergeFixture, "DB Merge preserves index across reopen when hint file exists", "[db][merge]")
{
    auto options = MakeOptions(bitcask::IndexType::ART);
    options.auto_merge_reclaim_ratio = 0.01;
    {
        auto db = bitcask::DB::Open(options);
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("persistent", "value").ok());
        REQUIRE(db->Put("transient", "old").ok());
        REQUIRE(db->Put("transient", "new").ok());
        REQUIRE(db->Put("persistent", "fresh").ok());
        REQUIRE(std::filesystem::exists(kMergeDir / "merge-finished"));
    }

    auto fresh_db = bitcask::DB::Open(options);
    REQUIRE(fresh_db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    auto keys = fresh_db->ListKeys();
    REQUIRE(keys.size() == 2);
    RequireValue(*fresh_db, "persistent", "fresh");
    RequireValue(*fresh_db, "transient", "new");
}

TEST_CASE_METHOD(MergeFixture, "DB reclaimable size is zero after reopen with hint file", "[db][merge][stat]")
{
    auto options = MakeOptions(bitcask::IndexType::BTree);
    {
        auto db = bitcask::DB::Open(options);
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key", "old").ok());
        REQUIRE(db->Put("key", "new").ok());
        REQUIRE(db->Merge().ok());
        db->Close();
    }

    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(std::filesystem::exists(kTestDir / "hint-index"));
    auto stat = db->Stat();
    REQUIRE(stat.reclaimable_size == 0);
    REQUIRE(stat.key_num == 1);
    RequireValue(*db, "key", "new");
}

TEST_CASE_METHOD(MergeFixture, "DB Merge with all unique live records preserves everything", "[db][merge]")
{
    auto options = SectionOptions();
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    for (int i = 0; i < 10; ++i)
    {
        REQUIRE(db->Put("key" + std::to_string(i), std::to_string(i)).ok());
    }

    REQUIRE(db->Stat().reclaimable_size == 0);
    REQUIRE(db->Merge().ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(db->Stat().key_num == 10);
    for (int i = 0; i < 10; ++i)
    {
        RequireValue(*db, "key" + std::to_string(i), std::to_string(i));
    }
}

TEST_CASE_METHOD(MergeFixture, "DB Merge with file containing only deleted records", "[db][merge]")
{
    auto options = SectionOptions(128);
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    for (int i = 0; i < 8; ++i)
    {
        REQUIRE(db->Put("key" + std::to_string(i), std::string(32, 'x')).ok());
    }
    REQUIRE(db->Delete("key0").ok());
    REQUIRE(db->Delete("key1").ok());
    REQUIRE(db->Delete("key2").ok());

    REQUIRE(db->Stat().reclaimable_size > 0);
    REQUIRE(db->Merge().ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE(db->Stat().reclaimable_size == 0);
    REQUIRE(db->Stat().key_num == 5);
    for (int i = 3; i < 8; ++i)
    {
        RequireValue(*db, "key" + std::to_string(i), std::string(32, 'x'));
    }
    REQUIRE_FALSE(db->Get("key0").has_value());
    REQUIRE_FALSE(db->Get("key1").has_value());
    REQUIRE_FALSE(db->Get("key2").has_value());
}
