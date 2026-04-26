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
