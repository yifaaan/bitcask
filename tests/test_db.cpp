#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "batch.h"
#include "db.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_db";
    const auto kMergeDir = kTestDir / "-merge";
    using Entry = std::pair<std::string, std::string>;

    struct DBFixture
    {
        DBFixture()
        {
            std::filesystem::remove_all(kTestDir);
            std::filesystem::create_directories(kTestDir);
        }
        ~DBFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }
    };

    std::vector<Entry> CollectEntries(bitcask::Iterator& iterator)
    {
        std::vector<Entry> entries;
        for (iterator.Rewind(); iterator.Valid(); iterator.Next())
        {
            auto value = iterator.Value();
            REQUIRE(value.has_value());
            entries.emplace_back(std::string(iterator.Key()), *value);
        }
        return entries;
    }

    bitcask::Options MakeOptions(uint64_t max_data_file_size = 1024 * 1024 * 10)
    {
        return bitcask::Options{.data_dir = kTestDir, .max_data_file_size = max_data_file_size};
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

TEST_CASE_METHOD(DBFixture, "DB Open creates directory", "[db]")
{
    auto new_dir = kTestDir / "new_db";
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = new_dir});
    REQUIRE(db != nullptr);
    REQUIRE(std::filesystem::exists(new_dir));
}

TEST_CASE_METHOD(DBFixture, "DB Open with empty path returns nullptr", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = ""});
    REQUIRE(db == nullptr);
}

TEST_CASE_METHOD(DBFixture, "DB Put and Get", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    auto status = db->Put("key1", "value1");
    REQUIRE(status.ok());

    auto value = db->Get("key1");
    REQUIRE(value.has_value());
    REQUIRE(*value == "value1");
}

TEST_CASE_METHOD(DBFixture, "DB Put overwrites existing key", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());
    REQUIRE(db->Put("key1", "value2").ok());

    auto value = db->Get("key1");
    REQUIRE(value.has_value());
    REQUIRE(*value == "value2");
}

TEST_CASE_METHOD(DBFixture, "DB Get nonexistent key", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    auto value = db->Get("no_such_key");
    REQUIRE_FALSE(value.has_value());
}

TEST_CASE_METHOD(DBFixture, "DB Put with empty key fails", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    auto status = db->Put("", "value");
    REQUIRE_FALSE(status.ok());
}

TEST_CASE_METHOD(DBFixture, "DB Delete", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());
    auto status = db->Delete("key1");
    REQUIRE(status.ok());

    auto value = db->Get("key1");
    REQUIRE_FALSE(value.has_value());
}

TEST_CASE_METHOD(DBFixture, "DB Delete nonexistent key is ok", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    auto status = db->Delete("no_such_key");
    REQUIRE(status.ok());
}

TEST_CASE_METHOD(DBFixture, "DB Close and reopen preserves data", "[db]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key1", "value1").ok());
        REQUIRE(db->Put("key2", "value2").ok());
        db->Close();
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        auto v1 = db->Get("key1");
        REQUIRE(v1.has_value());
        REQUIRE(*v1 == "value1");

        auto v2 = db->Get("key2");
        REQUIRE(v2.has_value());
        REQUIRE(*v2 == "value2");
    }
}

TEST_CASE_METHOD(DBFixture, "DB Close and reopen with delete preserves correctly", "[db]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key1", "value1").ok());
        REQUIRE(db->Put("key2", "value2").ok());
        REQUIRE(db->Delete("key1").ok());
        db->Close();
    }

    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        auto v1 = db->Get("key1");
        REQUIRE_FALSE(v1.has_value());

        auto v2 = db->Get("key2");
        REQUIRE(v2.has_value());
        REQUIRE(*v2 == "value2");
    }
}

TEST_CASE_METHOD(DBFixture, "DB Iterator scans keys and values in sorted order", "[db][iterator]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("gamma", "3").ok());
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());

    auto iterator = db->NewIterator();
    REQUIRE(iterator != nullptr);

    const auto expected = std::vector<Entry>{
        {"alpha", "1"},
        {"beta", "2"},
        {"gamma", "3"},
    };
    REQUIRE(CollectEntries(*iterator) == expected);
    REQUIRE_FALSE(iterator->Valid());
}

TEST_CASE_METHOD(DBFixture, "DB Iterator scans keys and values in reverse order", "[db][iterator]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("gamma", "3").ok());
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());

    bitcask::IteratorOptions options;
    options.reverse = true;
    auto iterator = db->NewIterator(options);
    REQUIRE(iterator != nullptr);

    const auto expected = std::vector<Entry>{
        {"gamma", "3"},
        {"beta", "2"},
        {"alpha", "1"},
    };
    REQUIRE(CollectEntries(*iterator) == expected);
    REQUIRE_FALSE(iterator->Valid());
}

TEST_CASE_METHOD(DBFixture, "DB Iterator seek moves to the matching key range", "[db][iterator]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());
    REQUIRE(db->Put("delta", "4").ok());

    auto iterator = db->NewIterator();
    REQUIRE(iterator != nullptr);

    iterator->Seek("bravo");
    REQUIRE(iterator->Valid());
    REQUIRE(iterator->Key() == "delta");

    auto value = iterator->Value();
    REQUIRE(value.has_value());
    REQUIRE(*value == "4");

    iterator->Seek("zeta");
    REQUIRE_FALSE(iterator->Valid());
}

TEST_CASE_METHOD(DBFixture, "DB Iterator filters keys by prefix", "[db][iterator]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("system:1", "ignored").ok());
    REQUIRE(db->Put("user:1", "alice").ok());
    REQUIRE(db->Put("user:2", "bob").ok());
    REQUIRE(db->Put("zone:1", "ignored").ok());

    bitcask::IteratorOptions options;
    options.prefix = "user:";
    auto iterator = db->NewIterator(options);
    REQUIRE(iterator != nullptr);

    const auto expected = std::vector<Entry>{
        {"user:1", "alice"},
        {"user:2", "bob"},
    };
    REQUIRE(CollectEntries(*iterator) == expected);

    iterator->Seek("user:2");
    REQUIRE(iterator->Valid());
    REQUIRE(iterator->Key() == "user:2");

    auto value = iterator->Value();
    REQUIRE(value.has_value());
    REQUIRE(*value == "bob");
}

TEST_CASE_METHOD(DBFixture, "DB ListKeys returns live keys in sorted order", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("gamma", "3").ok());
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());
    REQUIRE(db->Delete("beta").ok());

    const auto expected = std::vector<std::string>{"alpha", "gamma"};
    REQUIRE(db->ListKeys() == expected);
}

TEST_CASE_METHOD(DBFixture, "DB Fold visits key-value pairs in sorted order", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("gamma", "3").ok());
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());

    std::vector<Entry> entries;
    auto status = db->Fold([&entries](std::string_view key, std::string value) {
        entries.emplace_back(std::string(key), std::move(value));
        return true;
    });

    const auto expected = std::vector<Entry>{
        {"alpha", "1"},
        {"beta", "2"},
        {"gamma", "3"},
    };
    REQUIRE(status.ok());
    REQUIRE(entries == expected);
}

TEST_CASE_METHOD(DBFixture, "DB Fold stops when callback returns false", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("gamma", "3").ok());
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());

    std::vector<Entry> entries;
    auto status = db->Fold([&entries](std::string_view key, std::string value) {
        entries.emplace_back(std::string(key), std::move(value));
        return key != "beta";
    });

    const auto expected = std::vector<Entry>{
        {"alpha", "1"},
        {"beta", "2"},
    };
    REQUIRE(status.ok());
    REQUIRE(entries == expected);
}

TEST_CASE_METHOD(DBFixture, "DB Merge keeps latest live records after reopen", "[db][merge]")
{
    auto options = MakeOptions();
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("alpha", std::string(40, 'a')).ok());
    REQUIRE(db->Put("beta", std::string(40, 'b')).ok());
    REQUIRE(db->Put("stale", "old").ok());
    REQUIRE(db->Put("removed", std::string(40, 'r')).ok());
    REQUIRE(db->Put("stale", "new").ok());
    REQUIRE(db->Delete("removed").ok());

    REQUIRE(db->Merge().ok());
    REQUIRE(std::filesystem::exists(kTestDir / "-merge"));

    REQUIRE(db->Put("after_merge", "still here").ok());
    db->Close();

    db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kTestDir / "-merge"));

    auto alpha = db->Get("alpha");
    auto beta = db->Get("beta");
    auto stale = db->Get("stale");
    auto after_merge = db->Get("after_merge");

    REQUIRE(alpha.has_value());
    REQUIRE(beta.has_value());
    REQUIRE(stale.has_value());
    REQUIRE(after_merge.has_value());
    REQUIRE(*alpha == std::string(40, 'a'));
    REQUIRE(*beta == std::string(40, 'b'));
    REQUIRE(*stale == "new");
    REQUIRE(*after_merge == "still here");
    REQUIRE_FALSE(db->Get("removed").has_value());

    const auto expected = std::vector<std::string>{"after_merge", "alpha", "beta", "stale"};
    REQUIRE(db->ListKeys() == expected);
}

TEST_CASE_METHOD(DBFixture, "DB Merge on empty database is a no-op", "[db][merge]")
{
    auto db = bitcask::DB::Open(MakeOptions());
    REQUIRE(db != nullptr);

    REQUIRE(db->Merge().ok());

    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    REQUIRE(db->ListKeys().empty());
}

TEST_CASE_METHOD(DBFixture, "DB Open removes incomplete merge directory", "[db][merge]")
{
    {
        auto db = bitcask::DB::Open(MakeOptions());
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("survivor", "value").ok());
        db->Close();
    }

    std::filesystem::create_directories(kMergeDir);
    WriteDummyFile(kMergeDir / "000000000.data");

    auto db = bitcask::DB::Open(MakeOptions());
    REQUIRE(db != nullptr);
    REQUIRE_FALSE(std::filesystem::exists(kMergeDir));
    RequireValue(*db, "survivor", "value");
}

TEST_CASE_METHOD(DBFixture, "DB Open installs completed merge files", "[db][merge]")
{
    auto options = MakeOptions();
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

TEST_CASE_METHOD(DBFixture, "DB Merge compacts multiple data files", "[db][merge]")
{
    auto options = MakeOptions(128);
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

TEST_CASE_METHOD(DBFixture, "DB Merge preserves committed write batch records", "[db][merge][write_batch]")
{
    auto options = MakeOptions();
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

TEST_CASE_METHOD(DBFixture, "DB Merge can run repeatedly", "[db][merge]")
{
    auto options = MakeOptions();
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
