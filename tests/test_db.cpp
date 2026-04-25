#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "db.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_db";
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

    db->Put("key1", "value1");
    db->Put("key1", "value2");

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

    db->Put("key1", "value1");
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
        db->Put("key1", "value1");
        db->Put("key2", "value2");
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
        db->Put("key1", "value1");
        db->Put("key2", "value2");
        db->Delete("key1");
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
