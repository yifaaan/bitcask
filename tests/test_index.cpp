#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "index/index.h"

namespace
{

    void RequirePos(const bitcask::LogRecordPos& pos, std::uint32_t fid, std::int64_t offset)
    {
        REQUIRE(pos.fid == fid);
        REQUIRE(pos.offset == offset);
    }

    std::vector<std::string> CollectKeys(bitcask::IndexIterator& iterator)
    {
        std::vector<std::string> keys;
        for (iterator.Rewind(); iterator.Valid(); iterator.Next())
        {
            keys.emplace_back(iterator.Key());
        }
        return keys;
    }

} // namespace

TEST_CASE("BTree Index")
{
    auto index = bitcask::CreateIndexer(bitcask::IndexType::BTree);
    REQUIRE(index != nullptr);

    SECTION("Put and Get")
    {
        REQUIRE(index->Put("key1", {1, 100}));
        auto pos = index->Get("key1");
        REQUIRE(pos.has_value());
        REQUIRE(pos->fid == 1);
        REQUIRE(pos->offset == 100);
    }

    SECTION("Put overwrites existing")
    {
        index->Put("key1", {1, 100});
        index->Put("key1", {2, 200});
        auto pos = index->Get("key1");
        REQUIRE(pos.has_value());
        REQUIRE(pos->fid == 2);
        REQUIRE(pos->offset == 200);
    }

    SECTION("Get nonexistent key")
    {
        auto pos = index->Get("no_such_key");
        REQUIRE_FALSE(pos.has_value());
    }

    SECTION("Delete existing key")
    {
        index->Put("key1", {1, 100});
        REQUIRE(index->Delete("key1"));
        auto pos = index->Get("key1");
        REQUIRE_FALSE(pos.has_value());
    }

    SECTION("Delete nonexistent key")
    {
        REQUIRE_FALSE(index->Delete("no_such_key"));
    }

    SECTION("Put with empty key")
    {
        REQUIRE(index->Put("", {0, 0}));
        auto pos = index->Get("");
        REQUIRE(pos.has_value());
        REQUIRE(pos->fid == 0);
        REQUIRE(pos->offset == 0);
    }
}

TEST_CASE("BTree Iterator")
{
    auto index = bitcask::CreateIndexer(bitcask::IndexType::BTree);
    REQUIRE(index != nullptr);

    SECTION("Empty iterator is invalid")
    {
        auto iterator = index->Iterator();
        REQUIRE(iterator != nullptr);
        REQUIRE_FALSE(iterator->Valid());

        iterator->Rewind();
        REQUIRE_FALSE(iterator->Valid());

        iterator->Seek("key");
        REQUIRE_FALSE(iterator->Valid());
    }

    SECTION("Forward iterator scans keys in sorted order")
    {
        REQUIRE(index->Put("gamma", {3, 30}));
        REQUIRE(index->Put("alpha", {1, 10}));
        REQUIRE(index->Put("beta", {2, 20}));

        auto iterator = index->Iterator();
        REQUIRE(iterator != nullptr);

        const auto expected = std::vector<std::string>{"alpha", "beta", "gamma"};
        REQUIRE(CollectKeys(*iterator) == expected);
        REQUIRE_FALSE(iterator->Valid());
    }

    SECTION("Forward iterator seek moves to first key greater than or equal to target")
    {
        REQUIRE(index->Put("alpha", {1, 10}));
        REQUIRE(index->Put("beta", {2, 20}));
        REQUIRE(index->Put("delta", {4, 40}));

        auto iterator = index->Iterator();
        REQUIRE(iterator != nullptr);

        iterator->Seek("beta");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->Key() == "beta");
        RequirePos(iterator->Value(), 2, 20);

        iterator->Seek("bravo");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->Key() == "delta");
        RequirePos(iterator->Value(), 4, 40);

        iterator->Seek("zeta");
        REQUIRE_FALSE(iterator->Valid());
    }

    SECTION("Reverse iterator scans keys in descending order")
    {
        REQUIRE(index->Put("gamma", {3, 30}));
        REQUIRE(index->Put("alpha", {1, 10}));
        REQUIRE(index->Put("beta", {2, 20}));

        auto iterator = index->Iterator(true);
        REQUIRE(iterator != nullptr);

        const auto expected = std::vector<std::string>{"gamma", "beta", "alpha"};
        REQUIRE(CollectKeys(*iterator) == expected);
        REQUIRE_FALSE(iterator->Valid());
    }

    SECTION("Reverse iterator seek moves to last key less than or equal to target")
    {
        REQUIRE(index->Put("alpha", {1, 10}));
        REQUIRE(index->Put("beta", {2, 20}));
        REQUIRE(index->Put("delta", {4, 40}));

        auto iterator = index->Iterator(true);
        REQUIRE(iterator != nullptr);

        iterator->Seek("beta");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->Key() == "beta");
        RequirePos(iterator->Value(), 2, 20);

        iterator->Seek("bravo");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->Key() == "beta");
        RequirePos(iterator->Value(), 2, 20);

        iterator->Seek("aardvark");
        REQUIRE_FALSE(iterator->Valid());

        iterator->Seek("zeta");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->Key() == "delta");
        RequirePos(iterator->Value(), 4, 40);
    }

    SECTION("Iterator uses a snapshot of keys at creation time")
    {
        REQUIRE(index->Put("alpha", {1, 10}));
        REQUIRE(index->Put("beta", {2, 20}));

        auto iterator = index->Iterator();
        REQUIRE(iterator != nullptr);

        REQUIRE(index->Put("gamma", {3, 30}));
        REQUIRE(index->Delete("alpha"));

        const auto expected = std::vector<std::string>{"alpha", "beta"};
        REQUIRE(CollectKeys(*iterator) == expected);
    }
}
