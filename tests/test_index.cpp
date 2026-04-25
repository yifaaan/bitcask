#include <catch2/catch_test_macros.hpp>
#include <memory>

#include "index/index.h"

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
