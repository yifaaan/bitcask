#include <catch2/catch_test_macros.hpp>
#include "index.h"
#include <memory>

TEST_CASE("Index")
{
    auto index = bitcask::CreateBTreeIndex();

    SECTION("put and get")
    {
        index->Put("key1", { 1, 100 });
        auto pos = index->Get("key1");
        REQUIRE(pos.has_value());
        REQUIRE(pos->fid == 1);
        REQUIRE(pos->offset == 100);
    }

    SECTION("put overwrites existing")
    {
        index->Put("key1", { 1, 100 });
        index->Put("key1", { 2, 200 });
        auto pos = index->Get("key1");
        REQUIRE(pos.has_value());
        REQUIRE(pos->fid == 2);
        REQUIRE(pos->offset == 200);
    }

    SECTION("get nonexistent key")
    {
        auto pos = index->Get("no_such_key");
        REQUIRE_FALSE(pos.has_value());
    }
}
