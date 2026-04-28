#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

#include "resp/resp.h"

TEST_CASE("RESP serializes scalar values", "[resp]")
{
    using namespace bitcask::resp;

    REQUIRE(Serialize(Simple("OK")) == "+OK\r\n");
    REQUIRE(Serialize(Err("ERR unknown command")) == "-ERR unknown command\r\n");
    REQUIRE(Serialize(Int(-42)) == ":-42\r\n");
    REQUIRE(Serialize(Bulk("hello")) == "$5\r\nhello\r\n");
    REQUIRE(Serialize(Bulk("")) == "$0\r\n\r\n");
    REQUIRE(Serialize(NullBulk()) == "$-1\r\n");
}

TEST_CASE("RESP serializes arrays", "[resp]")
{
    using namespace bitcask::resp;

    auto command = ArrayOf({
        Bulk("SET"),
        Bulk("key"),
        Bulk("value"),
    });
    REQUIRE(Serialize(command) == "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");

    auto nested = ArrayOf({
        Int(1),
        ArrayOf({Simple("two"), NullBulk()}),
    });
    REQUIRE(Serialize(nested) == "*2\r\n:1\r\n*2\r\n+two\r\n$-1\r\n");
}

TEST_CASE("RESP StreamParser waits for full frames and handles pipelines", "[resp]")
{
    using namespace bitcask::resp;

    StreamParser parser;
    parser.Append("*2\r\n$3\r\nGE");

    auto pending = parser.Next();
    REQUIRE(pending.ok());
    REQUIRE_FALSE(pending->has_value());
    REQUIRE(parser.BufferedBytes() > 0);

    parser.Append("T\r\n$3\r\nkey\r\n*1\r\n$4\r\nPING\r\n");

    auto command = parser.Next();
    REQUIRE(command.ok());
    REQUIRE(command->has_value());
    const std::vector<std::string> expected_args{"GET", "key"};
    REQUIRE(**command == expected_args);

    auto ping = parser.Next();
    REQUIRE(ping.ok());
    REQUIRE(ping->has_value());
    const std::vector<std::string> expected_ping{"PING"};
    REQUIRE(**ping == expected_ping);

    auto empty = parser.Next();
    REQUIRE(empty.ok());
    REQUIRE_FALSE(empty->has_value());
    REQUIRE(parser.BufferedBytes() == 0);
}

TEST_CASE("RESP StreamParser rejects malformed commands", "[resp]")
{
    using namespace bitcask::resp;

    StreamParser parser;
    auto empty = parser.Next();
    REQUIRE(empty.ok());
    REQUIRE_FALSE(empty->has_value());

    parser.Append("+OK\r\n");
    REQUIRE_FALSE(parser.Next().ok());

    parser.Clear();
    parser.Append("*2\r\n$3\r\nGET\r\n$-1\r\n");
    REQUIRE_FALSE(parser.Next().ok());

    parser.Clear();
    parser.Append("*2\r\n$6\r\nINCRBY\r\n:2\r\n");
    REQUIRE_FALSE(parser.Next().ok());
}
