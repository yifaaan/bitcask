#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>

#include "fio/file_io.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test";

    struct TestFixture
    {
        TestFixture()
        {
            std::filesystem::create_directories(kTestDir);
        }

        ~TestFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }

        static auto NewFilePath(const std::string& name) -> std::filesystem::path
        {
            return kTestDir / name;
        }
    };
} // namespace

TEST_CASE_METHOD(TestFixture, "FileIO Open", "[fio]")
{
    auto path = NewFilePath("open_test.data");
    auto io = bitcask::FileIO::Open(path);
    REQUIRE(io != nullptr);
    REQUIRE(std::filesystem::exists(path));
}

TEST_CASE_METHOD(TestFixture, "FileIO Open nonexistent dir returns nullptr", "[fio]")
{
    auto io = bitcask::FileIO::Open("Z:/nonexistent_dir_12345/test.data");
    REQUIRE(io == nullptr);
}

TEST_CASE_METHOD(TestFixture, "FileIO Write and Read", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("rw_test.data"));
    REQUIRE(io != nullptr);

    std::vector<std::byte> data = {
        std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04},
        std::byte{0x05},
    };

    auto written = io->Write(data);
    REQUIRE(written == data.size());
    REQUIRE(io->Sync());

    auto read = io->Read(0, data.size());
    REQUIRE(read.size() == data.size());
    REQUIRE(read == data);
}

TEST_CASE_METHOD(TestFixture, "FileIO Read at offset", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("offset_test.data"));
    REQUIRE(io != nullptr);

    std::vector<std::byte> data(10, std::byte{0xAA});
    io->Write(data);

    auto read = io->Read(5, 3);
    REQUIRE(read.size() == 3);
    for (auto b : read)
        REQUIRE(b == std::byte{0xAA});
}

TEST_CASE_METHOD(TestFixture, "FileIO Read past end", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("eof_test.data"));
    REQUIRE(io != nullptr);

    std::vector<std::byte> data = {std::byte{0x01}, std::byte{0x02}};
    io->Write(data);

    auto read = io->Read(0, 100);
    REQUIRE(read.size() == 2);
}

TEST_CASE_METHOD(TestFixture, "FileIO Append mode", "[fio]")
{
    auto path = NewFilePath("append_test.data");

    {
        auto io = bitcask::FileIO::Open(path);
        std::vector<std::byte> d1 = {std::byte{0x01}};
        io->Write(d1);
    }

    {
        auto io = bitcask::FileIO::Open(path);
        std::vector<std::byte> d2 = {std::byte{0x02}};
        io->Write(d2);

        auto read = io->Read(0, 2);
        REQUIRE(read.size() == 2);
        REQUIRE(read[0] == std::byte{0x01});
        REQUIRE(read[1] == std::byte{0x02});
    }
}

TEST_CASE_METHOD(TestFixture, "FileIO Close", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("close_test.data"));
    REQUIRE(io != nullptr);
    io->Close();
    REQUIRE(std::filesystem::exists(NewFilePath("close_test.data")));
}
