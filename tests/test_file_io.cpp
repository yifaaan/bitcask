#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>

#include "fio/file_io.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_fio";

    struct FileIOFixture
    {
        FileIOFixture()
        {
            std::filesystem::create_directories(kTestDir);
        }
        ~FileIOFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }
        static auto NewFilePath(const std::string& name) -> std::filesystem::path
        {
            return kTestDir / name;
        }
    };
} // namespace

TEST_CASE_METHOD(FileIOFixture, "FileIO Open creates file", "[fio]")
{
    auto path = NewFilePath("open_test.data");
    auto io = bitcask::FileIO::Open(path);
    REQUIRE(io != nullptr);
    REQUIRE(std::filesystem::exists(path));
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Open nonexistent dir returns nullptr", "[fio]")
{
    auto io = bitcask::FileIO::Open("Z:/nonexistent_dir_12345/test.data");
    REQUIRE(io == nullptr);
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Write and Read", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("rw_test.data"));
    REQUIRE(io != nullptr);

    std::vector<std::byte> data = {
        std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
        std::byte{0x04}, std::byte{0x05},
    };

    auto written = io->Write(data);
    REQUIRE(written == static_cast<int>(data.size()));
    REQUIRE(io->Sync());

    std::vector<std::byte> buf(data.size());
    auto read = io->Read(buf, 0);
    REQUIRE(read == static_cast<int>(data.size()));
    REQUIRE(std::vector<std::byte>(buf.begin(), buf.end()) == data);
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Read at offset", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("offset_test.data"));
    REQUIRE(io != nullptr);

    std::vector<std::byte> data(10, std::byte{0xAA});
    io->Write(data);

    std::vector<std::byte> buf(3);
    auto read = io->Read(buf, 5);
    REQUIRE(read == 3);
    for (auto b : buf)
        REQUIRE(b == std::byte{0xAA});
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Read past end", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("eof_test.data"));
    REQUIRE(io != nullptr);

    std::vector<std::byte> data = {std::byte{0x01}, std::byte{0x02}};
    io->Write(data);

    std::vector<std::byte> buf(100);
    auto read = io->Read(buf, 0);
    REQUIRE(read == 2);
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Append mode", "[fio]")
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

        std::vector<std::byte> buf(2);
        auto read = io->Read(buf, 0);
        REQUIRE(read == 2);
        REQUIRE(buf[0] == std::byte{0x01});
        REQUIRE(buf[1] == std::byte{0x02});
    }
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Size", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("size_test.data"));
    REQUIRE(io != nullptr);
    REQUIRE(io->Size() == 0);

    std::vector<std::byte> data = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
    io->Write(data);
    REQUIRE(io->Size() == 3);
}

TEST_CASE_METHOD(FileIOFixture, "FileIO Close", "[fio]")
{
    auto io = bitcask::FileIO::Open(NewFilePath("close_test.data"));
    REQUIRE(io != nullptr);
    REQUIRE(io->Close());
    REQUIRE(std::filesystem::exists(NewFilePath("close_test.data")));
}
