#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <vector>

#include "data/data_file.h"
#include "data/log_record.h"

namespace
{
    const auto kTestDir = std::filesystem::temp_directory_path() / "bitcask_test_data";

    struct DataFileFixture
    {
        DataFileFixture()
        {
            std::filesystem::create_directories(kTestDir);
        }
        ~DataFileFixture()
        {
            std::filesystem::remove_all(kTestDir);
        }
    };
} // namespace

TEST_CASE_METHOD(DataFileFixture, "OpenDataFile creates file", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);
    REQUIRE(df->fid == 0);
    REQUIRE(df->write_offset == 0);

    auto path = kTestDir / "000000000.data";
    REQUIRE(std::filesystem::exists(path));
}

TEST_CASE_METHOD(DataFileFixture, "OpenDataFile different file IDs", "[data_file]")
{
    auto df0 = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df0 != nullptr);
    REQUIRE(df0->fid == 0);

    auto df1 = bitcask::DataFile::Open(kTestDir, 1);
    REQUIRE(df1 != nullptr);
    REQUIRE(df1->fid == 1);

    REQUIRE(std::filesystem::exists(kTestDir / "000000000.data"));
    REQUIRE(std::filesystem::exists(kTestDir / "000000001.data"));
}

TEST_CASE_METHOD(DataFileFixture, "DataFile Write updates offset", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);

    std::vector<std::byte> data = {std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    REQUIRE(df->Write(data));
    REQUIRE(df->write_offset == 5);

    std::vector<std::byte> data2 = {std::byte{' '}, std::byte{'w'}, std::byte{'o'}, std::byte{'r'}, std::byte{'l'}, std::byte{'d'}};
    REQUIRE(df->Write(data2));
    REQUIRE(df->write_offset == 11);
}

TEST_CASE_METHOD(DataFileFixture, "DataFile Sync", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);
    std::vector<std::byte> data = {std::byte{'d'}, std::byte{'a'}, std::byte{'t'}, std::byte{'a'}};
    REQUIRE(df->Write(data));
    REQUIRE(df->Sync());
}

TEST_CASE_METHOD(DataFileFixture, "Encode and ReadLogRecord roundtrip", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);

    bitcask::LogRecord record;
    record.key = "test_key";
    record.value = "test_value";
    record.type = bitcask::LogRecordType::kNormal;

    auto [encoded, size] = bitcask::EncodeLogRecord(record);
    REQUIRE(size > 0);
    REQUIRE(df->Write(encoded));

    auto [read_record, read_size, is_eof] = df->ReadLogRecord(0);
    REQUIRE(read_record.has_value());
    REQUIRE(read_record->key == "test_key");
    REQUIRE(read_record->value == "test_value");
    REQUIRE(read_record->type == bitcask::LogRecordType::kNormal);
    REQUIRE(read_size == size);
}

TEST_CASE_METHOD(DataFileFixture, "ReadLogRecord EOF on empty file", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);

    auto [record, size, is_eof] = df->ReadLogRecord(0);
    REQUIRE_FALSE(record.has_value());
}

TEST_CASE_METHOD(DataFileFixture, "ReadLogRecord multiple records", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);

    bitcask::LogRecord rec1;
    rec1.key = "key1";
    rec1.value = "value1";
    rec1.type = bitcask::LogRecordType::kNormal;

    bitcask::LogRecord rec2;
    rec2.key = "key2";
    rec2.value = "value2";
    rec2.type = bitcask::LogRecordType::kNormal;

    auto [enc1, size1] = bitcask::EncodeLogRecord(rec1);
    auto [enc2, size2] = bitcask::EncodeLogRecord(rec2);

    REQUIRE(df->Write(enc1));
    REQUIRE(df->Write(enc2));

    auto [r1, s1, eof1] = df->ReadLogRecord(0);
    REQUIRE(r1.has_value());
    REQUIRE(r1->key == "key1");
    REQUIRE(r1->value == "value1");

    auto [r2, s2, eof2] = df->ReadLogRecord(size1);
    REQUIRE(r2.has_value());
    REQUIRE(r2->key == "key2");
    REQUIRE(r2->value == "value2");
}

TEST_CASE_METHOD(DataFileFixture, "ReadLogRecord deleted record", "[data_file]")
{
    auto df = bitcask::DataFile::Open(kTestDir, 0);
    REQUIRE(df != nullptr);

    bitcask::LogRecord record;
    record.key = "deleted_key";
    record.value = "";
    record.type = bitcask::LogRecordType::kDeleted;

    auto [encoded, size] = bitcask::EncodeLogRecord(record);
    REQUIRE(df->Write(encoded));

    auto [read_record, read_size, is_eof] = df->ReadLogRecord(0);
    REQUIRE(read_record.has_value());
    REQUIRE(read_record->type == bitcask::LogRecordType::kDeleted);
    REQUIRE(read_record->key == "deleted_key");
}
