#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "batch.h"
#include "data/log_record.h"
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

    bitcask::Options MakeARTOptions(uint64_t max_data_file_size = 1024 * 1024 * 10)
    {
        auto opts = bitcask::Options{.data_dir = kTestDir, .max_data_file_size = max_data_file_size};
        opts.index_type = bitcask::IndexType::ART;
        return opts;
    }

    uint64_t EncodedRecordSize(std::string_view key, std::string_view value, bitcask::LogRecordType type)
    {
        auto record = bitcask::LogRecord{
            .key = bitcask::LogRecordKeyWithSeq(key, 0),
            .value = std::string(value),
            .type = type,
        };
        return static_cast<uint64_t>(bitcask::EncodeLogRecord(record).second);
    }

} // namespace

TEST_CASE_METHOD(DBFixture, "DB Open creates directory", "[db]")
{
    auto new_dir = kTestDir / "new_db";
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = new_dir});
    REQUIRE(db != nullptr);
    REQUIRE(std::filesystem::exists(new_dir));
}

TEST_CASE_METHOD(DBFixture, "DB Open with invalid auto_merge_reclaim_ratio returns nullptr", "[db]")
{
    auto opts = bitcask::Options{.data_dir = kTestDir};
    opts.auto_merge_reclaim_ratio = -0.1;
    REQUIRE(bitcask::DB::Open(opts) == nullptr);

    opts.auto_merge_reclaim_ratio = 1.1;
    REQUIRE(bitcask::DB::Open(opts) == nullptr);

    opts.auto_merge_reclaim_ratio = 0.0;
    REQUIRE(bitcask::DB::Open(opts) != nullptr);

    opts.auto_merge_reclaim_ratio = 1.0;
    REQUIRE(bitcask::DB::Open(opts) != nullptr);
}

TEST_CASE_METHOD(DBFixture, "DB Open with empty path returns nullptr", "[db]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = ""});
    REQUIRE(db == nullptr);
}

TEST_CASE_METHOD(DBFixture, "DB Open locks data directory", "[db]")
{
    auto options = bitcask::Options{.data_dir = kTestDir};
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    auto second = bitcask::DB::Open(options);
    REQUIRE(second == nullptr);

    db->Close();
    second = bitcask::DB::Open(options);
    REQUIRE(second != nullptr);
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

TEST_CASE_METHOD(DBFixture, "DB bytes_per_sync flushes written bytes", "[db]")
{
    auto options = bitcask::Options{.data_dir = kTestDir};
    options.bytes_per_sync = 1;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());

    const auto data_file = kTestDir / "000000000.data";
    REQUIRE(std::filesystem::exists(data_file));
    REQUIRE(std::filesystem::file_size(data_file) > 0);
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

TEST_CASE_METHOD(DBFixture, "DB Stat reports keys files and reclaimable bytes", "[db][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());
    auto stat = db->Stat();
    REQUIRE(stat.key_num == 1);
    REQUIRE(stat.data_file_num == 1);
    REQUIRE(stat.reclaimable_size == 0);
    REQUIRE(stat.disk_size >= EncodedRecordSize("key1", "value1", bitcask::LogRecordType::kNormal));

    REQUIRE(db->Put("key1", "value2").ok());
    const auto first_record_size = EncodedRecordSize("key1", "value1", bitcask::LogRecordType::kNormal);
    stat = db->Stat();
    REQUIRE(stat.key_num == 1);
    REQUIRE(stat.reclaimable_size == first_record_size);

    REQUIRE(db->Delete("key1").ok());
    const auto second_record_size = EncodedRecordSize("key1", "value2", bitcask::LogRecordType::kNormal);
    const auto delete_record_size = EncodedRecordSize("key1", "", bitcask::LogRecordType::kDeleted);
    stat = db->Stat();
    REQUIRE(stat.key_num == 0);
    REQUIRE(stat.reclaimable_size == first_record_size + second_record_size + delete_record_size);
    REQUIRE(stat.disk_size >= stat.reclaimable_size);
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

TEST_CASE_METHOD(DBFixture, "DB with ART Put and Get", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    auto status = db->Put("key1", "value1");
    REQUIRE(status.ok());

    auto value = db->Get("key1");
    REQUIRE(value.has_value());
    REQUIRE(*value == "value1");
}

TEST_CASE_METHOD(DBFixture, "DB with ART Put overwrites existing key", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());
    REQUIRE(db->Put("key1", "value2").ok());

    auto value = db->Get("key1");
    REQUIRE(value.has_value());
    REQUIRE(*value == "value2");
}

TEST_CASE_METHOD(DBFixture, "DB with ART Get nonexistent key", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    auto value = db->Get("no_such_key");
    REQUIRE_FALSE(value.has_value());
}

TEST_CASE_METHOD(DBFixture, "DB with ART Put with empty key fails", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    auto status = db->Put("", "value");
    REQUIRE_FALSE(status.ok());
}

TEST_CASE_METHOD(DBFixture, "DB with ART Delete", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());
    auto status = db->Delete("key1");
    REQUIRE(status.ok());

    auto value = db->Get("key1");
    REQUIRE_FALSE(value.has_value());
}

TEST_CASE_METHOD(DBFixture, "DB with ART Delete nonexistent key is ok", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    auto status = db->Delete("no_such_key");
    REQUIRE(status.ok());
}

TEST_CASE_METHOD(DBFixture, "DB with ART Close and reopen preserves data", "[db][art]")
{
    {
        auto db = bitcask::DB::Open(MakeARTOptions());
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key1", "value1").ok());
        REQUIRE(db->Put("key2", "value2").ok());
        db->Close();
    }

    {
        auto db = bitcask::DB::Open(MakeARTOptions());
        REQUIRE(db != nullptr);

        auto v1 = db->Get("key1");
        REQUIRE(v1.has_value());
        REQUIRE(*v1 == "value1");

        auto v2 = db->Get("key2");
        REQUIRE(v2.has_value());
        REQUIRE(*v2 == "value2");
    }
}

TEST_CASE_METHOD(DBFixture, "DB with ART Close and reopen with delete preserves correctly", "[db][art]")
{
    {
        auto db = bitcask::DB::Open(MakeARTOptions());
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key1", "value1").ok());
        REQUIRE(db->Put("key2", "value2").ok());
        REQUIRE(db->Delete("key1").ok());
        db->Close();
    }

    {
        auto db = bitcask::DB::Open(MakeARTOptions());
        REQUIRE(db != nullptr);

        auto v1 = db->Get("key1");
        REQUIRE_FALSE(v1.has_value());

        auto v2 = db->Get("key2");
        REQUIRE(v2.has_value());
        REQUIRE(*v2 == "value2");
    }
}

TEST_CASE_METHOD(DBFixture, "DB with ART Iterator scans keys and values in sorted order", "[db][art][iterator]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
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

TEST_CASE_METHOD(DBFixture, "DB with ART Iterator scans keys and values in reverse order", "[db][art][iterator]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
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

TEST_CASE_METHOD(DBFixture, "DB with ART Iterator seek moves to the matching key range", "[db][art][iterator]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
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

TEST_CASE_METHOD(DBFixture, "DB with ART Iterator filters keys by prefix", "[db][art][iterator]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
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

TEST_CASE_METHOD(DBFixture, "DB with ART ListKeys returns live keys in sorted order", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("gamma", "3").ok());
    REQUIRE(db->Put("alpha", "1").ok());
    REQUIRE(db->Put("beta", "2").ok());
    REQUIRE(db->Delete("beta").ok());

    const auto expected = std::vector<std::string>{"alpha", "gamma"};
    REQUIRE(db->ListKeys() == expected);
}

TEST_CASE_METHOD(DBFixture, "DB with ART Fold visits key-value pairs in sorted order", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
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

TEST_CASE_METHOD(DBFixture, "DB with ART Fold stops when callback returns false", "[db][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
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

TEST_CASE("LogRecordPos encode/decode roundtrip", "[log_record]")
{
    // Roundtrip with all fields non-zero
    bitcask::LogRecordPos original{42, 8192, 256};
    auto [encoded, n] = bitcask::EncodeLogRecordPos(original);
    REQUIRE(n > 0);
    auto [decoded, m] = bitcask::DecodeLogRecordPos(absl::MakeConstSpan(encoded));
    REQUIRE(decoded.has_value());
    REQUIRE(m == n);
    REQUIRE(decoded->fid == original.fid);
    REQUIRE(decoded->offset == original.offset);
    REQUIRE(decoded->size == original.size);

    // Decode old format without size field (backward compatibility)
    std::vector<std::byte> old_format(16);
    int index = 0;
    index += bitcask::PutVarint(absl::MakeSpan(old_format).subspan(index), 7);
    index += bitcask::PutVarint(absl::MakeSpan(old_format).subspan(index), 2048);
    old_format.resize(index);
    auto [decoded_old, m2] = bitcask::DecodeLogRecordPos(absl::MakeConstSpan(old_format));
    REQUIRE(decoded_old.has_value());
    REQUIRE(decoded_old->fid == 7);
    REQUIRE(decoded_old->offset == 2048);
    REQUIRE(decoded_old->size == 0);
}

TEST_CASE("Varint handles invalid input", "[log_record]")
{
    // Empty buffer
    std::vector<std::byte> empty;
    auto [v1, n1] = bitcask::Varint(absl::MakeConstSpan(empty));
    REQUIRE(v1 == 0);
    REQUIRE(n1 == 0);

    // Insufficient bytes (varint needs at least 1 byte)
    std::vector<std::byte> invalid = {std::byte{0x80}};
    auto [v2, n2] = bitcask::Varint(absl::MakeConstSpan(invalid));
    REQUIRE(v2 == 0);
    REQUIRE(n2 == 0);
}

TEST_CASE("LogRecordPos decode handles boundaries and edge cases", "[log_record]")
{
    // Only fid byte present, offset missing → invalid
    std::vector<std::byte> fid_only(4);
    bitcask::PutVarint(absl::MakeSpan(fid_only), 1);
    fid_only.resize(1);
    auto [decoded_missing_offset, n_missing] = bitcask::DecodeLogRecordPos(absl::MakeConstSpan(fid_only));
    REQUIRE_FALSE(decoded_missing_offset.has_value());
    REQUIRE(n_missing == 0);

    // fid + offset present, size missing → valid, size defaults to 0
    std::vector<std::byte> buf(16);
    int idx = 0;
    idx += bitcask::PutVarint(absl::MakeSpan(buf).subspan(idx), 1);
    idx += bitcask::PutVarint(absl::MakeSpan(buf).subspan(idx), 100);
    buf.resize(idx);
    auto [decoded1, n1] = bitcask::DecodeLogRecordPos(absl::MakeConstSpan(buf));
    REQUIRE(decoded1.has_value());
    REQUIRE(decoded1->fid == 1);
    REQUIRE(decoded1->offset == 100);
    REQUIRE(decoded1->size == 0);
    REQUIRE(n1 == idx);

    // All zero values
    std::vector<std::byte> all_zero(3, std::byte{0});
    auto [decoded2, _] = bitcask::DecodeLogRecordPos(absl::MakeConstSpan(all_zero));
    REQUIRE(decoded2.has_value());
    REQUIRE(decoded2->fid == 0);
    REQUIRE(decoded2->offset == 0);
    REQUIRE(decoded2->size == 0);

    // Large values that fit in varint
    bitcask::LogRecordPos large{std::numeric_limits<uint32_t>::max(), std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()};
    auto [large_encoded, large_n] = bitcask::EncodeLogRecordPos(large);
    auto [large_decoded, large_m] = bitcask::DecodeLogRecordPos(absl::MakeConstSpan(large_encoded));
    REQUIRE(large_decoded.has_value());
    REQUIRE(large_m == large_n);
    REQUIRE(large_decoded->fid == large.fid);
    REQUIRE(large_decoded->offset == large.offset);
    REQUIRE(large_decoded->size == large.size);
}

TEST_CASE_METHOD(DBFixture, "DB reclaimable size resets after Merge and reopen", "[db][stat][merge]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);

        REQUIRE(db->Put("key", "value1").ok());
        REQUIRE(db->Put("key", "value2").ok());
        auto before = db->Stat();
        REQUIRE(before.reclaimable_size > 0);

        REQUIRE(db->Merge().ok());
        db->Close();
    }

    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);
    auto after = db->Stat();
    REQUIRE(after.reclaimable_size == 0);
    REQUIRE(after.key_num == 1);
}

TEST_CASE_METHOD(DBFixture, "DB reclaimable size tracks batch write operations", "[db][stat][batch]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    bitcask::WriteBatch batch(db.get(), {});
    REQUIRE(batch.Put("key1", "old").ok());
    REQUIRE(batch.Put("key2", "value2").ok());
    REQUIRE(batch.Commit().ok());

    auto after_first = db->Stat();
    REQUIRE(after_first.reclaimable_size == 0);

    bitcask::WriteBatch batch2(db.get(), {});
    REQUIRE(batch2.Put("key1", "new").ok());
    REQUIRE(batch2.Delete("key2").ok());
    REQUIRE(batch2.Commit().ok());

    auto after_second = db->Stat();
    REQUIRE(after_second.reclaimable_size > 0);
}

TEST_CASE_METHOD(DBFixture, "DB reclaimable size is recalculated from data files on reopen", "[db][stat]")
{
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);
        REQUIRE(db->Put("key", "value1").ok());
        REQUIRE(db->Put("key", "value2").ok());
        auto before = db->Stat();
        REQUIRE(before.reclaimable_size > 0);
    }
    {
        auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
        REQUIRE(db != nullptr);
        auto after = db->Stat();
        REQUIRE(after.reclaimable_size > 0);
    }
}

TEST_CASE_METHOD(DBFixture, "DB Stat disk_size reflects actual directory size", "[db][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    auto empty_stat = db->Stat();
    REQUIRE(empty_stat.disk_size == 0);

    REQUIRE(db->Put("key", std::string(512, 'v')).ok());
    auto full_stat = db->Stat();
    REQUIRE(full_stat.disk_size > 0);
    REQUIRE(full_stat.disk_size >= full_stat.reclaimable_size);
}

TEST_CASE_METHOD(DBFixture, "DB batch commit triggers auto merge when ratio reached", "[db][stat][batch][merge]")
{
    auto options = bitcask::Options{.data_dir = kTestDir};
    options.auto_merge_reclaim_ratio = 0.01;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    bitcask::WriteBatch batch(db.get(), {});
    REQUIRE(batch.Put("key", std::string(128, 'a')).ok());
    REQUIRE(batch.Commit().ok());

    REQUIRE_FALSE(std::filesystem::exists(kTestDir / "-merge" / "merge-finished"));

    bitcask::WriteBatch batch2(db.get(), {});
    REQUIRE(batch2.Put("key", std::string(128, 'b')).ok());
    REQUIRE(batch2.Commit().ok());

    REQUIRE(std::filesystem::exists(kTestDir / "-merge" / "merge-finished"));
}

TEST_CASE_METHOD(DBFixture, "DB Stat with ART index tracks keys and reclaimable bytes", "[db][stat][art]")
{
    auto db = bitcask::DB::Open(MakeARTOptions());
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", "value1").ok());
    auto stat = db->Stat();
    REQUIRE(stat.key_num == 1);
    REQUIRE(stat.data_file_num == 1);
    REQUIRE(stat.reclaimable_size == 0);

    REQUIRE(db->Put("key1", "value2").ok());
    stat = db->Stat();
    REQUIRE(stat.key_num == 1);
    REQUIRE(stat.reclaimable_size == EncodedRecordSize("key1", "value1", bitcask::LogRecordType::kNormal));

    REQUIRE(db->Delete("key1").ok());
    stat = db->Stat();
    REQUIRE(stat.key_num == 0);
    REQUIRE(stat.reclaimable_size > 0);
    REQUIRE(stat.disk_size >= stat.reclaimable_size);
}

TEST_CASE_METHOD(DBFixture, "DB Stat data_file_num increases with file rotation", "[db][stat]")
{
    auto options = bitcask::Options{.data_dir = kTestDir, .max_data_file_size = 128};
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key1", std::string(64, 'a')).ok());
    auto stat = db->Stat();
    REQUIRE(stat.data_file_num == 1);

    REQUIRE(db->Put("key2", std::string(64, 'b')).ok());
    stat = db->Stat();
    REQUIRE(stat.data_file_num >= 2);
    REQUIRE(stat.key_num == 2);
}

TEST_CASE_METHOD(DBFixture, "DB reclaimable_size accumulates across multiple overwrites", "[db][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key", "v1").ok());
    REQUIRE(db->Stat().reclaimable_size == 0);

    REQUIRE(db->Put("key", "v2").ok());
    auto after_two = db->Stat();
    const auto first_size = EncodedRecordSize("key", "v1", bitcask::LogRecordType::kNormal);
    REQUIRE(after_two.reclaimable_size == first_size);

    REQUIRE(db->Put("key", "v3").ok());
    auto after_three = db->Stat();
    const auto second_size = EncodedRecordSize("key", "v2", bitcask::LogRecordType::kNormal);
    REQUIRE(after_three.reclaimable_size == first_size + second_size);
}

TEST_CASE_METHOD(DBFixture, "DB Stat key_num decreases after all keys deleted", "[db][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("a", "1").ok());
    REQUIRE(db->Put("b", "2").ok());
    REQUIRE(db->Stat().key_num == 2);

    REQUIRE(db->Delete("a").ok());
    REQUIRE(db->Stat().key_num == 1);

    REQUIRE(db->Delete("b").ok());
    auto stat = db->Stat();
    REQUIRE(stat.key_num == 0);
    REQUIRE(stat.reclaimable_size > 0);
}

TEST_CASE_METHOD(DBFixture, "DB Delete nonexistent key does not change reclaimable size", "[db][stat]")
{
    auto db = bitcask::DB::Open(bitcask::Options{.data_dir = kTestDir});
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key", "value").ok());
    auto before = db->Stat();

    REQUIRE(db->Delete("no_such_key").ok());
    auto after = db->Stat();
    REQUIRE(after.reclaimable_size == before.reclaimable_size);
    REQUIRE(after.key_num == before.key_num);
}

TEST_CASE_METHOD(DBFixture, "DB auto merge does not trigger when ratio is below threshold", "[db][stat][merge]")
{
    auto options = bitcask::Options{.data_dir = kTestDir};
    options.auto_merge_reclaim_ratio = 0.99;
    auto db = bitcask::DB::Open(options);
    REQUIRE(db != nullptr);

    REQUIRE(db->Put("key", std::string(128, 'a')).ok());
    REQUIRE(db->Put("key", std::string(128, 'b')).ok());
    REQUIRE_FALSE(std::filesystem::exists(kTestDir / "-merge" / "merge-finished"));
}
