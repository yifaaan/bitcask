#include "redis_data_struct.h"
#include "redis_data_key.h"

#include <absl/strings/str_cat.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <limits>

#include "../batch.h"

namespace bitcask
{

    namespace
    {
        constexpr int64_t kNanosPerSecond = 1'000'000'000LL;

        using redis_data_key_detail::AppendVarint;

        int64_t CurrentMonotonicTimeNs()
        {
            const auto now_ns = std::chrono::steady_clock::now().time_since_epoch();
            return std::chrono::duration_cast<std::chrono::nanoseconds>(now_ns).count();
        }

        int64_t ExpiryFromTTL(int64_t ttl)
        {
            if (ttl <= 0)
            {
                return 0;
            }

            const auto now = CurrentMonotonicTimeNs();
            const auto max = std::numeric_limits<int64_t>::max();
            if (ttl > (max - now) / kNanosPerSecond)
            {
                return max;
            }
            return now + ttl * kNanosPerSecond;
        }

        bool IsExpired(int64_t expiry)
        {
            return expiry > 0 && CurrentMonotonicTimeNs() >= expiry;
        }

        int64_t NewCollectionVersion()
        {
            static std::atomic<int64_t> counter{ 1 };
            const auto now_ns = std::chrono::system_clock::now().time_since_epoch();
            const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(now_ns).count();
            const auto next = counter.fetch_add(1, std::memory_order_relaxed);
            if (now <= std::numeric_limits<int64_t>::max() - next)
            {
                return now + next;
            }
            return next;
        }

        bool IsCollectionType(RedisDataType type)
        {
            return type == RedisDataType::kHash || type == RedisDataType::kSet;
        }

        struct DecodedValue
        {
            ValueMetadata meta;
            absl::string_view value;
        };

        // Encodes a typed Redis value stored at the user key.
        //
        // String example:
        //   Set("name", "alice", 0)
        //   DB key   = "name"
        //   DB value = [kString][expiry=0]["alice"]
        std::string EncodeValue(RedisDataType type, int64_t expiry, absl::string_view value)
        {
            // Layout: [type(1 byte)][expiry(varint)][value]
            std::string encoded;
            encoded.reserve(1 + 10 + value.size());
            encoded.push_back(static_cast<char>(type));
            AppendVarint(encoded, expiry);
            encoded.append(value.data(), value.size());

            return encoded;
        }

        // Encodes collection metadata stored at the user key.
        //
        // Hash example after HSet("user:1", "name", "alice"):
        //   DB key   = "user:1"
        //   DB value = [kHash][expiry=0][version=123][size=1]
        //
        // Set example after SAdd("tags", "cpp"):
        //   DB key   = "tags"
        //   DB value = [kSet][expiry=0][version=456][size=1]
        //
        // The metadata type is the only type discriminator; collection data
        // keys do not carry a separate prefix.
        std::string EncodeMetadata(RedisDataType type, const ValueMetadata& meta)
        {
            std::string payload;
            payload.reserve(20);
            AppendVarint(payload, meta.version);
            AppendVarint(payload, meta.size);
            return EncodeValue(type, meta.expiry, payload);
        }

        // Decodes metadata from a stored Redis value.
        //
        // String example:
        //   [kString][expiry=0]["alice"]
        //   -> meta{type=kString, expiry=0}, value="alice"
        //
        // Hash metadata example:
        //   [kHash][expiry=0][version=123][size=2]
        //   -> meta{type=kHash, expiry=0, version=123, size=2}
        //
        // Set metadata example:
        //   [kSet][expiry=0][version=456][size=2]
        //   -> meta{type=kSet, expiry=0, version=456, size=2}
        absl::StatusOr<DecodedValue> DecodeValue(absl::string_view encoded)
        {
            if (encoded.size() < 2)
            {
                return absl::DataLossError("Invalid encoded value");
            }

            DecodedValue decoded;
            decoded.meta.type = static_cast<RedisDataType>(encoded[0]);

            const auto remaining = absl::Span<const std::byte>(
                reinterpret_cast<const std::byte*>(encoded.data() + 1),
                encoded.size() - 1);
            const auto [expiry, expiry_len] = Varint(remaining);
            if (expiry_len <= 0)
            {
                return absl::DataLossError("Invalid value expiry");
            }

            decoded.meta.expiry = expiry;
            decoded.value = encoded.substr(1 + static_cast<size_t>(expiry_len));
            if (!IsCollectionType(decoded.meta.type))
            {
                return decoded;
            }

            const auto hash_payload = absl::Span<const std::byte>(
                reinterpret_cast<const std::byte*>(decoded.value.data()),
                decoded.value.size());
            const auto [version, version_len] = Varint(hash_payload);
            if (version_len <= 0 || version <= 0)
            {
                return absl::DataLossError("Invalid collection metadata version");
            }

            const auto [size, size_len] = Varint(hash_payload.subspan(version_len));
            if (size_len <= 0 || size < 0)
            {
                return absl::DataLossError("Invalid collection metadata size");
            }

            decoded.meta.version = version;
            decoded.meta.size = size;
            decoded.value = decoded.value.substr(static_cast<size_t>(version_len + size_len));
            return decoded;
        }
    } // namespace

    // Loads and validates collection metadata for a user key.
    //
    // Hash example:
    //   HGet("user:1", "name")
    //   1. LoadMetadata("user:1", kHash) -> version 123
    //   2. Build ["user:1"][varint(123)]["name"]
    //   3. Read the field value from DB
    //
    // Set example:
    //   SIsMember("tags", "cpp")
    //   1. LoadMetadata("tags", kSet) -> version 456
    //   2. Build ["tags"][varint(456)]["cpp"][varint(3)]
    //   3. Check whether the member key exists in DB
    absl::StatusOr<ValueMetadata> RedisDataStruct::LoadMetadata(
        absl::string_view key,
        RedisDataType expected_type)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        auto encoded = db_->Get(key);
        if (!encoded.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Key not found: ", key));
        }

        auto decoded = DecodeValue(*encoded);
        if (!decoded.ok())
        {
            return decoded.status();
        }
        if (IsExpired(decoded->meta.expiry))
        {
            (void)db_->Delete(key);
            return absl::NotFoundError(absl::StrCat("Key expired: ", key));
        }
        if (decoded->meta.type != expected_type)
        {
            return absl::FailedPreconditionError(absl::StrCat("Wrong type for key: ", key));
        }

        return decoded->meta;
    }

    absl::Status RedisDataStruct::Set(absl::string_view key, absl::string_view value, int64_t ttl)
    {
        const auto expiry = ExpiryFromTTL(ttl);

        return db_->Put(key, EncodeValue(RedisDataType::kString, expiry, value));
    }

    absl::StatusOr<std::string> RedisDataStruct::Get(absl::string_view key)
    {
        auto encoded = db_->Get(key);
        if (!encoded.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Key not found: ", key));
        }

        auto decoded = DecodeValue(*encoded);
        if (!decoded.ok())
        {
            return decoded.status();
        }

        if (IsExpired(decoded->meta.expiry))
        {
            // Best-effort cleanup of expired key
            (void)db_->Delete(key);
            return absl::NotFoundError(absl::StrCat("Key expired: ", key));
        }
        if (decoded->meta.type != RedisDataType::kString)
        {
            return absl::FailedPreconditionError(absl::StrCat("Wrong type for key: ", key));
        }

        return std::string(decoded->value);
    }

    absl::Status RedisDataStruct::Delete(absl::string_view key)
    {
        return db_->Delete(key);
    }

    absl::StatusOr<RedisDataType> RedisDataStruct::Type(absl::string_view key)
    {
        auto encoded = db_->Get(key);
        if (!encoded.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Key not found: ", key));
        }

        auto decoded = DecodeValue(*encoded);
        if (!decoded.ok())
        {
            return decoded.status();
        }

        if (IsExpired(decoded->meta.expiry))
        {
            (void)db_->Delete(key);
            return absl::NotFoundError(absl::StrCat("Key expired: ", key));
        }

        return decoded->meta.type;
    }

    absl::Status RedisDataStruct::HSet(
        absl::string_view key,
        absl::string_view field,
        absl::string_view value,
        int64_t ttl)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        ValueMetadata meta{
            .type = RedisDataType::kHash,
            .expiry = ExpiryFromTTL(ttl),
            .version = NewCollectionVersion(),
            .size = 0,
        };

        auto encoded = db_->Get(key);
        if (encoded.has_value())
        {
            auto decoded = DecodeValue(*encoded);
            if (!decoded.ok())
            {
                return decoded.status();
            }
            if (!IsExpired(decoded->meta.expiry))
            {
                if (decoded->meta.type != RedisDataType::kHash)
                {
                    return absl::FailedPreconditionError(absl::StrCat("Wrong type for key: ", key));
                }

                meta = decoded->meta;
                if (ttl > 0)
                {
                    meta.expiry = ExpiryFromTTL(ttl);
                }
            }
        }

        const auto data_key = HashDataKey{key, meta.version, field}.Encode();
        if (!db_->Get(data_key).has_value())
        {
            ++meta.size;
        }

        WriteBatch batch(db_, {});
        if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kHash, meta)); !status.ok())
        {
            return status;
        }
        if (auto status = batch.Put(data_key, value); !status.ok())
        {
            return status;
        }
        return batch.Commit();
    }

    absl::StatusOr<std::string> RedisDataStruct::HGet(absl::string_view key, absl::string_view field)
    {
        auto meta = LoadMetadata(key, RedisDataType::kHash);
        if (!meta.ok())
        {
            return meta.status();
        }

        auto value = db_->Get(HashDataKey{key, meta->version, field}.Encode());
        if (!value.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Hash field not found: ", key, ".", field));
        }
        return *value;
    }

    absl::Status RedisDataStruct::HDel(absl::string_view key, absl::string_view field)
    {
        auto meta = LoadMetadata(key, RedisDataType::kHash);
        if (!meta.ok())
        {
            if (absl::IsNotFound(meta.status()))
            {
                return absl::OkStatus();
            }
            return meta.status();
        }

        const auto data_key = HashDataKey{key, meta->version, field}.Encode();
        if (!db_->Get(data_key).has_value())
        {
            return absl::OkStatus();
        }

        if (meta->size > 0)
        {
            --meta->size;
        }
        WriteBatch batch(db_, {});
        if (auto status = batch.Delete(data_key); !status.ok())
        {
            return status;
        }

        if (meta->size == 0)
        {
            if (auto status = batch.Delete(key); !status.ok())
            {
                return status;
            }
        }
        else
        {
            if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kHash, *meta)); !status.ok())
            {
                return status;
            }
        }

        return batch.Commit();
    }

    absl::Status RedisDataStruct::SAdd(absl::string_view key, absl::string_view member, int64_t ttl)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        ValueMetadata meta{
            .type = RedisDataType::kSet,
            .expiry = ExpiryFromTTL(ttl),
            .version = NewCollectionVersion(),
            .size = 0,
        };

        auto encoded = db_->Get(key);
        if (encoded.has_value())
        {
            auto decoded = DecodeValue(*encoded);
            if (!decoded.ok())
            {
                return decoded.status();
            }
            if (!IsExpired(decoded->meta.expiry))
            {
                if (decoded->meta.type != RedisDataType::kSet)
                {
                    return absl::FailedPreconditionError(absl::StrCat("Wrong type for key: ", key));
                }

                meta = decoded->meta;
                if (ttl > 0)
                {
                    meta.expiry = ExpiryFromTTL(ttl);
                }
            }
        }

        const auto data_key = SetDataKey{key, meta.version, member}.Encode();
        const auto member_exists = db_->Get(data_key).has_value();
        if (member_exists && ttl <= 0)
        {
            return absl::OkStatus();
        }
        if (!member_exists)
        {
            ++meta.size;
        }

        WriteBatch batch(db_, {});
        if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kSet, meta)); !status.ok())
        {
            return status;
        }
        if (!member_exists)
        {
            if (auto status = batch.Put(data_key, ""); !status.ok())
            {
                return status;
            }
        }
        return batch.Commit();
    }

    absl::StatusOr<bool> RedisDataStruct::SIsMember(absl::string_view key, absl::string_view member)
    {
        auto meta = LoadMetadata(key, RedisDataType::kSet);
        if (!meta.ok())
        {
            if (absl::IsNotFound(meta.status()))
            {
                return false;
            }
            return meta.status();
        }

        return db_->Get(SetDataKey{key, meta->version, member}.Encode()).has_value();
    }

    absl::Status RedisDataStruct::SRem(absl::string_view key, absl::string_view member)
    {
        auto meta = LoadMetadata(key, RedisDataType::kSet);
        if (!meta.ok())
        {
            if (absl::IsNotFound(meta.status()))
            {
                return absl::OkStatus();
            }
            return meta.status();
        }

        const auto data_key = SetDataKey{key, meta->version, member}.Encode();
        if (!db_->Get(data_key).has_value())
        {
            return absl::OkStatus();
        }

        if (meta->size > 0)
        {
            --meta->size;
        }
        WriteBatch batch(db_, {});
        if (auto status = batch.Delete(data_key); !status.ok())
        {
            return status;
        }

        if (meta->size == 0)
        {
            if (auto status = batch.Delete(key); !status.ok())
            {
                return status;
            }
        }
        else
        {
            if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kSet, *meta)); !status.ok())
            {
                return status;
            }
        }

        return batch.Commit();
    }

} // namespace bitcask
