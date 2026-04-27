#include "redis_data_struct.h"
#include "redis_data_key.h"

#include <absl/strings/str_cat.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "../batch.h"

namespace bitcask
{

    namespace
    {
        constexpr int64_t kNanosPerSecond = 1'000'000'000LL;
        constexpr size_t kRedisFixedIntSize = redis_data_key_detail::kFixedIntSize;
        constexpr uint64_t kInitialListIndex = std::numeric_limits<uint64_t>::max() / 2;

        using redis_data_key_detail::AppendInt64;
        using redis_data_key_detail::AppendScore;
        using redis_data_key_detail::AppendUint64;
        using redis_data_key_detail::ReadInt64;
        using redis_data_key_detail::ReadScore;
        using redis_data_key_detail::ReadUint64;

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
            return type == RedisDataType::kHash || type == RedisDataType::kSet || type == RedisDataType::kList || type == RedisDataType::kZSet;
        }

        struct DecodedValue
        {
            ValueMetadata meta;
            absl::string_view value;
        };

        std::string EncodeValue(RedisDataType type, int64_t expiry, absl::string_view value)
        {
            std::string encoded;
            encoded.reserve(1 + kRedisFixedIntSize + value.size());
            encoded.push_back(static_cast<char>(type));
            AppendInt64(encoded, expiry);
            encoded.append(value.data(), value.size());

            return encoded;
        }

        std::string EncodeMetadata(RedisDataType type, const ValueMetadata& meta)
        {
            std::string payload;
            payload.reserve(type == RedisDataType::kList ? 4 * kRedisFixedIntSize : 2 * kRedisFixedIntSize);
            AppendInt64(payload, meta.version);
            AppendInt64(payload, meta.size);
            if (type == RedisDataType::kList)
            {
                AppendUint64(payload, meta.head);
                AppendUint64(payload, meta.tail);
            }
            return EncodeValue(type, meta.expiry, payload);
        }

        std::string EncodeScoreValue(double score)
        {
            std::string encoded;
            encoded.reserve(kRedisFixedIntSize);
            AppendScore(encoded, score);
            return encoded;
        }

        absl::StatusOr<double> DecodeScoreValue(absl::string_view encoded)
        {
            if (encoded.size() != kRedisFixedIntSize)
            {
                return absl::DataLossError("Invalid sorted set score size");
            }
            return ReadScore(encoded);
        }

        absl::Status DecodeCollectionMetadata(DecodedValue& decoded)
        {
            if (decoded.meta.type == RedisDataType::kHash || decoded.meta.type == RedisDataType::kSet || decoded.meta.type == RedisDataType::kZSet)
            {
                if (decoded.value.size() != 2 * kRedisFixedIntSize)
                {
                    return absl::DataLossError("Invalid collection metadata size");
                }

                decoded.meta.version = ReadInt64(decoded.value.substr(0, kRedisFixedIntSize));
                decoded.meta.size = ReadInt64(decoded.value.substr(kRedisFixedIntSize, kRedisFixedIntSize));
                if (decoded.meta.version <= 0)
                {
                    return absl::DataLossError("Invalid collection metadata version");
                }
                if (decoded.meta.size < 0)
                {
                    return absl::DataLossError("Invalid collection metadata size");
                }
                decoded.value = {};
                return absl::OkStatus();
            }

            if (decoded.meta.type == RedisDataType::kList)
            {
                if (decoded.value.size() != 4 * kRedisFixedIntSize)
                {
                    return absl::DataLossError("Invalid list metadata size");
                }

                decoded.meta.version = ReadInt64(decoded.value.substr(0, kRedisFixedIntSize));
                decoded.meta.size = ReadInt64(decoded.value.substr(kRedisFixedIntSize, kRedisFixedIntSize));
                decoded.meta.head = ReadUint64(decoded.value.substr(2 * kRedisFixedIntSize, kRedisFixedIntSize));
                decoded.meta.tail = ReadUint64(decoded.value.substr(3 * kRedisFixedIntSize, kRedisFixedIntSize));
                if (decoded.meta.version <= 0)
                {
                    return absl::DataLossError("Invalid list metadata version");
                }
                if (decoded.meta.size < 0)
                {
                    return absl::DataLossError("Invalid list metadata size");
                }
                if (decoded.meta.tail < decoded.meta.head || decoded.meta.tail - decoded.meta.head != static_cast<uint64_t>(decoded.meta.size))
                {
                    return absl::DataLossError("Invalid list metadata bounds");
                }
                decoded.value = {};
                return absl::OkStatus();
            }

            return absl::OkStatus();
        }

        absl::StatusOr<DecodedValue> DecodeValue(absl::string_view encoded)
        {
            if (encoded.size() < 1 + kRedisFixedIntSize)
            {
                return absl::DataLossError("Invalid encoded value");
            }

            DecodedValue decoded;
            decoded.meta.type = static_cast<RedisDataType>(encoded[0]);
            decoded.meta.expiry = ReadInt64(encoded.substr(1, kRedisFixedIntSize));
            decoded.value = encoded.substr(1 + kRedisFixedIntSize);
            if (!IsCollectionType(decoded.meta.type))
            {
                return decoded;
            }

            if (auto status = DecodeCollectionMetadata(decoded); !status.ok())
            {
                return status;
            }
            return decoded;
        }
    } // namespace

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

    absl::Status RedisDataStruct::LPush(absl::string_view key, absl::string_view value, int64_t ttl)
    {
        return ListPush(key, value, ttl, true);
    }

    absl::Status RedisDataStruct::RPush(absl::string_view key, absl::string_view value, int64_t ttl)
    {
        return ListPush(key, value, ttl, false);
    }

    absl::StatusOr<std::string> RedisDataStruct::LPop(absl::string_view key)
    {
        return ListPop(key, true);
    }

    absl::StatusOr<std::string> RedisDataStruct::RPop(absl::string_view key)
    {
        return ListPop(key, false);
    }

    absl::StatusOr<int64_t> RedisDataStruct::LLen(absl::string_view key)
    {
        auto meta = LoadMetadata(key, RedisDataType::kList);
        if (!meta.ok())
        {
            if (absl::IsNotFound(meta.status()))
            {
                return 0;
            }
            return meta.status();
        }
        return meta->size;
    }

    absl::Status RedisDataStruct::ListPush(absl::string_view key, absl::string_view value, int64_t ttl, bool push_left)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        ValueMetadata meta{
            .type = RedisDataType::kList,
            .expiry = ExpiryFromTTL(ttl),
            .version = NewCollectionVersion(),
            .size = 0,
            .head = kInitialListIndex,
            .tail = kInitialListIndex,
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
                if (decoded->meta.type != RedisDataType::kList)
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

        if (meta.size == std::numeric_limits<int64_t>::max())
        {
            return absl::ResourceExhaustedError("List size limit reached");
        }

        uint64_t index = meta.tail;
        if (push_left)
        {
            if (meta.head == 0)
            {
                return absl::ResourceExhaustedError("List left index limit reached");
            }
            index = --meta.head;
        }
        else
        {
            if (meta.tail == std::numeric_limits<uint64_t>::max())
            {
                return absl::ResourceExhaustedError("List right index limit reached");
            }
            ++meta.tail;
        }
        ++meta.size;

        WriteBatch batch(db_, {});
        if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kList, meta)); !status.ok())
        {
            return status;
        }
        if (auto status = batch.Put(ListDataKey{key, meta.version, index}.Encode(), value); !status.ok())
        {
            return status;
        }
        return batch.Commit();
    }

    absl::StatusOr<std::string> RedisDataStruct::ListPop(absl::string_view key, bool pop_left)
    {
        auto meta = LoadMetadata(key, RedisDataType::kList);
        if (!meta.ok())
        {
            return meta.status();
        }
        if (meta->size <= 0)
        {
            return absl::NotFoundError(absl::StrCat("List is empty: ", key));
        }

        uint64_t index = meta->head;
        if (pop_left)
        {
            ++meta->head;
        }
        else
        {
            if (meta->tail == 0)
            {
                return absl::DataLossError("Invalid list tail");
            }
            index = --meta->tail;
        }

        const auto data_key = ListDataKey{key, meta->version, index}.Encode();
        auto value = db_->Get(data_key);
        if (!value.has_value())
        {
            return absl::DataLossError(absl::StrCat("List item not found: ", key));
        }

        --meta->size;
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
            if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kList, *meta)); !status.ok())
            {
                return status;
            }
        }

        if (auto status = batch.Commit(); !status.ok())
        {
            return status;
        }
        return *value;
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

    absl::Status RedisDataStruct::ZAdd(absl::string_view key, absl::string_view member, double score, int64_t ttl)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }
        if (std::isnan(score))
        {
            return absl::InvalidArgumentError("Sorted set score cannot be NaN");
        }

        ValueMetadata meta{
            .type = RedisDataType::kZSet,
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
                if (decoded->meta.type != RedisDataType::kZSet)
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

        const auto member_key = ZSetMemberDataKey{key, meta.version, member}.Encode();
        const auto old_score_value = db_->Get(member_key);
        std::string old_score_key;
        bool score_changed = false;
        if (old_score_value.has_value())
        {
            auto old_score = DecodeScoreValue(*old_score_value);
            if (!old_score.ok())
            {
                return old_score.status();
            }
            score_changed = *old_score != score;
            if (!score_changed && ttl <= 0)
            {
                return absl::OkStatus();
            }
            if (score_changed)
            {
                old_score_key = ZSetScoreDataKey{key, meta.version, *old_score, member}.Encode();
            }
        }
        else
        {
            ++meta.size;
        }

        const auto score_key = ZSetScoreDataKey{key, meta.version, score, member}.Encode();
        WriteBatch batch(db_, {});
        if (score_changed)
        {
            if (auto status = batch.Delete(old_score_key); !status.ok())
            {
                return status;
            }
        }
        if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kZSet, meta)); !status.ok())
        {
            return status;
        }
        if (auto status = batch.Put(member_key, EncodeScoreValue(score)); !status.ok())
        {
            return status;
        }
        if (auto status = batch.Put(score_key, ""); !status.ok())
        {
            return status;
        }
        return batch.Commit();
    }

    absl::StatusOr<double> RedisDataStruct::ZScore(absl::string_view key, absl::string_view member)
    {
        auto meta = LoadMetadata(key, RedisDataType::kZSet);
        if (!meta.ok())
        {
            return meta.status();
        }

        auto score = db_->Get(ZSetMemberDataKey{key, meta->version, member}.Encode());
        if (!score.has_value())
        {
            return absl::NotFoundError(absl::StrCat("Sorted set member not found: ", key, ".", member));
        }
        return DecodeScoreValue(*score);
    }

    absl::Status RedisDataStruct::ZRem(absl::string_view key, absl::string_view member)
    {
        auto meta = LoadMetadata(key, RedisDataType::kZSet);
        if (!meta.ok())
        {
            if (absl::IsNotFound(meta.status()))
            {
                return absl::OkStatus();
            }
            return meta.status();
        }

        const auto member_key = ZSetMemberDataKey{key, meta->version, member}.Encode();
        auto score = db_->Get(member_key);
        if (!score.has_value())
        {
            return absl::OkStatus();
        }
        auto decoded_score = DecodeScoreValue(*score);
        if (!decoded_score.ok())
        {
            return decoded_score.status();
        }

        if (meta->size > 0)
        {
            --meta->size;
        }
        const auto score_key = ZSetScoreDataKey{key, meta->version, *decoded_score, member}.Encode();
        WriteBatch batch(db_, {});
        if (auto status = batch.Delete(member_key); !status.ok())
        {
            return status;
        }
        if (auto status = batch.Delete(score_key); !status.ok())
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
            if (auto status = batch.Put(key, EncodeMetadata(RedisDataType::kZSet, *meta)); !status.ok())
            {
                return status;
            }
        }

        return batch.Commit();
    }

} // namespace bitcask
