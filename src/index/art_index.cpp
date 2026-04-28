#include "art_index.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <mutex>
#include <string>
#include <vector>

#include "data/log_record.h"

namespace bitcask
{

    namespace
    {
        struct IterationContext
        {
            bool stop = false;
            std::vector<std::pair<std::string, LogRecordPos>> items;
        };

        int CollectCallback(void* data, const unsigned char* key, uint32_t key_len, void* value)
        {
            auto* ctx = static_cast<IterationContext*>(data);
            ctx->items.emplace_back(
                std::string(reinterpret_cast<const char*>(key), key_len),
                *static_cast<LogRecordPos*>(value));
            return 0;
        }

        class ARTIndexIterator final : public IndexIterator
        {
        public:
            using Item = std::pair<std::string, LogRecordPos>;

            ARTIndexIterator(std::vector<Item> items, bool reverse)
                : items_(std::move(items)), reverse_(reverse)
            {
                Rewind();
            }

            void Rewind() override
            {
                if (items_.empty())
                {
                    current_ = kInvalidIndex;
                    return;
                }
                current_ = reverse_ ? items_.size() - 1 : 0;
            }

            void Seek(absl::string_view key) override
            {
                auto target = std::string(key);
                if (items_.empty())
                {
                    current_ = kInvalidIndex;
                    return;
                }

                if (reverse_)
                {
                    auto it = std::upper_bound(items_.begin(), items_.end(), target,
                        [](const std::string& value, const Item& item) {
                            return value < item.first;
                        });
                    if (it == items_.begin())
                    {
                        current_ = kInvalidIndex;
                        return;
                    }
                    current_ = static_cast<size_t>(std::distance(items_.begin(), std::prev(it)));
                    return;
                }

                auto it = std::lower_bound(items_.begin(), items_.end(), target,
                    [](const Item& item, const std::string& value) {
                        return item.first < value;
                    });
                if (it == items_.end())
                {
                    current_ = kInvalidIndex;
                    return;
                }
                current_ = static_cast<size_t>(std::distance(items_.begin(), it));
            }

            void Next() override
            {
                if (!Valid())
                {
                    return;
                }

                if (reverse_)
                {
                    current_ = current_ == 0 ? kInvalidIndex : current_ - 1;
                    return;
                }

                ++current_;
                if (current_ >= items_.size())
                {
                    current_ = kInvalidIndex;
                }
            }

            bool Valid() const override
            {
                return current_ != kInvalidIndex && current_ < items_.size();
            }

            absl::string_view Key() const override
            {
                return items_[current_].first;
            }

            LogRecordPos Value() const override
            {
                return items_[current_].second;
            }

        private:
            static constexpr size_t kInvalidIndex = std::numeric_limits<size_t>::max();

            std::vector<Item> items_;
            bool reverse_ = false;
            size_t current_ = kInvalidIndex;
        };

        LogRecordPos* ClonePos(const LogRecordPos& pos)
        {
            auto* p = new LogRecordPos(pos);
            return p;
        }
    }

    AdaptiveRadixTreeIndex::AdaptiveRadixTreeIndex()
    {
        art_tree_init(&tree_);
    }

    AdaptiveRadixTreeIndex::~AdaptiveRadixTreeIndex()
    {
        IterationContext ctx;
        art_iter(&tree_, CollectCallback, &ctx);
        art_tree_destroy(&tree_);
    }

    bool AdaptiveRadixTreeIndex::Put(absl::string_view key, LogRecordPos pos)
    {
        auto* newPos = new LogRecordPos(pos);
        std::unique_lock lock(mutex_);
        void* old = art_insert(&tree_,
            reinterpret_cast<const unsigned char*>(key.data()),
            static_cast<int>(key.size()),
            newPos);
        if (old)
        {
            delete static_cast<LogRecordPos*>(old);
        }
        return true;
    }

    std::optional<LogRecordPos> AdaptiveRadixTreeIndex::Get(absl::string_view key) const
    {
        std::shared_lock lock(mutex_);
        void* found = art_search(&tree_,
            reinterpret_cast<const unsigned char*>(key.data()),
            static_cast<int>(key.size()));
        if (found)
        {
            return *static_cast<LogRecordPos*>(found);
        }
        return std::nullopt;
    }

    bool AdaptiveRadixTreeIndex::Delete(absl::string_view key)
    {
        std::unique_lock lock(mutex_);
        void* removed = art_delete(&tree_,
            reinterpret_cast<const unsigned char*>(key.data()),
            static_cast<int>(key.size()));
        if (removed)
        {
            delete static_cast<LogRecordPos*>(removed);
            return true;
        }
        return false;
    }

    std::unique_ptr<IndexIterator> AdaptiveRadixTreeIndex::Iterator(bool reverse) const
    {
        std::shared_lock lock(mutex_);
        IterationContext ctx;
        art_iter(const_cast<art_tree*>(&tree_), CollectCallback, &ctx);

        std::sort(ctx.items.begin(), ctx.items.end(),
            [](const ARTIndexIterator::Item& a, const ARTIndexIterator::Item& b) {
                return a.first < b.first;
            });

        return std::make_unique<ARTIndexIterator>(std::move(ctx.items), reverse);
    }

    size_t AdaptiveRadixTreeIndex::size() const
    {
        std::shared_lock lock(mutex_);
        return static_cast<size_t>(art_size(const_cast<art_tree*>(&tree_)));
    }

    std::unique_ptr<AdaptiveRadixTreeIndex> CreateAdaptiveRadixTreeIndex()
    {
        return std::make_unique<AdaptiveRadixTreeIndex>();
    }

} // namespace bitcask
