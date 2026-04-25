#include "btree_index.h"
#include "data/log_record.h"
#include <algorithm>
#include <iterator>
#include <limits>
#include <vector>

namespace bitcask
{

    namespace
    {
        class BTreeIndexIterator final : public IndexIterator
        {
        public:
            using Item = std::pair<std::string, LogRecordPos>;

            BTreeIndexIterator(std::vector<Item> items, bool reverse) : items_(std::move(items)), reverse_(reverse) {}

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

                // Find first item > target, then step back to <= target.
                if (reverse_)
                {
                    auto it = std::upper_bound(items_.begin(), items_.end(), target, [](const std::string& value, const Item& item) {
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

                auto it = std::lower_bound(items_.begin(), items_.end(), target, [](const Item& item, const std::string& value) {
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
    }

    bool BTreeIndex::Put(absl::string_view key, LogRecordPos pos)
    {
        std::unique_lock lock(mutex_);
        index_[std::string(key)] = pos;
        return true;
    }

    std::optional<LogRecordPos> BTreeIndex::Get(absl::string_view key) const
    {
        std::shared_lock lock(mutex_);
        if (auto it = index_.find(std::string(key)); it != index_.end())
        {
            return it->second;
        }
        return std::nullopt;
    }

    bool BTreeIndex::Delete(absl::string_view key)
    {
        std::unique_lock lock(mutex_);
        return index_.erase(std::string(key)) > 0;
    }

    std::unique_ptr<BTreeIndex> CreateBTreeIndex()
    {
        return std::make_unique<BTreeIndex>();
    }

    std::unique_ptr<IndexIterator> BTreeIndex::Iterator(bool reverse) const
    {
        std::shared_lock lock(mutex_);

        std::vector<BTreeIndexIterator::Item> items;
        items.reserve(index_.size());

        for (const auto& [key, pos] : index_)
        {
            items.emplace_back(key, pos);
        }

        return std::make_unique<BTreeIndexIterator>(std::move(items), reverse);
    }

} // namespace bitcask
