#include "BTreeIndex.h"

#include "Data/LogRecord.h"

namespace bitcask
{

	std::optional<LogRecordPos> BTreeIndex::Put(std::string_view key, const LogRecordPos& pos)
	{
		std::unique_lock lock(mutex);
		auto it = btree.find(std::string(key));
		if (it != btree.end())
		{
			auto old = std::exchange(it->second, pos);
			return old;
		}
		btree[std::string(key)] = pos;
		return std::nullopt;
	}

	absl::StatusOr<LogRecordPos> BTreeIndex::Get(std::string_view key) const
	{
		std::shared_lock lock(mutex);
		auto it = btree.find(std::string(key));
		if (it == btree.end())
		{
			return absl::NotFoundError("key not found");
		}
		return it->second;
	}

	std::optional<LogRecordPos> BTreeIndex::Delete(std::string_view key)
	{
		std::unique_lock lock(mutex);
		auto it = btree.find(std::string(key));
		if (it == btree.end())
		{
			return std::nullopt;
		}
		auto old = it->second;
		btree.erase(it);
		return old;
	}

	size_t BTreeIndex::Size() const
	{
		std::shared_lock lock(mutex);
		return btree.size();
	}

	void BTreeIndex::Clear()
	{
		std::unique_lock lock(mutex);
		btree.clear();
	}

	std::unique_ptr<Index> bitcask::NewIndex(IndexType type)
	{
		switch (type)
		{
		case IndexType::BTree:
			return std::make_unique<BTreeIndex>();
		case IndexType::FlatHash:
		default:
			return std::make_unique<BTreeIndex>();
		}
	}

	std::unique_ptr<IndexIterator> BTreeIndex::NewIterator() const
	{
		std::shared_lock lock(mutex);

		std::vector<IndexEntry> entries;
		entries.reserve(btree.size());

		for (const auto& [key, pos] : btree)
		{
			entries.push_back(IndexEntry{.key = key, .pos = pos,});
		}

		return std::make_unique<BTreeIndexIterator>(std::move(entries));
	}

} // namespace bitcask
