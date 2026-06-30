#include "BTreeIndex.h"

namespace bitcask
{

absl::Status BTreeIndex::Put(std::string_view key, const LogRecordPos& pos)
{
	std::unique_lock lock(mutex);
	btree[std::string(key)] = pos;
	return absl::OkStatus();
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

absl::Status BTreeIndex::Delete(std::string_view key)
{
	std::unique_lock lock(mutex);
	auto erased = btree.erase(std::string(key));
	if (erased == 0)
	{
		return absl::NotFoundError("key not found");
	}
	return absl::OkStatus();
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

} // namespace bitcask
