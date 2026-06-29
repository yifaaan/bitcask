#include "BTreeIndex.h"

namespace bitcask
{

absl::Status BTreeIndex::Put(std::string_view key, const LogRecordPos& pos)
{
	std::unique_lock lock(mutex);
	map[std::string(key)] = pos;
	return absl::OkStatus();
}

absl::StatusOr<LogRecordPos> BTreeIndex::Get(std::string_view key) const
{
	std::shared_lock lock(mutex);
	auto it = map.find(std::string(key));
	if (it == map.end())
	{
		return absl::NotFoundError("key not found");
	}
	return it->second;
}

absl::Status BTreeIndex::Delete(std::string_view key)
{
	std::unique_lock lock(mutex);
	auto erased = map.erase(std::string(key));
	if (erased == 0)
	{
		return absl::NotFoundError("key not found");
	}
	return absl::OkStatus();
}

size_t BTreeIndex::Size() const
{
	std::shared_lock lock(mutex);
	return map.size();
}

void BTreeIndex::Clear()
{
	std::unique_lock lock(mutex);
	map.clear();
}

std::unique_ptr<Index> CreateIndex(IndexType type)
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
