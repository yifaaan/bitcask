#include "index.h"
#include "art_index.h"
#include "btree_index.h"

namespace bitcask
{

    std::unique_ptr<Indexer> CreateIndexer(IndexType type)
    {
        switch (type)
        {
        case IndexType::BTree:
            return CreateBTreeIndex();
        case IndexType::ART:
            return CreateAdaptiveRadixTreeIndex();
        default:
            return nullptr;
        }
    }

} // namespace bitcask
