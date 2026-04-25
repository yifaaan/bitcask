#include "index.h"
#include "btree_index.h"

namespace bitcask
{

    std::unique_ptr<Indexer> CreateIndexer(IndexType type)
    {
        switch (type)
        {
        case IndexType::BTree:
            return CreateBTreeIndex();
        default:
            return nullptr;
        }
    }

} // namespace bitcask
