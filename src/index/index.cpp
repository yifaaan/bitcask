#include "index.h"
#include "btree_index.h"

namespace bitcask
{
    std::unique_ptr<Indexer> CreateIndex(IndexType type)
    {
        switch (type)
        {
        case IndexType::BTree:
            return CreateBTreeIndex();
        // case IndexType::ART:
        //     return CreateARTIndex();
        default:
            return nullptr;
        }
    }
}