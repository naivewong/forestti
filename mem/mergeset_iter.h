#pragma once

#include "db/dbformat.h"
#include <cstdint>

#include "leveldb/db.h"

namespace tsdb {
namespace mem {

class MergeSet;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
leveldb::Iterator* NewDBIterator(MergeSet* db, const leveldb::Comparator* user_key_comparator,
                        leveldb::Iterator* internal_iter, leveldb::SequenceNumber sequence,
                        uint32_t seed);

}
}