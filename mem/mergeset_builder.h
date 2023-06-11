#pragma once

#include "leveldb/status.h"

#include <set>
#include <vector>

namespace leveldb {
struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
}

namespace tsdb {
namespace mem {

struct art_tree;

class VersionEdit;
class VersionSet;

leveldb::Status BuildTable(const std::string& dbname, leveldb::Env* env, const leveldb::Options& options,
                           leveldb::TableCache* table_cache, leveldb::Iterator* iter,
                           leveldb::FileMetaData* meta);

leveldb::Status BuildTablesFromTrie(const std::string& dbname, leveldb::Env* env, const leveldb::Options& options,
                            leveldb::TableCache* table_cache, struct art_tree* tree,
                            std::vector<leveldb::FileMetaData>* metas, VersionSet* const vset,
                            std::set<uint64_t>* pending_outputs);

}
}