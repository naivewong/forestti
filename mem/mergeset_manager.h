#pragma once

#include <atomic>
#include <vector>

#include "leveldb/options.h"

namespace tsdb {
namespace mem {

class MergeSet;

class MergeSetManager {
private:
  std::vector<MergeSet*> m_;
  std::atomic<uint64_t> id_;
  std::string dir_;
  leveldb::Options opts_;

public:
  MergeSetManager()=default;
  MergeSetManager(const std::string& dir, leveldb::Options opts);
  ~MergeSetManager();
  MergeSet* new_mergeset(const std::string& subdir);
  MergeSet* get_mergeset(uint64_t id) { return m_[id]; }
};

}
}