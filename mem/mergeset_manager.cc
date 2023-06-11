#include <boost/filesystem.hpp>

#include "mem/mergeset.h"
#include "mem/mergeset_manager.h"

namespace tsdb {
namespace mem {

MergeSetManager::MergeSetManager(const std::string& dir, leveldb::Options opts): m_(65536, nullptr), id_(0), dir_(dir), opts_(opts) {
  boost::filesystem::remove_all(dir);
  boost::filesystem::create_directories(dir);
}

MergeSetManager::~MergeSetManager() {
  for (size_t i = 0; i < m_.size(); i++) {
    if (m_[i])
      delete m_[i];
  }
}

MergeSet* MergeSetManager::new_mergeset(const std::string& subdir) {
  uint64_t idx = id_++;
  leveldb::DestroyDB(dir_ + "/" + subdir, opts_);
  MergeSet* m;
  leveldb::Status s = MergeSet::MergeSetOpen(opts_, dir_ + "/" + subdir, &m);
  if (!s.ok()) return nullptr;
  m_[idx] = m;
  return m;
}

}
}