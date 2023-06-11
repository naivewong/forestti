#pragma once

#include <atomic>
#include <vector>

#include "disk/log_manager.h"
#include "leveldb/env.h"

#define PRESERVED_BLOCKS 65536

namespace tsdb {
namespace mem {

class IndirectionManager {
 private:
  std::vector<std::vector<std::atomic<uint64_t>>> blocks_;
  std::vector<std::atomic<uint64_t>> block_counters_;
  std::atomic<uint64_t> num_slots_;
  disk::SpinLock lock_;

  std::vector<int> available_blocks_;

 public:
  IndirectionManager();

  uint64_t alloc_slot();
  bool cas_slot(uint64_t idx, uint64_t old_val, uint64_t new_val);
  void set_slot(uint64_t idx, uint64_t v);
  uint64_t read_slot(uint64_t idx);
  uint64_t check_read_slot(uint64_t idx);

  void iter(void (*cb)(uint64_t v));

  std::vector<std::pair<uint64_t, uint64_t>> get_ids();

  uint64_t random_alloc();
  uint64_t specific_alloc(int block);

  leveldb::Status snapshot(const std::string& dir);
  leveldb::Status recover_from_snapshot(const std::string& dir);
};

}  // namespace mem
}  // namespace tsdb