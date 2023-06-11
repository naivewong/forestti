#include "mem/indirection_manager.h"

#include <stdlib.h>

#include <algorithm>
#include <limits>

#include "util/coding.h"

namespace tsdb {
namespace mem {

IndirectionManager::IndirectionManager()
    : block_counters_(PRESERVED_BLOCKS), num_slots_(0) {
  blocks_.reserve(PRESERVED_BLOCKS);
  std::vector<std::atomic<uint64_t>> v(PRESERVED_BLOCKS);
  block_counters_[blocks_.size()] = 0;
  blocks_.push_back(std::move(v));
  available_blocks_.push_back(0);
}

uint64_t IndirectionManager::alloc_slot() {
  lock_.lock();
  if (block_counters_[blocks_.size() - 1] >= PRESERVED_BLOCKS) {
    std::vector<std::atomic<uint64_t>> v(PRESERVED_BLOCKS);
    block_counters_[blocks_.size()] = 0;
    blocks_.push_back(std::move(v));
  }

  uint64_t idx = (((uint64_t)(blocks_.size() - 1)) << 32) |
                 (block_counters_[blocks_.size() - 1]++);
  lock_.unlock();
  ++num_slots_;
  return idx;
}

void IndirectionManager::set_slot(uint64_t idx, uint64_t v) {
  blocks_[idx >> 32][idx & 0xffffffff].store(v);
}

bool IndirectionManager::cas_slot(uint64_t idx, uint64_t old_val,
                                  uint64_t new_val) {
  return blocks_[idx >> 32][idx & 0xffffffff].compare_exchange_weak(old_val,
                                                                    new_val);
}

uint64_t IndirectionManager::read_slot(uint64_t idx) {
  return blocks_[idx >> 32][idx & 0xffffffff].load();
}

uint64_t IndirectionManager::check_read_slot(uint64_t idx) {
  if ((idx >> 32) >= blocks_.size() ||
      (idx & 0xffffffff) >= block_counters_[idx >> 32])
    return std::numeric_limits<uint64_t>::max();
  return blocks_[idx >> 32][idx & 0xffffffff].load();
}

void IndirectionManager::iter(void (*cb)(uint64_t v)) {
  for (size_t i = 0; i < blocks_.size(); i++) {
    for (size_t j = 0; j < block_counters_[i]; j++) {
      cb(blocks_[i][j]);
    }
  }
}

std::vector<std::pair<uint64_t, uint64_t>> IndirectionManager::get_ids() {
  std::vector<std::pair<uint64_t, uint64_t>> ids;
  uint64_t counter = 0;
  for (size_t i = 0; i < blocks_.size(); i++) {
    for (size_t j = 0; j < block_counters_[i]; j++) {
      ids.push_back(std::make_pair(counter++, blocks_[i][j].load()));
    }
  }
  return ids;
}

uint64_t IndirectionManager::random_alloc() {
  lock_.lock();
  int block = rand() % available_blocks_.size();
  uint64_t idx = (((uint64_t)(available_blocks_[block])) << 32) |
                 block_counters_[available_blocks_[block]]++;
  if (block_counters_[available_blocks_[block]] == 65536)
    available_blocks_.erase(available_blocks_.begin() + block);
  ++num_slots_;
  if (num_slots_ > 0.9 * 65536 * blocks_.size()) {
    available_blocks_.push_back(blocks_.size());
    std::vector<std::atomic<uint64_t>> v(PRESERVED_BLOCKS);
    block_counters_[blocks_.size()] = 0;
    blocks_.push_back(std::move(v));
  }
  lock_.unlock();
  return idx;
}

uint64_t IndirectionManager::specific_alloc(int block) {
  lock_.lock();
  if (block_counters_[block] >= PRESERVED_BLOCKS) {
    lock_.unlock();
    return random_alloc();
  }
  uint64_t idx = (((uint64_t)(block)) << 32) | block_counters_[block]++;
  if (block_counters_[block] == 65536)
    available_blocks_.erase(std::lower_bound(available_blocks_.begin(),
                                             available_blocks_.end(), block));

  ++num_slots_;
  if (num_slots_ > 0.9 * 65536 * blocks_.size()) {
    available_blocks_.push_back(blocks_.size());
    std::vector<std::atomic<uint64_t>> v(PRESERVED_BLOCKS);
    block_counters_[blocks_.size()] = 0;
    blocks_.push_back(std::move(v));
  }
  lock_.unlock();
  return idx;
}

leveldb::Status IndirectionManager::snapshot(const std::string& dir) {
  leveldb::WritableFile* file;
  leveldb::Status s =
      leveldb::Env::Default()->NewAppendableFile(dir + "/indirection", &file);
  if (!s.ok()) return s;
  std::string record, header;
  for (size_t i = 0; i < blocks_.size(); i++) {
    leveldb::PutVarint64(&record, block_counters_[i].load());
    for (size_t j = 0; j < block_counters_[i].load(); j++)
      leveldb::PutVarint64(&record, blocks_[i][j].load());
    leveldb::PutVarint64(&header, record.size());
    file->Append(header);
    file->Append(record);
    header.clear();
    record.clear();
  }
  delete file;

  return leveldb::Status::OK();
}

leveldb::Status IndirectionManager::recover_from_snapshot(
    const std::string& dir) {
  leveldb::RandomAccessFile* file;
  uint64_t offset = 0;
  leveldb::Status s =
      leveldb::Env::Default()->NewRandomAccessFile(dir + "/indirection", &file);
  if (!s.ok()) return s;

  blocks_.clear();

  char header[10];
  leveldb::Slice record;
  uint64_t record_size, v;
  int block_id = 0;
  while (offset < file->FileSize()) {
    s = file->Read(offset, 10, &record, header);
    if (!s.ok()) return s;
    const char* p = leveldb::GetVarint64Ptr(record.data(), record.data() + 10,
                                            &record_size);
    offset += p - record.data();

    char* buf = new char[record_size];
    s = file->Read(offset, record_size, &record, buf);
    if (!s.ok()) {
      delete[] buf;
      return s;
    }

    leveldb::GetVarint64(&record, &v);
    block_counters_[block_id] = v;
    ++block_id;

    std::vector<std::atomic<uint64_t>> vec(PRESERVED_BLOCKS);
    int idx = 0;
    while (record.size() > 0) {
      leveldb::GetVarint64(&record, &v);
      vec[idx++] = v;
    }
    blocks_.push_back(std::move(vec));

    delete[] buf;
    offset += record_size;
  }

  return leveldb::Status::OK();
}

}  // namespace mem
}  // namespace tsdb