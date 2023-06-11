#ifndef MEMSERIES_H
#define MEMSERIES_H

#include <atomic>

#include "base/Atomic.hpp"
#include "base/Mutex.hpp"
#include "chunk/XORChunk.hpp"
#include "db/DBUtils.hpp"
#include "disk/log_manager.h"
#include "head/HeadUtils.hpp"
#include "label/Label.hpp"

namespace leveldb {
class DB;
class Status;
}  // namespace leveldb

namespace tsdb {
namespace head {

extern int MEM_TUPLE_SIZE;

// Current assumption: data are in-order.
class MemSeries {
 public:
  disk::SpinLock mutex_;
  uint64_t ref;
  uint64_t log_pos;
  uint64_t logical_id;
  label::Labels labels;

  int64_t min_time_;
  int64_t max_time_;
  int64_t global_max_time_;
  int num_samples_;
  int64_t tuple_st_;
  chunk::XORChunk chunk_;
  std::unique_ptr<chunk::ChunkAppenderInterface> appender;

  std::atomic<int64_t> flushed_txn_;
  int64_t log_clean_txn_;  // used to store the temporary max txn when cleaning
                           // logs.

  std::string key_;
  std::string val_;

  // Used for GC.
  std::atomic<uint64_t> access_epoch;
  std::atomic<uint32_t> version;

  int64_t time_boundary_;  // The time boundary of the first sample.

  MemSeries();
  MemSeries(const label::Labels& labels, uint64_t id, uint64_t log_pos = 0);
  MemSeries(label::Labels&& labels, uint64_t id, uint64_t log_pos = 0);

  void update_access_epoch();

  int64_t min_time();
  int64_t max_time();

  leveldb::Status _flush(leveldb::DB* db, int64_t txn);

  bool append(leveldb::DB* db, int64_t timestamp, double value,
              int64_t txn = 0);

  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }

  void release_labels() { label::Labels().swap(labels); }
};

}  // namespace head
}  // namespace tsdb

#endif