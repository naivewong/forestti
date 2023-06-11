#include "head/MemSeries.hpp"

#include <chrono>

#include "base/Logging.hpp"
#include "base/TSDBException.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "db/DBUtils.hpp"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace tsdb {
namespace head {

int MEM_TUPLE_SIZE = 32;

MemSeries::MemSeries()
    : mutex_(),
      ref(0),
      log_pos(0),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      flushed_txn_(0),
      log_clean_txn_(-1),
      chunk_(LEVELDB_VALUE_HEADER_SIZE),
      appender(chunk_.appender()),
      access_epoch(0),
      version(0),
      time_boundary_(-1) {}

MemSeries::MemSeries(const label::Labels& labels, uint64_t id, uint64_t log_pos)
    : mutex_(),
      ref(id),
      log_pos(log_pos),
      labels(labels),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      flushed_txn_(0),
      log_clean_txn_(-1),
      chunk_(LEVELDB_VALUE_HEADER_SIZE),
      appender(chunk_.appender()),
      access_epoch(0),
      version(0),
      time_boundary_(-1) {}

MemSeries::MemSeries(label::Labels&& labels, uint64_t id, uint64_t log_pos)
    : mutex_(),
      ref(id),
      log_pos(log_pos),
      labels(std::move(labels)),
      min_time_(std::numeric_limits<int64_t>::max()),
      max_time_(std::numeric_limits<int64_t>::min()),
      global_max_time_(std::numeric_limits<int64_t>::min()),
      num_samples_(0),
      flushed_txn_(0),
      log_clean_txn_(-1),
      chunk_(LEVELDB_VALUE_HEADER_SIZE),
      appender(chunk_.appender()),
      access_epoch(0),
      version(0),
      time_boundary_(-1) {}

int64_t MemSeries::min_time() {
  if (min_time_ == std::numeric_limits<int64_t>::max())
    return std::numeric_limits<int64_t>::min();
  return min_time_;
}

int64_t MemSeries::max_time() {
  if (max_time_ == std::numeric_limits<int64_t>::min())
    return std::numeric_limits<int64_t>::max();
  return max_time_;
}

leveldb::Status MemSeries::_flush(leveldb::DB* db, int64_t txn) {
  key_.clear();
  leveldb::encodeKey(&key_, ref, tuple_st_);

  // val_.clear();
  chunk_.bstream.stream[0] = 1;  // Has header.
  leveldb::EncodeFixed64BE(
      reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 1), logical_id);
  leveldb::EncodeFixed64BE(
      reinterpret_cast<char*>(&chunk_.bstream.stream[0] + 9), txn);
  // leveldb::PutVarint64(&val_, txn);
  // leveldb::PutVarint64(&val_, max_time_ - min_time_);
  // leveldb::PutVarint32(&val_, chunk_.size());
  // val_.append(reinterpret_cast<const char*>(chunk_.bytes()), chunk_.size());
  leveldb::Status s;
  // if (db)
  s = db->Put(leveldb::WriteOptions(), key_,
              leveldb::Slice(reinterpret_cast<char*>(&chunk_.bstream.stream[0]),
                             chunk_.bstream.stream.size()));

  appender.reset();
  chunk_ = chunk::XORChunk(LEVELDB_VALUE_HEADER_SIZE);
  appender = chunk_.appender();

  num_samples_ = 0;
  min_time_ = std::numeric_limits<int64_t>::max();
  max_time_ = std::numeric_limits<int64_t>::min();
  return s;
}

bool MemSeries::append(leveldb::DB* db, int64_t timestamp, double value,
                       int64_t txn) {
  bool flushed = false;
  if (timestamp > global_max_time_) global_max_time_ = timestamp;

  if (min_time_ == std::numeric_limits<int64_t>::max()) min_time_ = timestamp;

  int64_t time_boundary =
      timestamp / leveldb::PARTITION_LENGTH * leveldb::PARTITION_LENGTH;
  if (time_boundary != time_boundary_ && time_boundary_ != -1 &&
      num_samples_ != 0) {
    // This cover both the previous partition and the next partition.
    _flush(db, txn);
    flushed = true;
  }

  max_time_ = timestamp;
  appender->append(timestamp, value);
  ++num_samples_;

  if (num_samples_ == 1) tuple_st_ = timestamp;

  if (num_samples_ == MEM_TUPLE_SIZE) {
    _flush(db, txn);
    flushed = true;
  }
  time_boundary_ = time_boundary;

  return flushed;
}

void MemSeries::update_access_epoch() {
  access_epoch.store(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count());
}

}  // namespace head
}  // namespace tsdb