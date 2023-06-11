#pragma once

#include <limits>

#include "base/Error.hpp"

namespace tsdb {
namespace head {

extern int LEVELDB_VALUE_HEADER_SIZE;

// ErrNotFound is returned if a looked up resource was not found.
extern const error::Error ErrNotFound;

// ErrOutOfOrderSample is returned if an appended sample has a
// timestamp larger than the most recent sample.
extern const error::Error ErrOutOfOrderSample;

// ErrAmendSample is returned if an appended sample has the same timestamp
// as the most recent sample but a different value.
extern const error::Error ErrAmendSample;

// ErrOutOfBounds is returned if an appended sample is out of the
// writable time range.
extern const error::Error ErrOutOfBounds;

extern const int SAMPLES_PER_CHUNK;

extern const int STRIPE_SIZE;
extern const uint64_t STRIPE_MASK;

class Sample {
 public:
  int64_t t;
  double v;

  Sample() {
    t = std::numeric_limits<int64_t>::min();
    v = 0;
  }

  Sample(int64_t t, double v) {
    this->t = t;
    this->v = v;
  }

  void reset() {
    t = std::numeric_limits<int64_t>::min();
    v = 0;
  }

  void reset(int64_t t, double v) {
    this->t = t;
    this->v = v;
  }

  Sample& operator=(const Sample& s) {
    t = s.t;
    v = s.v;
    return (*this);
  }
};

// compute_chunk_end_time estimates the end timestamp based the beginning of a
// chunk, its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
int64_t compute_chunk_end_time(int64_t min_time, int64_t max_time,
                               int64_t next_at);

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panics if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
uint64_t pack_chunk_id(uint64_t series_id, uint64_t chunk_id);
std::pair<uint64_t, uint64_t> unpack_chunk_id(uint64_t id);

void mem_usage(double& vm_usage, double& resident_set);

}  // namespace head
}  // namespace tsdb