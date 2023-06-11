#include "head/HeadUtils.hpp"

#include <fstream>

#include "base/Logging.hpp"

// #include <iostream>

namespace tsdb {
namespace head {

int LEVELDB_VALUE_HEADER_SIZE = 17;

const error::Error ErrNotFound = error::Error("not found");
const error::Error ErrOutOfOrderSample = error::Error("out of order sample");
const error::Error ErrAmendSample = error::Error("amending sample");
const error::Error ErrOutOfBounds = error::Error("out of bounds");

const int SAMPLES_PER_CHUNK = 120;

const int STRIPE_SIZE = 1 << 14;
const uint64_t STRIPE_MASK = STRIPE_SIZE - 1;

// compute_chunk_end_time estimates the end timestamp based the beginning of a
// chunk, its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
int64_t compute_chunk_end_time(int64_t min_time, int64_t max_time,
                               int64_t next_at) {
  int a = (next_at - min_time) /
          ((max_time - min_time + 1) * 4);  // Avoid dividing by 0
  if (a == 0) return next_at;
  return (min_time + (next_at - min_time) / a);
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panics if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
uint64_t pack_chunk_id(uint64_t series_id, uint64_t chunk_id) {
  // std::cerr << series_id << " " << (static_cast<uint64_t>(1) << 40) -
  // static_cast<uint64_t>(1) << std::endl;
  if (series_id > (static_cast<uint64_t>(1) << 40) - static_cast<uint64_t>(1))
    LOG_FATAL << "head series id exceeds 5 bytes";
  if (chunk_id > (static_cast<uint64_t>(1) << 24) - static_cast<uint64_t>(1))
    LOG_FATAL << "head chunk id exceeds 3 bytes";
  return (series_id << 24) | chunk_id;
}
std::pair<uint64_t, uint64_t> unpack_chunk_id(uint64_t id) {
  return {id >> 24, (id << 40) >> 40};
}

void mem_usage(double& vm_usage, double& resident_set) {
  vm_usage = 0.0;
  resident_set = 0.0;
  std::ifstream stat_stream("/proc/self/stat",
                            std::ios_base::in);  // get info from proc directory
  // create some variables to get info
  std::string pid, comm, state, ppid, pgrp, session, tty_nr;
  std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  std::string utime, stime, cutime, cstime, priority, nice;
  std::string O, itrealvalue, starttime;
  unsigned long vsize;
  long rss;
  stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
      tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
      stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
      starttime >> vsize >> rss;  // don't care about the rest
  stat_stream.close();
  long page_size_kb = sysconf(_SC_PAGE_SIZE) /
                      1024;  // for x86-64 is configured to use 2MB pages
  vm_usage = vsize / 1024.0;
  resident_set = rss * page_size_kb;
}

}  // namespace head
}  // namespace tsdb