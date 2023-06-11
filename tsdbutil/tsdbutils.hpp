#ifndef FILEUTILS_H
#define FILEUTILS_H

// #include "head/GroupMemSeries.hpp"
#include "head/MemSeries.hpp"
#include "label/Label.hpp"
#include "tombstone/Interval.hpp"

namespace tsdb {
namespace tsdbutil {

// const int ErrNotFound = 1;

std::string filepath_join(const std::string& f1, const std::string& f2);

bool is_number(const std::string& s);

std::pair<int64_t, int64_t> clamp_interval(int64_t a, int64_t b, int64_t mint,
                                           int64_t maxt);

class Stone {
 public:
  uint64_t ref;
  tombstone::Intervals itvls;

  Stone() = default;
  Stone(uint64_t ref, const tombstone::Intervals& itvls)
      : ref(ref), itvls(itvls) {}
};

// RefSeries is the series labels with the series ID.
class RefSeries {
 public:
  uint64_t ref;
  label::Labels lset;
  label::Labels* lset_ptr;
  int64_t flushed_txn;
  int64_t log_clean_txn;
  head::MemSeries* series_ptr;

  RefSeries() : lset_ptr(nullptr), series_ptr(nullptr) {}
  RefSeries(uint64_t ref, const label::Labels& lset,
            head::MemSeries* series = nullptr)
      : ref(ref),
        lset(lset),
        lset_ptr(nullptr),
        flushed_txn(0),
        log_clean_txn(0),
        series_ptr(series) {}
  RefSeries(uint64_t ref, label::Labels* lset,
            head::MemSeries* series = nullptr)
      : ref(ref),
        lset_ptr(lset),
        flushed_txn(0),
        log_clean_txn(0),
        series_ptr(series) {}
};

// class RefGroupSeries {
//  public:
//   uint64_t group_ref;
//   std::deque<RefSeries> series;

//   RefGroupSeries() = default;
//   RefGroupSeries(uint64_t group_ref) : group_ref(group_ref) {}
//   RefGroupSeries(uint64_t group_ref, const std::deque<RefSeries> &series)
//       : group_ref(group_ref), series(series) {}

//   void push_back(uint64_t ref, const label::Labels &lset) {
//     series.emplace_back(ref, lset);
//   }
//   void push_back(const RefSeries &rs) { series.push_back(rs); }
// };

// RefSample is a timestamp/value pair associated with a reference to a series.
class RefSample {
 public:
  uint64_t ref;
  uint64_t logical_id;
  int64_t t;
  double v;
  int64_t txn;
  head::MemSeries* series_ptr = nullptr;

  RefSample() = default;
  RefSample(uint64_t ref, int64_t t, double v) : ref(ref), t(t), v(v) {}
  RefSample(uint64_t ref, int64_t t, double v, int64_t txn)
      : ref(ref), t(t), v(v), txn(txn) {}
  RefSample(uint64_t ref, uint64_t logical_id, int64_t t, double v, int64_t txn)
      : ref(ref), logical_id(logical_id), t(t), v(v), txn(txn) {}
  RefSample(uint64_t ref, int64_t t, double v, head::MemSeries* series)
      : ref(ref), t(t), v(v), series_ptr(series) {}
  RefSample(uint64_t ref, int64_t t, double v, int64_t txn,
            head::MemSeries* series)
      : ref(ref), t(t), v(v), txn(txn), series_ptr(series) {}
  // std::pair<bool, bool> append(int64_t timestamp, double value) {
  //   if (series)
  //     return series->append(timestamp, value);
  //   else
  //     return series_ptr->append(timestamp, value);
  // }
  // base::MutexLock& get_lock() {
  //   if (series)
  //     return series->mutex_;
  //   else
  //     return series_ptr->mutex_;
  // }
};

// WAL specific record.
typedef uint8_t RECORD_ENTRY_TYPE;
extern const RECORD_ENTRY_TYPE RECORD_INVALID;
extern const RECORD_ENTRY_TYPE RECORD_SERIES;
extern const RECORD_ENTRY_TYPE RECORD_SAMPLES;
extern const RECORD_ENTRY_TYPE RECORD_TOMBSTONES;
extern const RECORD_ENTRY_TYPE RECORD_GROUP_SERIES;
extern const RECORD_ENTRY_TYPE RECORD_GROUP_SAMPLES;
extern const RECORD_ENTRY_TYPE RECORD_GROUP_TOMBSTONES;

}  // namespace tsdbutil
}  // namespace tsdb

#endif