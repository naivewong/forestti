#pragma once

#include <list>
#include <unordered_map>
#include <unordered_set>

#include "head/MemSeries.hpp"
#include "label/Label.hpp"

namespace tsdb {
namespace head {

// SeriesHashmap is a simple hashmap for MemSeries by their label set. It is
// built on top of a regular hashmap and holds a slice of series to resolve hash
// collisions. Its methods require the hash to be submitted with it to avoid
// re-computations throughout the code.
class SeriesHashmap {
 public:
  std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>> map;

 public:
  SeriesHashmap() = default;

  std::shared_ptr<MemSeries> get(uint64_t hash, const label::Labels &lset);
  void set(uint64_t hash, const std::shared_ptr<MemSeries> &s);

  // Return the iterator on hash or the next iterator if this one being deleted.
  std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator
  del(uint64_t hash, const label::Labels &lset);
  std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator
  del(uint64_t hash, uint64_t ref);

  // void gc(int64_t min_time, std::unordered_set<uint64_t>& rm_series, int&
  // rm_chunks);
};

}  // namespace head
}  // namespace tsdb
