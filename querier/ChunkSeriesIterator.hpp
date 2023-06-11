#ifndef CHUNKSERIESITERATOR_H
#define CHUNKSERIESITERATOR_H

#include <deque>

#include "chunk/ChunkMeta.hpp"
#include "querier/SeriesIteratorInterface.hpp"
#include "tombstone/Interval.hpp"

namespace tsdb {
namespace querier {

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks
// which belong to EXACTLY ONE SERIES.
class ChunkSeriesIterator : public SeriesIteratorInterface {
 private:
  const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks;
  mutable int i;
  mutable std::unique_ptr<chunk::ChunkIteratorInterface> cur;
  int64_t min_time;
  int64_t max_time;
  const tombstone::Intervals &intervals;
  mutable bool err_;

 public:
  ChunkSeriesIterator(
      const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks,
      const tombstone::Intervals &intervals, int64_t min_time,
      int64_t max_time);

  // This method is not available in ChunkIterator
  bool seek(int64_t t) const;

  std::pair<int64_t, double> at() const;

  bool next() const;

  bool error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif