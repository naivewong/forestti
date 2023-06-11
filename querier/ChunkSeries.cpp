#include "querier/ChunkSeries.hpp"

#include "querier/ChunkSeriesIterator.hpp"
#include "querier/ChunkSeriesMeta.hpp"

namespace tsdb {
namespace querier {

// chunkSeries is a series that is backed by a sequence of chunks holding
// time series data.
ChunkSeries::ChunkSeries(const std::shared_ptr<ChunkSeriesMeta> &cm,
                         int64_t min_time, int64_t max_time)
    : cm(cm), min_time(min_time), max_time(max_time) {}

const label::Labels &ChunkSeries::labels() const { return cm->lset; }

uint64_t ChunkSeries::tsid() const { return cm->tsid; }

std::unique_ptr<SeriesIteratorInterface> ChunkSeries::iterator() {
  if (cm->chunks.empty()) {
    return std::unique_ptr<SeriesIteratorInterface>(new EmptySeriesIterator());
  }
  return std::unique_ptr<SeriesIteratorInterface>(
      new ChunkSeriesIterator(cm->chunks, cm->intervals, min_time, max_time));
}

}  // namespace querier
}  // namespace tsdb