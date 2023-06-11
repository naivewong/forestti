#include "querier/BlockSeriesSet.hpp"

#include "querier/ChunkSeries.hpp"

namespace tsdb {
namespace querier {

BlockSeriesSet::BlockSeriesSet(
    const std::shared_ptr<ChunkSeriesSetInterface> &cs, int64_t min_time,
    int64_t max_time)
    : cs(cs), min_time(min_time), max_time(max_time), err_(false) {}

bool BlockSeriesSet::next() const {
  while (cs->next()) {
    cur.reset(new ChunkSeries(cs->at(), min_time, max_time));
    return true;
  }
  if (cs->error()) {
    cur.reset();
    err_ = true;
  }
  return false;
}

std::shared_ptr<SeriesInterface> BlockSeriesSet::at() { return cur; }

bool BlockSeriesSet::error() const { return err_; }

}  // namespace querier
}  // namespace tsdb