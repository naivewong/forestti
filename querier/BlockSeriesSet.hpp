#ifndef BLOCKSERIESSET_H
#define BLOCKSERIESSET_H

#include "querier/ChunkSeriesSetInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace querier {

class BlockSeriesSet : public SeriesSetInterface {
 private:
  std::shared_ptr<ChunkSeriesSetInterface> cs;
  mutable std::shared_ptr<SeriesInterface> cur;
  int64_t min_time;
  int64_t max_time;
  mutable bool err_;

 public:
  BlockSeriesSet(const std::shared_ptr<ChunkSeriesSetInterface> &cs,
                 int64_t min_time, int64_t max_time);

  bool next() const;

  std::shared_ptr<SeriesInterface> at();

  bool error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif