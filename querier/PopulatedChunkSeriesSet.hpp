#ifndef POPULATEDCHUNKSERIESSET_H
#define POPULATEDCHUNKSERIESSET_H

#include "block/ChunkReaderInterface.hpp"
#include "querier/ChunkSeriesMeta.hpp"
#include "querier/ChunkSeriesSetInterface.hpp"

namespace tsdb {
namespace querier {

// Similar to BaseChunkSeriesSet, but it has two extra
// fields: 1.min_time 2.max_time, which are used for filtering the chunks not in
// time range. NOTE(Alec), PopulatedChunkSeriesSet coarse-grained filters the
// chunks using min_time and max_time.
class PopulatedChunkSeriesSet : public ChunkSeriesSetInterface {
 private:
  std::shared_ptr<ChunkSeriesSetInterface> set;
  std::shared_ptr<block::ChunkReaderInterface> chunkr;

  int64_t min_time;
  int64_t max_time;

  mutable std::shared_ptr<ChunkSeriesMeta> cm;
  mutable bool err_;

 public:
  PopulatedChunkSeriesSet(
      const std::shared_ptr<ChunkSeriesSetInterface> &set,
      const std::shared_ptr<block::ChunkReaderInterface> &chunkr,
      int64_t min_time, int64_t max_time);

  // next() always called before at().
  const std::shared_ptr<ChunkSeriesMeta> &at() const;

  bool next() const;

  bool error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif