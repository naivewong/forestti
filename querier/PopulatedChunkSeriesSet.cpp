#include "querier/PopulatedChunkSeriesSet.hpp"

#include "base/Logging.hpp"

namespace tsdb {
namespace querier {

// Similar to BaseChunkSeriesSet, but it has two extra
// fields: 1.min_time 2.max_time, which are used for filtering the chunks not in
// time range. NOTE(Alec), PopulatedChunkSeriesSet coarse-grained filters the
// chunks using min_time and max_time.
PopulatedChunkSeriesSet::PopulatedChunkSeriesSet(
    const std::shared_ptr<ChunkSeriesSetInterface> &set,
    const std::shared_ptr<block::ChunkReaderInterface> &chunkr,
    int64_t min_time, int64_t max_time)
    : set(set),
      chunkr(chunkr),
      min_time(min_time),
      max_time(max_time),
      cm(new ChunkSeriesMeta()),
      err_(false) {}

// next() always called before at().
const std::shared_ptr<ChunkSeriesMeta> &PopulatedChunkSeriesSet::at() const {
  return cm;
}

bool PopulatedChunkSeriesSet::next() const {
  while (set->next()) {
    cm = set->at();

    while (!cm->chunks.empty()) {
      if (cm->chunks[0]->max_time >= min_time) break;
      cm->chunks.pop_front();
    }

    // This is to delete in place while iterating.
    for (int i = 0, rlen = cm->chunks.size(); i < rlen; i++) {
      int j = i - (rlen - cm->chunks.size());

      // Break out at the first chunk that has no overlap with mint, maxt.
      if (cm->chunks[j]->min_time > max_time) {
        int chunk_size = cm->chunks.size();
        for (int k = j; k < chunk_size; k++) cm->chunks.pop_back();
        break;
      }

      // LOG_DEBUG << "chunk " << cm->chunks[j]->min_time << " " <<
      // cm->chunks[j]->max_time;
      std::tie(cm->chunks[j]->chunk, err_) = chunkr->chunk(cm->chunks[j]->ref);

      err_ = !err_;
      if (err_) {
        // This means that the chunk has be garbage collected. Remove it from
        // the list. Only used in Head --> in-momery ErrNotFound
        // chunks[j]->chunk.reset();
        cm->chunks.clear();
        return false;
      }
    }

    if (cm->chunks.empty() && cm->lset.empty()) {
      continue;
    }

    return true;
  }

  if (set->error()) err_ = true;
  return false;
}

bool PopulatedChunkSeriesSet::error() const { return err_; }

}  // namespace querier
}  // namespace tsdb