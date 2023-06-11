#include "querier/BlockQuerier.hpp"

#include "base/Logging.hpp"
#include "querier/BaseChunkSeriesSet.hpp"
#include "querier/BlockSeriesSet.hpp"
#include "querier/EmptySeriesSet.hpp"
#include "querier/PopulatedChunkSeriesSet.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace querier {

BlockQuerier::BlockQuerier(const std::shared_ptr<block::BlockInterface> &block,
                           int64_t min_time, int64_t max_time,
                           bool contains_gindex)
    : min_time(min_time),
      max_time(max_time),
      contains_global_index_(contains_gindex) {
  bool succeed_;
  std::tie(indexr, succeed_) = block->index();
  if (!succeed_) {
    // LOG_ERROR << "Error getting block index";
    err_.set("error get block index");
    return;
  }
  std::tie(chunkr, succeed_) = block->chunks();
  if (!succeed_) {
    // LOG_ERROR << "Error getting block chunks";
    err_.set("error get block chunks");
    return;
  }
  std::tie(tombstones, succeed_) = block->tombstones();
  if (!succeed_) {
    // LOG_ERROR << "Error getting block tombstones";
    err_.set("error get block tombstones");
    return;
  }
}

std::shared_ptr<SeriesSetInterface> BlockQuerier::select(
    const std::vector<std::shared_ptr<label::MatcherInterface>> &l,
    std::unique_ptr<index::PostingsInterface> &&p) const {
  std::shared_ptr<ChunkSeriesSetInterface> base =
      std::make_shared<BaseChunkSeriesSet>(
          indexr, std::move(p), contains_global_index_, tombstones, l);
  if (base->error()) {
    // Happens when it cannot find the matching postings.
    // LOG_ERROR << "Error get BaseChunkSeriesSet";
    return nullptr;
  }
  return std::make_shared<BlockSeriesSet>(
      std::make_shared<PopulatedChunkSeriesSet>(base, chunkr, min_time,
                                                max_time),
      min_time, max_time);
}

std::vector<std::string> BlockQuerier::label_values(
    const std::string &s) const {
  return indexr->label_values({s});
}

std::vector<std::string> BlockQuerier::label_names() const {
  return indexr->label_names();
}

}  // namespace querier
}  // namespace tsdb