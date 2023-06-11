#include "querier/BaseChunkSeriesSet.hpp"

#include "base/Logging.hpp"

namespace tsdb {
namespace querier {

// BaseChunkSeriesSet loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be
// unset
//
// The chunk pointer in ChunkMeta is not set
// NOTE(Alec), BaseChunkSeriesSet fine-grained filters the chunks using
// tombstone.
BaseChunkSeriesSet::BaseChunkSeriesSet(
    const std::shared_ptr<block::IndexReaderInterface>& ir,
    std::unique_ptr<index::PostingsInterface>&& p, bool is_global_idx,
    const std::shared_ptr<tombstone::TombstoneReaderInterface>& tr,
    const std::vector<std::shared_ptr<label::MatcherInterface>>& list,
    bool group_)
    : p_(std::move(p)),
      ir_(ir),
      tr_(tr),
      cm_(new ChunkSeriesMeta()),
      err_(false),
      group_(group_),
      is_global_idx_(is_global_idx) {
  // std::tie(p, err_) = postings_for_matchers(ir, list);
  // err_ = !err_;
  // if (err_) {
  //   LOG_DEBUG << "error postings_for_matchers";
  //   this->p.reset();
  //   this->ir.reset();
  //   this->tr.reset();
  //   this->cm_.reset();
  // }
}

// next() always called before at().
const std::shared_ptr<ChunkSeriesMeta>& BaseChunkSeriesSet::at() const {
  return cm_;
}

bool BaseChunkSeriesSet::next() const {
  if (err_) return false;

  while (p_->next()) {
    uint64_t ref = p_->at();
    cm_->clear();
    cm_->tsid = ref;
    // Get labels and deque of ChunkMeta of the corresponding series.
    if (is_global_idx_) {
      if (!ir_->series(ref, cm_->lset, cm_->chunks)) {
        // TODO, ErrNotFound
        // err_ = true;
        // return false;
        // LOG_DEBUG << "head not found:" << ref;
        continue;
      }
      // LOG_DEBUG << "head chunks size:" << cm_->chunks.size();
    } else {
      if (!ir_->series(ref, cm_->chunks)) {
        // LOG_DEBUG << "block not found:" << ref;
        continue;
      }
    }

    // Get Intervals from MemTombstones
    // LOG_DEBUG << "cm_->chunks size:" << (int)(cm_->chunks.size());
    try {
      // LOG_DEBUG << ref;
      if (group_)
        cm_->intervals = tr_->get(cm_->chunks.front()->logical_group_ref);
      else
        cm_->intervals = tr_->get(ref);
    } catch (const std::out_of_range& e) {
    }

    if (!(cm_->intervals).empty()) {
      // LOG_DEBUG << "tombstone not empty";
      std::deque<std::shared_ptr<chunk::ChunkMeta>>::iterator it =
          cm_->chunks.begin();
      while (it != cm_->chunks.end()) {
        if (tombstone::is_subrange((*it)->min_time, (*it)->max_time,
                                   cm_->intervals))
          it = cm_->chunks.erase(it);
        else
          ++it;
      }
    }
    return true;
  }
  return false;
}

bool BaseChunkSeriesSet::error() const { return err_; }

}  // namespace querier
}  // namespace tsdb