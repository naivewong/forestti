#include "compact/CompactionChunkSeriesSet.hpp"

#include "base/Logging.hpp"

namespace tsdb {
namespace compact {

CompactionChunkSeriesSet::CompactionChunkSeriesSet(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::shared_ptr<block::ChunkReaderInterface> &cr,
    const std::shared_ptr<tombstone::TombstoneReaderInterface> &tr,
    std::unique_ptr<index::PostingsInterface> &&p, bool is_global_idx)
    : p(std::move(p)),
      ir(ir),
      cr(cr),
      tr(tr),
      csm(new querier::ChunkSeriesMeta()),
      err_(),
      is_global_idx_(is_global_idx) {}

bool CompactionChunkSeriesSet::next() const {
  if (!p->next()) return false;

  csm->clear();
  try {
    csm->intervals = tr->get(p->at());
  } catch (const std::out_of_range &e) {
  }

  if (is_global_idx_) {
    if (!ir->series(p->at(), csm->lset, csm->chunks)) {
      err_.wrap("Error get series " + std::to_string(p->at()));
      return false;
    }
  } else {
    if (!ir->series(p->at(), csm->chunks)) {
      err_.wrap("Error get series " + std::to_string(p->at()));
      return false;
    }
  }

  csm->tsid = p->at();

  // Remove completely deleted chunks.
  if (!csm->intervals.empty()) {
    std::deque<std::shared_ptr<chunk::ChunkMeta>>::iterator it =
        csm->chunks.begin();
    while (it != csm->chunks.end()) {
      if (tombstone::is_subrange((*it)->min_time, (*it)->max_time,
                                 csm->intervals))
        it = csm->chunks.erase(it);
      else
        ++it;
    }
  }

  // Read real chunk for each chunk meta.
  for (int i = 0; i < csm->chunks.size(); i++) {
    bool succeed;
    std::tie(csm->chunks[i]->chunk, succeed) = cr->chunk(csm->chunks[i]->ref);
    if (!succeed) {
      err_.wrap("Chunk " + std::to_string(csm->chunks[i]->ref) + "not found");
      return false;
    }
  }

  return true;
}

// Need DeleteIterator to filter the real chunk.
const std::shared_ptr<querier::ChunkSeriesMeta> &CompactionChunkSeriesSet::at()
    const {
  return csm;
}

bool CompactionChunkSeriesSet::error() const {
  if (err_)
    return true;
  else
    return false;
}

error::Error CompactionChunkSeriesSet::error_detail() const { return err_; }

}  // namespace compact
}  // namespace tsdb