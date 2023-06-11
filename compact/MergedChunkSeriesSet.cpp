#include "compact/MergedChunkSeriesSet.hpp"

namespace tsdb {
namespace compact {

MergedChunkSeriesSet::MergedChunkSeriesSet(
    const std::shared_ptr<querier::ChunkSeriesSets> &sets)
    : sets(sets), csm(new querier::ChunkSeriesMeta()) {
  // To move one step for each SeriesInterface.
  sets->next();
  if (sets->empty()) err_.wrap("Empty sets");
}

bool MergedChunkSeriesSet::next_helper() const {
  // To move one step for the former SeriesInterface s.
  sets->next(id);

  csm->clear();
  id.clear();

  if (sets->empty()) {
    err_.wrap("Empty sets");
    return false;
  }

  csm->tsid = sets->at(0)->at()->tsid;
  csm->lset = sets->at(0)->at()->lset;
  id.push_back(0);
  for (int i = 1; i < sets->size(); i++) {
    int cmp = label::lbs_compare(sets->at(i)->at()->lset, csm->lset);
    if (cmp < 0) {
      id.clear();
      id.push_back(i);
      csm->tsid = sets->at(i)->at()->tsid;
      csm->lset = sets->at(i)->at()->lset;
    } else if (cmp == 0) {
      id.push_back(i);
    }
  }
  for (int i : id) {
    csm->chunks.insert(csm->chunks.end(), sets->at(i)->at()->chunks.begin(),
                       sets->at(i)->at()->chunks.end());
    csm->intervals.insert(csm->intervals.end(),
                          sets->at(i)->at()->intervals.begin(),
                          sets->at(i)->at()->intervals.end());
  }

  // Sort the chunks by min_time.
  std::sort(csm->chunks.begin(), csm->chunks.end(),
            [](const std::shared_ptr<chunk::ChunkMeta> &lhs,
               const std::shared_ptr<chunk::ChunkMeta> &rhs) {
              return lhs->min_time < rhs->min_time;
            });
  return true;
}

bool MergedChunkSeriesSet::next() const {
  if (err_) return false;
  return next_helper();
}

const std::shared_ptr<querier::ChunkSeriesMeta> &MergedChunkSeriesSet::at()
    const {
  return csm;
}

bool MergedChunkSeriesSet::error() const {
  if (err_)
    return false;
  else
    return true;
}

error::Error MergedChunkSeriesSet::error_detail() const { return err_; }

MergedIDChunkSeriesSet::MergedIDChunkSeriesSet(
    const std::shared_ptr<querier::ChunkSeriesSets> &sets)
    : sets(sets), csm(new querier::ChunkSeriesMeta()) {
  // To move one step for each SeriesInterface.
  sets->next();
  if (sets->empty()) err_.wrap("Empty sets");
}

bool MergedIDChunkSeriesSet::next_helper() const {
  // To move one step for the former SeriesInterface s.
  sets->next(id);

  csm->clear();
  id.clear();

  if (sets->empty()) {
    err_.wrap("Empty sets");
    return false;
  }

  csm->tsid = sets->at(0)->at()->tsid;
  if (!sets->at(0)->at()->lset.empty()) csm->lset = sets->at(0)->at()->lset;
  id.push_back(0);
  for (int i = 1; i < sets->size(); i++) {
    if (sets->at(i)->at()->tsid < csm->tsid) {
      id.clear();
      id.push_back(i);
      csm->tsid = sets->at(i)->at()->tsid;
      if (!sets->at(i)->at()->lset.empty()) csm->lset = sets->at(i)->at()->lset;
    } else if (sets->at(i)->at()->tsid == csm->tsid) {
      id.push_back(i);
    }
  }
  for (int i : id) {
    csm->chunks.insert(csm->chunks.end(), sets->at(i)->at()->chunks.begin(),
                       sets->at(i)->at()->chunks.end());
    csm->intervals.insert(csm->intervals.end(),
                          sets->at(i)->at()->intervals.begin(),
                          sets->at(i)->at()->intervals.end());
  }

  // Sort the chunks by min_time.
  std::sort(csm->chunks.begin(), csm->chunks.end(),
            [](const std::shared_ptr<chunk::ChunkMeta> &lhs,
               const std::shared_ptr<chunk::ChunkMeta> &rhs) {
              return lhs->min_time < rhs->min_time;
            });
  return true;
}

bool MergedIDChunkSeriesSet::next() const {
  if (err_) return false;
  return next_helper();
}

const std::shared_ptr<querier::ChunkSeriesMeta> &MergedIDChunkSeriesSet::at()
    const {
  return csm;
}

bool MergedIDChunkSeriesSet::error() const {
  if (err_)
    return false;
  else
    return true;
}

error::Error MergedIDChunkSeriesSet::error_detail() const { return err_; }

}  // namespace compact
}  // namespace tsdb