#ifndef MERGEDCHUNKSERIESSET_H
#define MERGEDCHUNKSERIESSET_H

#include "querier/ChunkSeriesSetInterface.hpp"
#include "querier/QuerierUtils.hpp"

namespace tsdb {
namespace compact {

class MergedChunkSeriesSet : public querier::ChunkSeriesSetInterface {
 private:
  std::shared_ptr<querier::ChunkSeriesSets> sets;

  mutable std::shared_ptr<querier::ChunkSeriesMeta> csm;
  mutable std::deque<int> id;
  mutable error::Error err_;

 public:
  MergedChunkSeriesSet(const std::shared_ptr<querier::ChunkSeriesSets> &sets);

  bool next_helper() const;

  bool next() const override;

  const std::shared_ptr<querier::ChunkSeriesMeta> &at() const override;

  bool error() const override;

  error::Error error_detail() const override;
};

class MergedIDChunkSeriesSet : public querier::ChunkSeriesSetInterface {
 private:
  std::shared_ptr<querier::ChunkSeriesSets> sets;

  mutable std::shared_ptr<querier::ChunkSeriesMeta> csm;
  mutable std::deque<int> id;
  mutable error::Error err_;

 public:
  MergedIDChunkSeriesSet(const std::shared_ptr<querier::ChunkSeriesSets> &sets);

  bool next_helper() const;

  bool next() const override;

  const std::shared_ptr<querier::ChunkSeriesMeta> &at() const override;

  bool error() const override;

  error::Error error_detail() const override;
};

}  // namespace compact
}  // namespace tsdb

#endif