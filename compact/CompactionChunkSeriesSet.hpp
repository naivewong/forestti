#ifndef COMPACTIONCHUNKSERIESSET_H
#define COMPACTIONCHUNKSERIESSET_H

#include "block/ChunkReaderInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "index/PostingsInterface.hpp"
#include "querier/ChunkSeriesMeta.hpp"
#include "querier/ChunkSeriesSetInterface.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"

namespace tsdb {
namespace compact {

// Only those chunks completely inside the Interval will be filtered during
// iteration.
class CompactionChunkSeriesSet : public querier::ChunkSeriesSetInterface {
 private:
  std::unique_ptr<index::PostingsInterface> p;
  std::shared_ptr<block::IndexReaderInterface> ir;
  std::shared_ptr<block::ChunkReaderInterface> cr;
  std::shared_ptr<tombstone::TombstoneReaderInterface> tr;

  mutable std::shared_ptr<querier::ChunkSeriesMeta> csm;
  mutable error::Error err_;

  bool is_global_idx_;

 public:
  CompactionChunkSeriesSet(
      const std::shared_ptr<block::IndexReaderInterface> &ir,
      const std::shared_ptr<block::ChunkReaderInterface> &cr,
      const std::shared_ptr<tombstone::TombstoneReaderInterface> &tr,
      std::unique_ptr<index::PostingsInterface> &&p,
      bool is_global_idx = false);

  bool next() const;

  const std::shared_ptr<querier::ChunkSeriesMeta> &at() const;

  bool error() const;

  error::Error error_detail() const;
};

}  // namespace compact
}  // namespace tsdb

#endif