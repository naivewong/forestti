#ifndef BASECHUNKSERIESSET_H
#define BASECHUNKSERIESSET_H

#include "block/IndexReaderInterface.hpp"
#include "index/PostingsInterface.hpp"
#include "querier/ChunkSeriesSetInterface.hpp"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"

namespace tsdb {
namespace querier {

// BaseChunkSeriesSet loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be
// unset
//
// The chunk pointer in ChunkMeta is not set.
// NOTE(Alec), BaseChunkSeriesSet fine-grained filters the chunks using
// tombstone.
class BaseChunkSeriesSet : public ChunkSeriesSetInterface {
 private:
  std::unique_ptr<index::PostingsInterface> p_;
  std::shared_ptr<block::IndexReaderInterface> ir_;
  std::shared_ptr<tombstone::TombstoneReaderInterface> tr_;

  std::shared_ptr<ChunkSeriesMeta> cm_;
  mutable bool err_;
  const bool group_;
  const bool is_global_idx_;

 public:
  BaseChunkSeriesSet(
      const std::shared_ptr<block::IndexReaderInterface>& ir,
      std::unique_ptr<index::PostingsInterface>&& p, bool is_global_idx = false,
      const std::shared_ptr<tombstone::TombstoneReaderInterface>& tr =
          std::shared_ptr<tombstone::TombstoneReaderInterface>(
              new tombstone::MemTombstones()),
      const std::vector<std::shared_ptr<label::MatcherInterface>>& list = {},
      bool group_ = false);

  // next() always called before at().
  const std::shared_ptr<ChunkSeriesMeta>& at() const;

  bool next() const;

  bool error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif