#ifndef LEVELEDCOMPACTOR_H
#define LEVELEDCOMPACTOR_H

#include <vector>

#include "base/Channel.hpp"
#include "block/BlockUtils.hpp"
#include "block/ChunkWriterInterface.hpp"
#include "block/IndexWriterInterface.hpp"
#include "compact/CompactorInterface.hpp"
#include "leveldb/db.h"

namespace tsdb {
namespace compact {

// NOTE(Alec): we may only use it to compact Head.
class LeveledCompactor : public CompactorInterface {
 private:
  std::deque<int64_t> ranges;
  std::shared_ptr<base::Channel<char>> cancel;
  error::Error err_;

 public:
  std::deque<std::string> overlapping_dirs(
      const std::shared_ptr<block::DirMetas> &dms);

  // selectDirs returns the dir metas that should be compacted into a single new
  // block. If only a single block range is configured, the result is always
  // nil.
  std::shared_ptr<block::DirMetas> select_dirs(
      const std::shared_ptr<block::DirMetas> &dms);

  // splitByRange splits the directories by the time range. The range sequence
  // starts at 0.
  //
  // For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split
  // range tr is 30 it returns [0-10, 10-20], [50-60], [90-100].
  //
  // Suppose dms are sorted by meta.min_time.
  //
  // TODO(Alec), multiple seperate ovelaps.
  std::deque<std::shared_ptr<block::DirMetas>> split_by_range(
      const std::shared_ptr<block::DirMetas> &dms, int64_t range);

  block::BlockMeta compact_block_metas(
      const ulid::ULID &ulid, const std::shared_ptr<block::BlockMetas> &bms);

  // Actual writting.
  // TODO(Alec), add metrics tracking the duration and writting information.
  error::Error write_helper(const std::string &dest, block::BlockMeta *bm,
                            const std::shared_ptr<block::Blocks> &blocks);

  // populate_blocks fills the index and chunk writers with new data gathered as
  // the union of the provided blocks. It returns meta information for the new
  // block. It expects sorted blocks input by mint.
  //
  // 1. Create ChunkSeriesSets.
  // -- 1.1. Check overlapping of blocks.
  // -- 1.2. Create CompactionChunkSeriesSet.
  // ------- 1.2.1. std::shared_ptr<block::IndexReaderInterface>
  // ------- 1.2.2. std::shared_ptr<block::ChunkReaderInterface>
  // ------- 1.2.3. std::shared_ptr<tombstone::TombstoneReaderInterface>
  // ------- 1.2.4. std::unique_ptr<index::PostingsInterface> which contains
  // label::ALL_POSTINGS_KEYS
  // 2. Create MergedChunkSeriesSet.
  // -- 2.1. Sort csm->chunks by time of 'overlapping' detected before.
  // -- 2.2. For each csm->chunks
  // ------- 2.2.1. Rewrite chunk.
  // -- 2.3. Merge overlapping csm->chunks.
  // -- 2.4. chunkw->write_chunks(csm->chunks) and add_series.
  // 3. write_label_index and write_postings.
  //
  // TODO(Alec), add metrics tracking the number of populated blocks.
  error::Error populate_blocks(
      const std::shared_ptr<block::Blocks> &blocks, block::BlockMeta *bm,
      const std::shared_ptr<block::IndexWriterInterface> &indexw,
      const std::shared_ptr<block::ChunkWriterInterface> &chunkw);

  std::pair<std::deque<std::string>, error::Error> plan_helper(
      const std::shared_ptr<block::DirMetas> &dms);

  LeveledCompactor() = default;
  LeveledCompactor(const std::deque<int64_t> &ranges,
                   const std::shared_ptr<base::Channel<char>> &cancel);
  LeveledCompactor(const std::vector<int64_t> &ranges,
                   const std::shared_ptr<base::Channel<char>> &cancel);
  LeveledCompactor(const std::initializer_list<int64_t> &ranges,
                   const std::shared_ptr<base::Channel<char>> &cancel);

  std::pair<std::deque<std::string>, error::Error> plan(const std::string &dir);

  std::pair<ulid::ULID, error::Error> write(
      const std::string &dest, const std::shared_ptr<block::BlockInterface> &b,
      int64_t min_time, int64_t max_time,
      const std::shared_ptr<block::BlockMeta> &parent);

  // Compact creates a new block in the compactor's directory from the blocks in
  // the provided directories.
  std::pair<ulid::ULID, error::Error> compact(
      const std::string &dest, const std::deque<std::string> &dirs,
      const std::shared_ptr<block::Blocks> &open);

  error::Error error() const { return err_; }
};

}  // namespace compact
}  // namespace tsdb

#endif