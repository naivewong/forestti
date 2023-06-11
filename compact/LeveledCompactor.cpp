#include "compact/LeveledCompactor.hpp"

#include <limits>
#include <unordered_map>
#include <unordered_set>

#include "base/Logging.hpp"
#include "base/TimeStamp.hpp"
#include "block/Block.hpp"       // Block, Blocks
#include "chunk/ChunkUtils.hpp"  // merge_overlapping_chunks.
#include "chunk/ChunkWriter.hpp"
#include "chunk/DeleteIterator.hpp"
#include "chunk/XORChunk.hpp"
#include "compact/CompactionChunkSeriesSet.hpp"
#include "compact/MergedChunkSeriesSet.hpp"
#include "db/DBUtils.hpp"
#include "index/IndexWriter.hpp"
#include "index/MemPostings.hpp"
#include "querier/ChunkSeriesSetInterface.hpp"
#include "tombstone/TombstoneUtils.hpp"

namespace tsdb {
namespace compact {

LeveledCompactor::LeveledCompactor(
    const std::deque<int64_t> &ranges,
    const std::shared_ptr<base::Channel<char>> &cancel)
    : ranges(ranges), cancel(cancel) {
  if (ranges.empty()) err_.set("at least one range must be provided");
}
LeveledCompactor::LeveledCompactor(
    const std::vector<int64_t> &ranges,
    const std::shared_ptr<base::Channel<char>> &cancel)
    : ranges(ranges.begin(), ranges.end()), cancel(cancel) {
  if (this->ranges.empty()) err_.set("at least one range must be provided");
}
LeveledCompactor::LeveledCompactor(
    const std::initializer_list<int64_t> &ranges,
    const std::shared_ptr<base::Channel<char>> &cancel)
    : ranges(ranges.begin(), ranges.end()), cancel(cancel) {
  if (this->ranges.empty()) err_.set("at least one range must be provided");
}

std::pair<std::deque<std::string>, error::Error> LeveledCompactor::plan_helper(
    const std::shared_ptr<block::DirMetas> &dms) {
  dms->sort_by_min_time();

  std::deque<std::string> res = overlapping_dirs(dms);
  if (!res.empty()) return {res, error::Error()};

  // No overlapping blocks, do compaction the usual way.
  // We do not include a recently created block with max(minTime), so the block
  // which was just created from WAL. This gives users a window of a full block
  // size to piece-wise backup new data without having to care about data
  // overlap.
  dms->pop_back();

  std::shared_ptr<block::DirMetas> selected_dms = select_dirs(dms);
  if (selected_dms) {
    for (int i = 0; i < selected_dms->size(); i++)
      res.push_back(selected_dms->at(i).dir);
  }
  if (!res.empty()) return {res, error::Error()};
  // Compact any blocks with big enough time range that have >5% tombstones.
  for (int i = dms->size() - 1; i >= 0; i--) {
    if (dms->at(i).meta->max_time - dms->at(i).meta->min_time <
        ranges[ranges.size() / 2])
      break;
    if ((double)(dms->at(i).meta->stats.num_tombstones) /
            (double)(dms->at(i).meta->stats.num_series) >
        0.05) {
      res.push_back(dms->at(i).dir);
      return {res, error::Error()};
    }
  }
  return {res, error::Error()};
}

std::pair<std::deque<std::string>, error::Error> LeveledCompactor::plan(
    const std::string &dir) {
  std::deque<std::string> dirs = db::block_dirs(dir);
  if (dirs.empty()) return {dirs, error::Error()};

  std::shared_ptr<block::DirMetas> dms(new block::DirMetas());
  for (const std::string &s : dirs) {
    std::pair<block::BlockMeta, bool> p = block::read_block_meta(s);
    if (!p.second)
      return {std::deque<std::string>(),
              error::Error("Error reading block meta")};
    dms->push_back(block::DirMeta(
        s, std::shared_ptr<block::BlockMeta>(new block::BlockMeta(p.first))));
  }

  return plan_helper(dms);
}

// All the overlapping dirs will be collected.
std::deque<std::string> LeveledCompactor::overlapping_dirs(
    const std::shared_ptr<block::DirMetas> &dms) {
  if (dms->size() < 2) return std::deque<std::string>();

  std::deque<std::string> overlappings;
  int64_t max = dms->at(0).meta->max_time;
  for (int i = 1; i < dms->size(); i++) {
    if (dms->at(i).meta->min_time < max) {
      if (overlappings.empty()) {
        overlappings.push_back(dms->at(i - 1).dir);
      }
      overlappings.push_back(dms->at(i).dir);
    } else if (!overlappings.empty()) {
      break;
    }
    if (dms->at(i).meta->max_time > max) max = dms->at(i).meta->max_time;
  }
  return overlappings;
}

// selectDirs returns the dir metas that should be compacted into a single new
// block. If only a single block range is configured, the result is always nil.
std::shared_ptr<block::DirMetas> LeveledCompactor::select_dirs(
    const std::shared_ptr<block::DirMetas> &dms) {
  if (ranges.size() < 2 || dms->empty())
    return std::shared_ptr<block::DirMetas>();

  int64_t high_time = dms->back().meta->min_time;

  for (int i = 1; i < ranges.size(); i++) {
    std::deque<std::shared_ptr<block::DirMetas>> parts =
        split_by_range(dms, ranges[i]);
    if (parts.empty()) continue;

    for (auto const &part : parts) {
      // Do not select the range if it has a block whose compaction failed.
      // TODO(Alec)
      bool jump = false;
      for (int j = 0; j < part->size(); j++) {
        if (part->at(j).meta->compaction.failed) {
          jump = true;
          break;
        }
      }
      if (jump) continue;

      int64_t min_time = part->front().meta->min_time;
      int64_t max_time = part->back().meta->max_time;
      // Pick the range of blocks if it spans the full range (potentially with
      // gaps) or is before the most recent block. This ensures we don't compact
      // blocks prematurely when another one of the same size still fits in the
      // range.
      //
      // Prerequisite: [1]. no overlapping blocks [2]. sorted by min_time
      // 1. number of blocks must > 1.
      // 2. the most recent block(the popped one in plan()) may reside in range
      // of last part.
      if ((max_time - min_time == ranges[i] || max_time <= high_time) &&
          part->size() > 1)
        return part;
    }
  }
  return std::shared_ptr<block::DirMetas>();
}

// splitByRange splits the directories by the time range. The range sequence
// starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split
// range tr is 30 it returns [0-10, 10-20], [50-60], [90-100].
//
// Suppose dms are sorted by meta.min_time.
//
// TODO(Alec), multiple seperate overlaps.
std::deque<std::shared_ptr<block::DirMetas>> LeveledCompactor::split_by_range(
    const std::shared_ptr<block::DirMetas> &dms, int64_t range) {
  std::deque<std::shared_ptr<block::DirMetas>> parts;

  int i = 0;
  while (i < dms->size()) {
    std::shared_ptr<block::DirMetas> group(new block::DirMetas());
    int64_t t0;

    // Compute start of aligned time range of size tr closest to the current
    // block's start.
    int64_t mt = dms->at(i).meta->min_time;
    if (mt >= 0)
      t0 = range * (mt / range);
    else
      t0 = range * ((mt - range + 1) / range);

    // Skip blocks that don't fall into the range. This can happen via
    // mis-alignment or by being the multiple of the intended range.
    if (dms->at(i).meta->max_time > t0 + range) {
      ++i;
      continue;
    }

    // Add all dirs to the current group that are within [t0, t0+range].
    for (; i < dms->size(); i++) {
      if (dms->at(i).meta->max_time > t0 + range) break;
      // LOG_INFO << dms->at(i).dir;
      group->push_back(dms->at(i));
    }

    if (!group->empty()) {
      // for(int i = 0; i < group->size(); i ++)
      //     LOG_INFO << group->at(i).dir;
      parts.push_back(group);
    }
  }
  return parts;
}

// Compact creates a new block in the compactor's directory from the blocks in
// the provided directories.
std::pair<ulid::ULID, error::Error> LeveledCompactor::compact(
    const std::string &dest, const std::deque<std::string> &dirs,
    const std::shared_ptr<block::Blocks> &open) {
  if (dirs.empty())
    return {ulid::ULID(), error::Error("compact: no dir passed in")};

  base::TimeStamp start = base::TimeStamp::now();

  std::shared_ptr<block::Blocks> blocks(new block::Blocks());
  std::shared_ptr<block::BlockMetas> metas(new block::BlockMetas());
  std::deque<std::string> ulids;
  for (auto const &d : dirs) {
    std::pair<block::BlockMeta, bool> meta_pair = block::read_block_meta(d);
    if (!meta_pair.second)
      return {ulid::ULID(), error::Error("Error reading block meta: " + d)};

    // Use already open blocks if we can, to avoid
    // having the index data in memory twice.
    std::shared_ptr<block::BlockInterface> b;
    if (open) {
      for (int i = 0; i < open->size(); i++) {
        if (meta_pair.first.ulid_ == open->at(i)->meta().ulid_) {
          b = open->at(i);
          break;
        }
      }
    }
    if (!b) {
      b = std::shared_ptr<block::BlockInterface>(new block::Block(d));
      if (b->error())
        return {ulid::ULID(), error::Error("Error opening block: " + d)};
    }
    blocks->push_back(b);
    metas->push_back(meta_pair.first);
    ulids.push_back(ulid::Marshal(meta_pair.first.ulid_));
  }

  ulid::ULID ulid = ulid::CreateNowRand();

  block::BlockMeta compacted_meta = compact_block_metas(ulid, metas);
  error::Error err = write_helper(dest, &compacted_meta, blocks);
  if (!err) {
    std::string ulids_string("[");
    for (const std::string &uid : ulids) ulids_string += uid + ", ";
    ulids_string += "]";

    if (compacted_meta.stats.num_samples == 0) {
      for (int i = 0; i < blocks->size(); ++i) {
        if (!blocks->at(i)->set_deletable())
          LOG_ERROR << "msg=\"Failed to write 'deletable' to meta file after "
                       "compaction\" "
                    << "ulid=\"" << ulid::Marshal(blocks->at(i)->meta().ulid_)
                    << "\"";
      }
      LOG_INFO << "msg=\"compact blocks resulted in empty block\" "
               << "count=" << blocks->size() << " sources=" << ulids_string
               << " duration="
               << base::timeDifference(base::TimeStamp::now(), start);
      return {ulid::ULID(), error::Error()};
    } else {
      LOG_INFO << "msg=\"compact blocks\" "
               << "count=" << blocks->size()
               << " min_time=" << compacted_meta.min_time
               << " max_time=" << compacted_meta.max_time << " ulid=\""
               << ulid::Marshal(compacted_meta.ulid_) << "\" "
               << "sources=" << ulids_string << " duration="
               << base::timeDifference(base::TimeStamp::now(), start);
      return {ulid, error::Error()};
    }
  }

  // set compaction failure if the err isn't cancel signal.
  if (err != "cancel") {
    for (int i = 0; i < blocks->size(); ++i) {
      if (blocks->at(i)->set_compaction_failed())
        err.wrap("setting compaction failed for block: " +
                 blocks->at(i)->dir());
    }
  }

  return {ulid, err};
}

block::BlockMeta LeveledCompactor::compact_block_metas(
    const ulid::ULID &ulid, const std::shared_ptr<block::BlockMetas> &bms) {
  block::BlockMeta bm;
  bm.ulid_ = ulid;
  bm.min_time = bms->front().min_time;

  std::unordered_set<ulid::ULID, ulid::ULIDHasher> sources;
  int64_t max_time = std::numeric_limits<int64_t>::min();

  for (int i = 0; i < bms->size(); i++) {
    if (bms->at(i).max_time > max_time) max_time = bms->at(i).max_time;
    if (bms->at(i).compaction.level > bm.compaction.level)
      bm.compaction.level = bms->at(i).compaction.level;
    for (auto const &ulid_ : bms->at(i).compaction.sources)
      sources.insert(ulid_);
    bm.compaction.parents.emplace_back(bms->at(i).ulid_, bms->at(i).min_time,
                                       bms->at(i).max_time);
  }
  ++bm.compaction.level;

  bm.compaction.sources.insert(bm.compaction.sources.end(), sources.begin(),
                               sources.end());

  std::sort(bm.compaction.sources.begin(), bm.compaction.sources.end(),
            [](const ulid::ULID &lhs, const ulid::ULID &rhs) {
              return ulid::CompareULIDs(lhs, rhs) < 0;
            });

  bm.max_time = max_time;
  return bm;
}

// populate_blocks fills the index and chunk writers with new data gathered as
// the union of the provided blocks. It returns meta information for the new
// block. It expects sorted BLOCKS input by mint.
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
error::Error LeveledCompactor::populate_blocks(
    const std::shared_ptr<block::Blocks> &blocks, block::BlockMeta *bm,
    const std::shared_ptr<block::IndexWriterInterface> &indexw,
    const std::shared_ptr<block::ChunkWriterInterface> &chunkw) {
  // LOG_DEBUG << bm->min_time << " " << bm->max_time;
  if (blocks->empty()) {
    return error::Error("cannot populate block from no readers");
  }

  std::shared_ptr<querier::ChunkSeriesSetInterface> set;
  std::unordered_set<std::string> symbols;
  bool overlapping = false;
  int err;

  // Create ChunkSeriesSets.
  std::shared_ptr<querier::ChunkSeriesSets> sets(
      new querier::ChunkSeriesSets());

  int64_t max_time = blocks->front()->MaxTime();
  for (int i = 0; i < blocks->size(); i++) {
    // Check if receiving cancel signal.
    if (!cancel->empty()) return error::Error("cancel");

    if (!overlapping) {
      if (i > 0 && blocks->at(i)->MinTime() < max_time) {
        overlapping = true;
        LOG_WARN << "found overlapping blocks during populate_blocks(), ulid: "
                 << ulid::Marshal(bm->ulid_);
      }
      if (blocks->at(i)->MaxTime() > max_time)
        max_time = blocks->at(i)->MaxTime();
    }

    std::pair<std::shared_ptr<block::IndexReaderInterface>, bool> index_pair =
        blocks->at(i)->index();
    if (!index_pair.second)
      return error::Error("Error open index reader for block " +
                          ulid::Marshal(blocks->at(i)->meta().ulid_));

    std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool> chunks_pair =
        blocks->at(i)->chunks();
    if (!chunks_pair.second)
      return error::Error("Error open chunks reader for block " +
                          ulid::Marshal(blocks->at(i)->meta().ulid_));

    std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
        tombstones_pair = blocks->at(i)->tombstones();
    if (!tombstones_pair.second)
      return error::Error("Error open tombstone reader for block " +
                          ulid::Marshal(blocks->at(i)->meta().ulid_));

    const std::deque<std::string> &temp_symbols =
        index_pair.first->symbols_deque();

    symbols.insert(temp_symbols.begin(), temp_symbols.end());

    std::pair<std::unique_ptr<index::PostingsInterface>, bool>
        all_postings_pair = index_pair.first->postings(
            label::ALL_POSTINGS_KEYS.label, label::ALL_POSTINGS_KEYS.value);
    if (!all_postings_pair.second)
      return error::Error("Error get postings of ALL_POSTINGS_KEYS " +
                          ulid::Marshal(blocks->at(i)->meta().ulid_));

    // Append block to ChunkSeriesSets.
    sets->push_back(std::shared_ptr<querier::ChunkSeriesSetInterface>(
        new CompactionChunkSeriesSet(
            index_pair.first, chunks_pair.first, tombstones_pair.first,
            std::move(index_pair.first->sorted_id_postings(
                std::move(all_postings_pair.first))),
            true)));
  }

  // Create MergedChunkSeriesSet.
  MergedIDChunkSeriesSet mcss(sets);

  while (mcss.next()) {
    // Check if receiving cancel signal.
    if (!cancel->empty()) return error::Error("cancel");

    std::shared_ptr<querier::ChunkSeriesMeta> csm = mcss.at();
    if (overlapping) {
      // If blocks are overlapping, it is possible to have unsorted chunks.
      std::sort(csm->chunks.begin(), csm->chunks.end(),
                [](const std::shared_ptr<chunk::ChunkMeta> &lhs,
                   const std::shared_ptr<chunk::ChunkMeta> &rhs) {
                  return lhs->min_time < rhs->min_time;
                });
    }
    // Skip the series with all deleted chunks.
    if (csm->chunks.empty()) {
      // LOG_DEBUG << "csm->chunks.empty()";
      continue;
    }

    for (int i = 0; i < csm->chunks.size(); ++i) {
      // LOG_DEBUG << csm->chunks[i]->min_time << " " <<
      // csm->chunks[i]->max_time;
      if (csm->chunks[i]->min_time < bm->min_time ||
          csm->chunks[i]->max_time > bm->max_time)
        return error::Error(
            "found chunk with minTime: " +
            std::to_string(csm->chunks[i]->min_time) +
            " maxTime: " + std::to_string(csm->chunks[i]->max_time) +
            " outside of compacted minTime: " + std::to_string(bm->min_time) +
            " maxTime: " + std::to_string(bm->max_time));
      // Re-encode the chunk to not have deleted values.
      if (!csm->intervals.empty()) {
        if (!csm->chunks[i]->overlap_closed(csm->intervals.front().min_time,
                                            csm->intervals.back().max_time))
          continue;

        // TODO(alec), different types of chunk.
        std::shared_ptr<chunk::ChunkInterface> new_chunk =
            std::make_shared<chunk::XORChunk>();
        std::unique_ptr<chunk::ChunkAppenderInterface> app;
        try {
          app = new_chunk->appender();
        } catch (const base::TSDBException &e) {
          return error::Error(e.what());
        }

        chunk::DeleteIterator it(std::move(csm->chunks[i]->chunk->iterator()),
                                 csm->intervals.cbegin(),
                                 csm->intervals.cend());
        while (it.next()) {
          std::pair<int64_t, double> p = it.at();
          app->append(p.first, p.second);
        }
        csm->chunks[i]->chunk = new_chunk;
      }
    }

    if (overlapping) {
      std::pair<std::deque<std::shared_ptr<chunk::ChunkMeta>>, error::Error>
          merged_chunks = chunk::merge_overlapping_chunks(csm->chunks);
      if (merged_chunks.second)
        return error::wrap(merged_chunks.second, "merge overlapping chunks");
      csm->chunks = merged_chunks.first;
    }

    // write_chunks will update ref in ChunkMeta.
    chunkw->write_chunks(csm->chunks);

    // STEP 2 in index writer.
    // Monotonically increasing ID.
    if ((err = indexw->add_series(csm->tsid, csm->lset, csm->chunks)) !=
        index::SUCCEED)
      return error::wrap(error::Error(index::error_string(err)), "add_series");

    bm->stats.num_chunks += csm->chunks.size();
    ++bm->stats.num_series;
    for (auto const &chk : csm->chunks)
      bm->stats.num_samples += chk->chunk->num_samples();
  }
  if (mcss.error())
    return error::wrap(mcss.error_detail(), "iterate MergedChunkSeriesSet");

  return error::Error();
}

// Given a new block meta, a series of Block.
// Actual writting.
//
// TODO(Alec), add metrics tracking the duration and writting information.
error::Error LeveledCompactor::write_helper(
    const std::string &dest, block::BlockMeta *bm,
    const std::shared_ptr<block::Blocks> &blocks) {
  // Clean and create dir dest/ulid_.tmp
  boost::filesystem::path dir =
      boost::filesystem::path(dest) /
      boost::filesystem::path(ulid::Marshal(bm->ulid_));
  boost::filesystem::path tmp(dir.string() + ".tmp");
  boost::filesystem::remove_all(tmp);
  boost::filesystem::create_directories(tmp);

  {
    error::Error err;
    // Make sure chunk and index files are closed after populate_blocks().
    {
      // Populate chunk and index files into temporary directory with data of
      // all blocks.
      std::shared_ptr<block::ChunkWriterInterface> chunkw;
      chunkw = std::make_shared<chunk::ChunkWriter>(tmp.string() + "/chunks");

      std::shared_ptr<block::IndexWriterInterface> indexw;
      indexw = std::make_shared<index::IndexWriter>(tmp.string() + "/index");
      err = populate_blocks(blocks, bm, indexw, chunkw);
    }
    if (err) {
      boost::filesystem::remove_all(tmp);
      return error::wrap(err, "populate_blocks");
    }

    // Check if receiving cancel signal.
    if (!cancel->empty()) {
      boost::filesystem::remove_all(tmp);
      return error::Error("cancel");
    }

    // Populated block is empty, so exit early.
    if (bm->stats.num_samples == 0) {
      try {
        boost::filesystem::remove_all(tmp);
      } catch (const boost::filesystem::filesystem_error &e) {
      }
      return error::Error();
    }

    if (!block::write_block_meta(tmp.string(), *bm)) {
      boost::filesystem::remove_all(tmp);
      return error::Error("write_helper: write_block_meta");
    }
    tombstone::write_tombstones(tmp.string(), nullptr);
  }
  try {
    boost::filesystem::rename(tmp, dir);
  } catch (const boost::filesystem::filesystem_error &e) {
    boost::filesystem::remove_all(tmp);
    return error::Error(e.what());
  }
  return error::Error();
}

// Given a dest dir, a Block.
//
// 1. create BlockMeta.
// 2. call write_helper.
// 3. log the meta information.
std::pair<ulid::ULID, error::Error> LeveledCompactor::write(
    const std::string &dest, const std::shared_ptr<block::BlockInterface> &b,
    int64_t min_time, int64_t max_time,
    const std::shared_ptr<block::BlockMeta> &parent) {
  base::TimeStamp start = base::TimeStamp::now();
  usleep(1);
  ulid::ULID ulid = ulid::CreateNowRand();

  block::BlockMeta bm;
  bm.ulid_ = ulid;
  bm.min_time = min_time;
  bm.max_time = max_time;
  bm.compaction.level = 1;
  bm.compaction.sources.push_back(ulid);
  if (parent) {
    bm.compaction.parents.emplace_back(parent->ulid_, parent->min_time,
                                       parent->max_time);
  }

  error::Error err = write_helper(
      dest, &bm, std::shared_ptr<block::Blocks>(new block::Blocks({b})));
  if (err) return {ulid, error::wrap(err, "write_helper")};

  if (bm.stats.num_samples == 0) {
    LOG_DEBUG << "bm.stats.num_samples == 0";
    return {ulid::ULID(), error::Error()};
  }

  LOG_INFO << "msg=\"write blocks\" "
           << "mint=" << bm.min_time << " maxt=" << bm.max_time << " ulid=\""
           << ulid::Marshal(bm.ulid_) << "\" duration="
           << base::timeDifference(base::TimeStamp::now(), start)
           << "s num_series=" << bm.stats.num_series;
  ;

  return {ulid, error::Error()};
}

}  // namespace compact
}  // namespace tsdb