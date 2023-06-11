#include "block/Block.hpp"

#include "base/Logging.hpp"
#include "chunk/ChunkReader.hpp"
#include "compact/CompactorInterface.hpp"
#include "index/IndexReader.hpp"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tombstone/TombstoneUtils.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace block {

Block::Block(uint8_t type_) : type_(type_) {}

Block::Block(const std::string &dir, uint8_t type_,
             const std::shared_ptr<IndexReaderInterface> &gindexr)
    : mutex_(),
      pending_readers(),
      closing(false),
      dir_(dir),
      global_indexr(gindexr),
      type_(type_) {
  std::pair<BlockMeta, bool> meta_pair = read_block_meta(dir);
  if (!meta_pair.second) {
    // LOG_ERROR << "Error reading meta.json";
    err_.set("error read meta.json");
    return;
  }
  meta_ = meta_pair.first;

  std::string chunks_dir = tsdbutil::filepath_join(dir, "chunks");
  std::string index_path = tsdbutil::filepath_join(dir, "index");
  if (type_ == static_cast<uint8_t>(OriginalBlock)) {
    chunkr = std::shared_ptr<ChunkReaderInterface>(
        new chunk::ChunkReader(chunks_dir));

    if (chunkr->error()) {
      // LOG_ERROR << "Error creating chunk reader";
      err_.set("error create chunk reader");
      return;
    }

    indexr = std::shared_ptr<IndexReaderInterface>(
        new index::IndexReader(index_path));
    if (indexr->error()) {
      // LOG_ERROR << "Error creating index reader";
      err_.set("error create index reader");
      return;
    }
  }

  int tombstone_size;
  std::tie(tr, tombstone_size) = tombstone::read_tombstones(dir);
  if (!tr) {
    // LOG_ERROR << "Error creating tombstonereader";
    err_.set("error tombstone reader");
    return;
  }

  // Sum up the size of chunk files, index file, and tombstone file
  meta_.stats.num_bytes = chunkr->size() + indexr->size() + tombstone_size;
  if (!write_block_meta(dir, meta_)) {
    // LOG_ERROR << "Error write_block_meta";
    err_.set("error write_block_meta");
  }
}

Block::Block(bool closing, const std::string &dir_, const BlockMeta &meta_,
             uint64_t symbol_table_size_,
             const std::shared_ptr<ChunkReaderInterface> &chunkr,
             const std::shared_ptr<IndexReaderInterface> &indexr,
             std::shared_ptr<tombstone::TombstoneReaderInterface> &tr,
             const error::Error &err_, uint8_t type_)
    : closing(closing),
      dir_(dir_),
      meta_(meta_),
      symbol_table_size_(symbol_table_size_),
      chunkr(chunkr),
      indexr(indexr),
      tr(tr),
      err_(err_),
      type_(type_) {}

// dir returns the directory of the block.
std::string Block::dir() { return dir_; }

bool Block::overlap_closed(int64_t mint, int64_t maxt) const {
  // The block itself is a half-open interval
  // [pb.meta.MinTime, pb.meta.MaxTime).
  return meta_.min_time <= maxt && mint < meta_.max_time;
}

// meta returns meta information about the block.
BlockMeta Block::meta() const { return meta_; }

int64_t Block::MaxTime() const { return meta_.max_time; }

int64_t Block::MinTime() const { return meta_.min_time; }

// size returns the number of bytes that the block takes up.
uint64_t Block::size() const { return meta_.stats.num_bytes; }

// get_symbol_table_size returns the Symbol Table Size in the index of this
// block.
uint64_t Block::get_symbol_table_size() {
  return index::symbol_table_size(indexr);
}

error::Error Block::error() const { return err_; }

bool Block::start_read() const {
  // Must protect the whole scope
  base::RWLockGuard mutex(mutex_, 0);
  if (closing) return false;
  p_add(1);
  return true;
}

// Wrapper for add() of pending_readers.
void Block::p_add(int i) const { pending_readers.add(i); }

// Wrapper for done() of pending_readers.
void Block::p_done() const { pending_readers.done(); }

// Wrapper for wait() of pending_readers.
void Block::p_wait() const { pending_readers.wait(); }

bool Block::set_compaction_failed() {
  meta_.compaction.failed = true;
  if (write_block_meta(dir_, meta_))
    return true;
  else
    return false;
}

bool Block::set_deletable() {
  meta_.compaction.deletable = true;
  if (write_block_meta(dir_, meta_))
    return true;
  else
    return false;
}

// label_names returns all the unique label names present in the Block in sorted
// order.
std::vector<std::string> Block::label_names() {
  if (!err_)
    return indexr->label_names();
  else
    return std::vector<std::string>();
}

std::pair<std::shared_ptr<IndexReaderInterface>, bool> Block::index() const {
  if (start_read()) {
    return std::make_pair(std::shared_ptr<IndexReaderInterface>(
                              new BlockIndexReader(indexr, this)),
                          true);
  } else {
    LOG_ERROR << "Cannot Block::start_read()";
    return std::make_pair(nullptr, false);
  }
}

std::shared_ptr<IndexReaderInterface> Block::global_index() const {
  return global_indexr;
}

std::pair<std::shared_ptr<ChunkReaderInterface>, bool> Block::chunks() const {
  if (start_read())
    return std::make_pair(std::shared_ptr<ChunkReaderInterface>(
                              new BlockChunkReader(chunkr, this)),
                          true);
  else {
    LOG_ERROR << "Cannot Block::start_read()";
    return std::make_pair(nullptr, false);
  }
}

std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
Block::tombstones() const {
  if (start_read())
    return std::make_pair(std::shared_ptr<tombstone::TombstoneReaderInterface>(
                              new BlockTombstoneReader(tr, this)),
                          true);
  else {
    LOG_ERROR << "Cannot Block::start_read()";
    return std::make_pair(nullptr, false);
  }
}

error::Error Block::del(
    int64_t mint, int64_t maxt,
    const std::deque<std::shared_ptr<label::MatcherInterface>> &matchers) {
  base::RWLockGuard mutex(mutex_, 1);
  if (closing) return error::Error("error closing");

  std::pair<std::unique_ptr<index::PostingsInterface>, bool> pp =
      querier::postings_for_matchers(indexr, matchers);
  if (!pp.second) return error::Error("error select series");

  // Choose only valid postings which have chunks in the time-range.
  std::shared_ptr<tombstone::TombstoneReaderInterface> stones(
      new tombstone::MemTombstones());

  label::Labels lset;
  std::deque<std::shared_ptr<chunk::ChunkMeta>> chks;

  std::unordered_map<uint64_t, tombstone::Interval> itvls;
  while (pp.first->next()) {
    lset.clear();
    chks.clear();
    if (!indexr->series(pp.first->at(), lset, chks))
      return error::Error("error read series from index reader");

    for (const std::shared_ptr<chunk::ChunkMeta> &chk : chks) {
      if (chk->overlap_closed(mint, maxt)) {
        // delete only until the current values and not beyond.
        std::pair<int64_t, int64_t> tp = tsdbutil::clamp_interval(
            mint, maxt, chks.front()->min_time, chks.back()->max_time);
        // LOG_DEBUG << chk->min_time << " " << chk->max_time;
        // LOG_DEBUG << pp.first->at() << " " << tp.first << " " << tp.second;
        if (type_ == static_cast<uint8_t>(OriginalBlock)) {
          stones->add_interval(pp.first->at(), {tp.first, tp.second});
        } else if (type_ == static_cast<uint8_t>(GroupBlock)) {
          if (itvls.find(chk->logical_group_ref) == itvls.end())
            itvls.insert({chk->logical_group_ref,
                          tombstone::Interval({tp.first, tp.second})});
        }
        break;
      }
    }
  }
  if (type_ == static_cast<uint8_t>(GroupBlock)) {
    for (auto &itvl : itvls) stones->add_interval(itvl.first, itvl.second);
  }

  tr->iter(static_cast<
           boost::function<void(uint64_t, const tombstone::Intervals &)>>(
      [&stones](uint64_t id, const tombstone::Intervals &ivs) -> void {
        for (const tombstone::Interval &iv : ivs) stones->add_interval(id, iv);
      }));
  tr = stones;
  meta_.stats.num_tombstones = tr->total();

  if (!tombstone::write_tombstones(dir_, tr))
    return error::Error("error write tombstones");

  if (write_block_meta(dir_, meta_))
    return error::Error();
  else
    return error::Error("error write_block_meta()");
}

// clean_tombstones will remove the tombstones and rewrite the block (only if
// there are any tombstones). If there was a rewrite, then it returns the ULID
// of the new block written, else nil.
//
// NOTE(Alec), use it carefully.
std::pair<ulid::ULID, error::Error> Block::clean_tombstones(
    const std::string &dest, void *compactor) {
  int num_tombstones = 0;

  tr->iter(static_cast<
           boost::function<void(uint64_t, const tombstone::Intervals &)>>(
      [&num_tombstones](uint64_t id, const tombstone::Intervals &ivs) {
        num_tombstones += ivs.size();
      }));
  if (num_tombstones == 0) return {ulid::ULID(), error::Error()};

  std::shared_ptr<BlockInterface> b(new Block(
      closing, dir_, meta_, symbol_table_size_, chunkr, indexr, tr, err_));
  std::shared_ptr<BlockMeta> m(new BlockMeta(meta_));
  std::pair<ulid::ULID, error::Error> ulid_pair =
      ((compact::CompactorInterface *)compactor)
          ->write(dest, b, MinTime(), MaxTime(), m);
  if (ulid_pair.second) return {ulid::ULID(), ulid_pair.second};
  return ulid_pair;
}

void Block::close() const {
  {
    base::RWLockGuard mutex(mutex_, 1);
    closing = true;
  }
  p_wait();
}

Block::~Block() {
  if (!closing) close();
}

BlockChunkReader::~BlockChunkReader() { b->p_done(); }

BlockIndexReader::~BlockIndexReader() { b->p_done(); }

BlockTombstoneReader::~BlockTombstoneReader() { b->p_done(); }

}  // namespace block
}  // namespace tsdb