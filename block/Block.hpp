#ifndef BLOCK_H
#define BLOCK_H

#include "base/Error.hpp"
#include "base/WaitGroup.hpp"
#include "block/BlockInterface.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace block {

enum BlockType { OriginalBlock, GroupBlock };

class Block;

class BlockChunkReader : public ChunkReaderInterface {
 private:
  std::shared_ptr<ChunkReaderInterface> chunkr;
  const Block *b;

 public:
  BlockChunkReader(const std::shared_ptr<ChunkReaderInterface> &chunkr,
                   const Block *b)
      : chunkr(chunkr), b(b) {}

  virtual std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(
      uint64_t ref) override {
    return chunkr->chunk(ref);
  }
  virtual std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(
      uint64_t ref, int64_t starting_time) override {
    return chunkr->chunk(ref, starting_time);
  }

  virtual bool error() override { return chunkr->error(); }

  virtual uint64_t size() override { return chunkr->size(); }

  ~BlockChunkReader();
};

class BlockIndexReader : public IndexReaderInterface {
 private:
  std::shared_ptr<IndexReaderInterface> indexr;
  const Block *b;

 public:
  BlockIndexReader(const std::shared_ptr<IndexReaderInterface> &indexr,
                   const Block *b)
      : indexr(indexr), b(b) {}

  virtual std::set<std::string> symbols() override { return indexr->symbols(); }

  virtual const std::deque<std::string> &symbols_deque() const override {
    return indexr->symbols_deque();
  }

  // Return empty SerializedTuples when error
  virtual std::vector<std::string> label_values(
      const std::initializer_list<std::string> &names) override {
    return indexr->label_values(names);
  }
  virtual std::vector<std::string> label_values(
      const std::string &name) override {
    return indexr->label_values(name);
  }

  virtual std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string &name, const std::string &value) override {
    return indexr->postings(name, value);
  }

  virtual bool series(
      uint64_t ref, label::Labels &lset,
      std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) override {
    return indexr->series(ref, lset, chunks);
  }
  virtual bool series(
      uint64_t ref,
      std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) override {
    return indexr->series(ref, chunks);
  }
  virtual bool series(uint64_t ref, label::Labels &lset,
                      const std::shared_ptr<chunk::ChunkMeta> &chunk) override {
    return indexr->series(ref, lset, chunk);
  }

  virtual std::vector<std::string> label_names() override {
    return indexr->label_names();
  }

  virtual bool error() override { return indexr->error(); }

  virtual uint64_t size() override { return indexr->size(); }

  virtual std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface> &&p) override {
    return std::move(indexr->sorted_postings(std::move(p)));
  }

  virtual std::unique_ptr<index::PostingsInterface> sorted_id_postings(
      std::unique_ptr<index::PostingsInterface> &&p) override {
    return std::move(indexr->sorted_id_postings(std::move(p)));
  }

  ~BlockIndexReader();
};

class BlockTombstoneReader : public tombstone::TombstoneReaderInterface {
 private:
  std::shared_ptr<tombstone::TombstoneReaderInterface> tombstones;
  const Block *b;

 public:
  BlockTombstoneReader(
      const std::shared_ptr<tombstone::TombstoneReaderInterface> &tombstones,
      const Block *b)
      : tombstones(tombstones), b(b) {}

  // NOTICE, may throw std::out_of_range.
  virtual const tombstone::Intervals &get(uint64_t ref) const override {
    return tombstones->get(ref);
  }

  virtual void iter(const IterFunc &f) const override { tombstones->iter(f); }
  virtual error::Error iter(const ErrIterFunc &f) const override {
    return tombstones->iter(f);
  }

  virtual uint64_t total() const override { return tombstones->total(); }

  virtual void add_interval(uint64_t ref,
                            const tombstone::Interval &itvl) override {
    tombstones->add_interval(ref, itvl);
  }

  ~BlockTombstoneReader();
};

class Block : public BlockInterface {
 private:
  mutable base::RWMutexLock mutex_;
  mutable base::WaitGroup pending_readers;
  mutable bool closing;
  std::string dir_;
  BlockMeta meta_;

  uint64_t symbol_table_size_;

  std::shared_ptr<ChunkReaderInterface> chunkr;
  std::shared_ptr<IndexReaderInterface> indexr;
  std::shared_ptr<IndexReaderInterface> global_indexr;
  std::shared_ptr<tombstone::TombstoneReaderInterface> tr;

  error::Error err_;

  uint8_t type_;

  Block(const Block &) = delete;             // non construction-copyable
  Block &operator=(const Block &) = delete;  // non copyable

 public:
  Block(uint8_t type_ = static_cast<uint8_t>(OriginalBlock));
  Block(const std::string &dir,
        uint8_t type_ = static_cast<uint8_t>(OriginalBlock),
        const std::shared_ptr<IndexReaderInterface> &gindexr = nullptr);
  Block(bool closing, const std::string &dir_, const BlockMeta &meta_,
        uint64_t symbol_table_size_,
        const std::shared_ptr<ChunkReaderInterface> &chunkr,
        const std::shared_ptr<IndexReaderInterface> &indexr,
        std::shared_ptr<tombstone::TombstoneReaderInterface> &tr,
        const error::Error &err_,
        uint8_t type_ = static_cast<uint8_t>(OriginalBlock));

  bool is_closing() { return closing; }
  uint8_t type() { return type_; }
  // dir returns the directory of the block.
  std::string dir();

  bool overlap_closed(int64_t mint, int64_t maxt) const;

  // meta returns meta information about the block.
  BlockMeta meta() const;

  int64_t MaxTime() const;

  int64_t MinTime() const;

  // size returns the number of bytes that the block takes up.
  uint64_t size() const;

  // get_symbol_table_size returns the Symbol Table Size in the index of this
  // block.
  uint64_t get_symbol_table_size();

  error::Error error() const;

  bool start_read() const;

  // Wrapper for add() of pending_readers.
  void p_add(int i) const;

  // Wrapper for done() of pending_readers.
  void p_done() const;

  // Wrapper for wait() of pending_readers.
  void p_wait() const;

  bool set_compaction_failed();

  bool set_deletable();

  // label_names returns all the unique label names present in the Block in
  // sorted order.
  std::vector<std::string> label_names();

  virtual std::pair<std::shared_ptr<IndexReaderInterface>, bool> index()
      const override;

  virtual std::shared_ptr<IndexReaderInterface> global_index() const override;

  virtual std::pair<std::shared_ptr<ChunkReaderInterface>, bool> chunks()
      const override;

  virtual std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
  tombstones() const override;

  error::Error del(
      int64_t mint, int64_t maxt,
      const std::deque<std::shared_ptr<label::MatcherInterface>> &matchers);

  // clean_tombstones will remove the tombstones and rewrite the block (only if
  // there are any tombstones). If there was a rewrite, then it returns the ULID
  // of the new block written, else nil.
  //
  // NOTE(Alec), use it carefully.
  std::pair<ulid::ULID, error::Error> clean_tombstones(const std::string &dest,
                                                       void *compactor);

  void close() const;

  ~Block();
};

}  // namespace block
}  // namespace tsdb

#endif