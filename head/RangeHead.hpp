#ifndef RANGEHEAD_H
#define RANGEHEAD_H

#include "block/BlockInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadChunkReader.hpp"
#include "head/HeadIndexReader.hpp"
#include "tombstone/MemTombstones.hpp"

namespace tsdb {
namespace head {

class RangeHead : public block::BlockInterface {
 private:
  std::shared_ptr<Head> head;
  int64_t min_time;
  int64_t max_time;

 public:
  RangeHead(const std::shared_ptr<Head> &head, int64_t min_time,
            int64_t max_time)
      : head(head), min_time(min_time), max_time(max_time) {}

  virtual std::shared_ptr<block::IndexReaderInterface> global_index()
      const override {
    return nullptr;
  }

  // index returns an IndexReader over the block's data.
  std::pair<std::shared_ptr<block::IndexReaderInterface>, bool> index()
      const override {
    return {std::shared_ptr<block::IndexReaderInterface>(new HeadIndexReader(
                head.get(), std::max(min_time, head->MinTime()), max_time)),
            true};
  }

  // chunks returns a ChunkReader over the block's data.
  std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool> chunks()
      const override {
    return {std::shared_ptr<block::ChunkReaderInterface>(new HeadChunkReader(
                head.get(), std::max(min_time, head->MinTime()), max_time)),
            true};
  }

  // tombstones returns a TombstoneReader over the block's deleted data.
  std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
  tombstones() const override {
    return {std::shared_ptr<tombstone::TombstoneReaderInterface>(
                new tombstone::MemTombstones()),
            true};
  }

  virtual int64_t MaxTime() const override { return max_time; }

  virtual int64_t MinTime() const override { return min_time; }

  // TODO(Alec).
  error::Error error() const override { return error::Error(); }
};

}  // namespace head
}  // namespace tsdb

#endif