#ifndef BLOCKQUERIER_H
#define BLOCKQUERIER_H

#include "block/BlockInterface.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "querier/QuerierInterface.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"

namespace tsdb {
namespace querier {

class BlockQuerier : public QuerierInterface {
 private:
  std::shared_ptr<block::IndexReaderInterface> indexr;
  std::shared_ptr<block::ChunkReaderInterface> chunkr;
  std::shared_ptr<tombstone::TombstoneReaderInterface> tombstones;
  int64_t min_time;
  int64_t max_time;
  mutable error::Error err_;

  bool contains_global_index_;

 public:
  BlockQuerier(const std::shared_ptr<block::BlockInterface> &block,
               int64_t min_time, int64_t max_time,
               bool contains_gindex = false);

  std::shared_ptr<SeriesSetInterface> select(
      const std::vector<std::shared_ptr<label::MatcherInterface>> &l,
      std::unique_ptr<index::PostingsInterface> &&p) const override;

  std::vector<std::string> label_values(const std::string &s) const override;

  std::vector<std::string> label_names() const override;

  error::Error error() const override { return err_; }

  virtual bool contains_global_index() const override {
    return contains_global_index_;
  }
};

}  // namespace querier
}  // namespace tsdb

#endif