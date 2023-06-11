#ifndef CHUNKREADER_H
#define CHUNKREADER_H

#include <stdint.h>

#include <deque>
#include <limits>

#include "block/ChunkReaderInterface.hpp"
#include "chunk/ChunkInterface.hpp"
#include "leveldb/db.h"
#include "tsdbutil/ByteSlice.hpp"

namespace tsdb {
namespace chunk {

// TODO(Alec), more chunk types.
class ChunkReader : public block::ChunkReaderInterface {
 private:
  std::deque<std::shared_ptr<tsdbutil::ByteSlice>> bs;

  bool err_;

  uint64_t size_;

 public:
  // Implicit construct from const char *
  ChunkReader(const std::string& dir);

  // Validate the back of bs after each push_back
  bool validate();

  // Will return EmptyChunk when error
  std::pair<std::shared_ptr<ChunkInterface>, bool> chunk(uint64_t ref) override;
  std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(
      uint64_t ref, int64_t starting_time) override;

  bool error() override;

  uint64_t size() override;
};

class LevelDBChunkReader : public block::ChunkReaderInterface {
 private:
  leveldb::DB* db_;
  std::string value_;
  bool err_;

 public:
  // Implicit construct from const char *
  LevelDBChunkReader(leveldb::DB* db) : db_(db), err_(false) {}

  // Will return EmptyChunk when error
  std::pair<std::shared_ptr<ChunkInterface>, bool> chunk(
      uint64_t ref) override {
    return std::make_pair(std::shared_ptr<ChunkInterface>(new EmptyChunk()),
                          false);
  }

  std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(
      uint64_t ref, int64_t starting_time) override;

  bool error() override { return err_; }

  uint64_t size() override { return std::numeric_limits<uint64_t>::max(); }
};

}  // namespace chunk
}  // namespace tsdb

#endif