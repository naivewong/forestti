#ifndef CHUNKWRITER_H
#define CHUNKWRITER_H

#include <stdio.h>

#include <vector>

#include "block/ChunkWriterInterface.hpp"
#include "chunk/ChunkMeta.hpp"
#include "leveldb/db.h"

namespace tsdb {
namespace chunk {

class ChunkWriter : public block::ChunkWriterInterface {
 private:
  std::string dir;  // "[ulid]/chunks"
  std::vector<FILE *> files;
  std::vector<std::string> file_names;
  std::vector<int> seqs;
  uint64_t pos;
  uint64_t chunk_size;

 public:
  // Implicit construct from const char *
  ChunkWriter(const std::string &dir);

  FILE *tail();

  // finalize_tail writes all pending data to the current tail file and close it
  void finalize_tail();

  void cut();

  void write(const uint8_t *bytes, int size);

  void write_chunks(
      const std::deque<std::shared_ptr<ChunkMeta>> &chunks) override;

  uint64_t seq();

  void close() override;

  ~ChunkWriter();
};

class LevelDBChunkWriter : public block::ChunkWriterInterface {
 private:
  leveldb::DB *db_;

 public:
  LevelDBChunkWriter(leveldb::DB *db) : db_(db) {}

  void write_chunks(
      const std::deque<std::shared_ptr<ChunkMeta>> &chunks) override;
  void close() override {}
};

}  // namespace chunk
}  // namespace tsdb

#endif