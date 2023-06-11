#pragma once

#include <string>

#include "chunk/BitStream.hpp"
#include "chunk/ChunkAppenderInterface.hpp"
#include "chunk/ChunkInterface.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
#include "chunk/XORIterator.hpp"

namespace tsdb {
namespace chunk {

class XORChunk : public ChunkInterface {
 public:
  BitStream bstream;
  bool read_mode;
  uint64_t size_;

  uint8_t *rbuf_;

 public:
  // The first two bytes store the num of samples using big endian
  XORChunk();

  // Reserve a header space.
  XORChunk(int header_size);

  XORChunk(const uint8_t *stream_ptr, uint64_t size, bool copy = false);

  ~XORChunk();

  const uint8_t *bytes();

  uint8_t encoding();

  std::unique_ptr<ChunkAppenderInterface> appender();

  std::unique_ptr<ChunkIteratorInterface> iterator();

  std::unique_ptr<XORIterator> xor_iterator();

  int num_samples();

  uint64_t size();
};

}  // namespace chunk
}  // namespace tsdb
