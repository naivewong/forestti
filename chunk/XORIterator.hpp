#ifndef XORIterator_H
#define XORIterator_H

#include "chunk/BitStream.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
// #include <iostream>

namespace tsdb {
namespace chunk {

class XORIterator : public ChunkIteratorInterface {
 public:
  mutable BitStream bstream;
  mutable int64_t timestamp;  // Millisecond
  mutable double value;
  mutable uint64_t delta_timestamp;
  mutable uint8_t leading_zero;
  mutable uint8_t trailing_zero;
  mutable uint16_t num_total;
  mutable uint16_t num_read;
  mutable bool err_;
  bool safe_mode;

 public:
  // Read mode BitStream
  XORIterator(BitStream &bstream, bool safe_mode);

  std::pair<int64_t, double> at() const;

  bool next() const;

  bool read_value() const;

  bool error() const;
};

}  // namespace chunk
}  // namespace tsdb

#endif