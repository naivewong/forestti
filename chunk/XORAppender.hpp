#ifndef XORAPPENDER_H
#define XORAPPENDER_H

#include "chunk/BitStream.hpp"
#include "chunk/ChunkAppenderInterface.hpp"

namespace tsdb {
namespace chunk {

class XORAppender : public ChunkAppenderInterface {
 private:
  BitStream &bstream;
  int64_t timestamp;  // Millisecond
  double value;
  uint64_t delta_timestamp;
  uint8_t leading_zero;
  uint8_t trailing_zero;

 public:
  // Must be vector mode BitStream
  XORAppender(BitStream &bstream, int64_t timestamp, double value,
              uint64_t delta_timestamp, uint8_t leading_zero,
              uint8_t trailing_zero);

  void set_leading_zero(uint8_t lz);

  void set_trailing_zero(uint8_t tz);

  void write_delta_value(double value);

  void append(int64_t timestamp, double value);

  bool bit_range(int64_t delta_delta_timestamp, int num);
};

}  // namespace chunk
}  // namespace tsdb

#endif