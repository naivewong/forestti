#ifndef BASE_BitStream_H
#define BASE_BitStream_H

// #include <iostream>
#include <string>
#include <vector>

#include "base/TSDBException.hpp"

namespace tsdb {
namespace chunk {

extern const bool ZERO;
extern const bool ONE;

// TODO possible memory fragmentation
// TODO RW mode
class BitStream {
 public:
  std::vector<uint8_t> stream;
  const uint8_t *stream_ptr;
  uint8_t head_count;  // number of valid bits in current byte
  uint8_t tail_count;
  bool vector_mode;
  int index;
  int end;

  int header_size;

 public:
  BitStream(BitStream &bstream, bool iterator_mode);

  // Read mode & vector_mode
  BitStream(const std::vector<uint8_t> &stream);

  // Read mode & pointer_mode
  BitStream(const uint8_t *stream_ptr, int size);

  // Write mode, tail_count
  BitStream();

  // Write mode, tail_count
  BitStream(int size, int header_size = 0);

  void write_bit(bool bit);
  void write_byte(uint8_t byte);
  void write_bytes_be(int64_t bytes, int num);
  void write_bits(uint64_t bits, int num);
  void write_stream(const BitStream &s, int num);
  void write_padding(int num);
  bool read_bit();
  uint8_t read_byte();
  uint64_t read_bits(int num);
  uint64_t read_unsigned_varint();
  int64_t read_signed_varint();

  // position in bit.
  bool read_bit(int pos);
  uint8_t read_byte(int pos);
  int64_t read_bytes_be(int pos, int num);
  uint64_t read_bits(int pos, int num);
  // <value, decode bits num>.
  std::pair<uint64_t, uint8_t> read_unsigned_varint(int pos);
  std::pair<int64_t, uint8_t> read_signed_varint(int pos);

  // Only called when vector mode
  uint8_t *bytes();

  const uint8_t *bytes_ptr() const;

  void pop_front();

  int size();
  int write_pos();

 private:
  uint8_t read_byte_helper(int &index_, uint8_t &head_count_);
};

}  // namespace chunk
}  // namespace tsdb

#endif