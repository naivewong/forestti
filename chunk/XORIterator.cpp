#include "chunk/XORIterator.hpp"

#include <string.h>

#include "base/Endian.hpp"

namespace tsdb {
namespace chunk {

// Read mode BitStream
XORIterator::XORIterator(BitStream &bstream, bool safe_mode)
    : safe_mode(safe_mode),
      timestamp(0),
      value(0),
      delta_timestamp(0),
      leading_zero(0),
      trailing_zero(0),
      num_read(0),
      err_(false) {
  // This is to prevent pointer invalidation when vector resizes during
  // appending new data. For those XORChunk not created in read mode.
  if (safe_mode) {
    std::vector<uint8_t> stream(bstream.size(), 0);
    memcpy(&(stream.front()), bstream.bytes_ptr(), bstream.size());
    this->bstream = BitStream(stream);
  } else
    this->bstream = BitStream(bstream.bytes_ptr(), bstream.size());

  num_total = base::get_uint16_big_endian(bstream.bytes_ptr());
  // Pop the first two bytes
  this->bstream.pop_front();
  this->bstream.pop_front();
}

std::pair<int64_t, double> XORIterator::at() const {
  return std::make_pair(timestamp, value);
}

bool XORIterator::next() const {
  if (err_ || num_read == num_total) return false;

  if (num_read == 0) {
    int64_t current_t;
    try {
      current_t = bstream.read_signed_varint();
      // std::cout << "current_t: " << current_t << std::endl;
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    uint64_t current_v;
    try {
      current_v = bstream.read_bits(64);
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    timestamp = current_t;
    value = base::decode_double(current_v);
    // std::cout << "first value " << current_v << " " << value << std::endl;
    ++num_read;
    return true;
  } else if (num_read == 1) {
    int64_t delta_t;
    try {
      delta_t = bstream.read_signed_varint();
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }

    delta_timestamp = delta_t;
    timestamp += delta_t;

    return read_value();
  }

  // Read timestamp delta-delta
  uint8_t type = 0;
  int64_t delta_delta = 0;
  for (int i = 0; i < 4; i++) {
    try {
      bool bit = bstream.read_bit();
      type <<= 1;
      if (!bit) break;
      type |= 1;
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  int size = 0;
  switch (type) {
    case 0x02:
      size = 14;
      break;
    case 0x06:
      size = 17;
      break;
    case 0x0e:
      size = 20;
      break;
    case 0x0f:
      try {
        delta_delta = static_cast<int64_t>(bstream.read_bits(64));
      } catch (const base::TSDBException &e) {
        err_ = true;
        return false;
      }
  }
  if (size != 0) {
    try {
      delta_delta = static_cast<int64_t>(bstream.read_bits(size));
      if (delta_delta > (1 << (size - 1))) {
        delta_delta -= (1 << size);
      }
    } catch (const base::TSDBException &e) {
      err_ = true;
      return false;
    }
  }

  // Possible overflow
  delta_timestamp = static_cast<uint64_t>(
      delta_delta + static_cast<int64_t>(delta_timestamp));
  timestamp += static_cast<int64_t>(delta_timestamp);
  return read_value();
}

bool XORIterator::read_value() const {
  bool control_bit;
  try {
    control_bit = bstream.read_bit();  // First control bit
  } catch (const base::TSDBException &e) {
    return false;
  }

  if (control_bit == ZERO) {
    // it.val = it.val
  } else {
    try {
      control_bit = bstream.read_bit();  // Second control bit
    } catch (const base::TSDBException &e) {
      return false;
    }

    if (control_bit != ZERO) {
      uint8_t bits;
      try {
        bits = static_cast<uint8_t>(bstream.read_bits(5));
      } catch (const base::TSDBException &e) {
        return false;
      }
      leading_zero = bits;

      try {
        bits = static_cast<uint8_t>(bstream.read_bits(6));
      } catch (const base::TSDBException &e) {
        return false;
      }
      // 0 significant bits here means we overflowed and we actually need 64;
      // see comment in encoder
      if (bits == 0) {
        bits = 64;
      }
      trailing_zero = static_cast<uint8_t>(64 - leading_zero - bits);
    }

    uint64_t bits;
    try {
      bits = bstream.read_bits(
          static_cast<int>(64 - leading_zero - trailing_zero));
    } catch (const base::TSDBException &e) {
      return false;
    }
    uint64_t vbits = base::encode_double(value);
    vbits ^= (bits << trailing_zero);
    value = base::decode_double(vbits);
  }

  ++num_read;
  return true;
}

bool XORIterator::error() const { return err_; }

}  // namespace chunk
}  // namespace tsdb