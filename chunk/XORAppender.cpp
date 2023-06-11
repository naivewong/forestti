#include "chunk/XORAppender.hpp"

#include "base/Endian.hpp"

namespace tsdb {
namespace chunk {

// Must be vector mode BitStream
XORAppender::XORAppender(BitStream &bstream, int64_t timestamp, double value,
                         uint64_t delta_timestamp, uint8_t leading_zero,
                         uint8_t trailing_zero)
    : bstream(bstream),
      timestamp(timestamp),
      value(value),
      delta_timestamp(delta_timestamp),
      leading_zero(leading_zero),
      trailing_zero(trailing_zero) {}

void XORAppender::set_leading_zero(uint8_t lz) { leading_zero = lz; }

void XORAppender::set_trailing_zero(uint8_t tz) { trailing_zero = tz; }

void XORAppender::write_delta_value(double value) {
  uint64_t delta_vale =
      base::encode_double(value) ^ base::encode_double(this->value);

  if (delta_vale == 0) {
    bstream.write_bit(ZERO);
    return;
  }

  bstream.write_bit(ONE);  // First control bit

  // TODO compiler unrelated
  uint8_t lz = static_cast<uint8_t>(__builtin_clzll(delta_vale));
  uint8_t tz = static_cast<uint8_t>(__builtin_ctzll(delta_vale));

  // Clamp number of leading zeros to avoid overflow when encoding.
  if (lz >= 32) lz = 31;

  if (leading_zero != 0xff && lz >= leading_zero && tz >= trailing_zero) {
    // std::cout << "write bits " << 64 - static_cast<int>(leading_zero) -
    // static_cast<int>(trailing_zero) << std::endl;
    bstream.write_bit(ZERO);  // Second control bit
    bstream.write_bits(
        delta_vale >> trailing_zero,
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero));
  } else {
    leading_zero = lz;
    trailing_zero = tz;

    bstream.write_bit(ONE);  // Second control bit
    bstream.write_bits(static_cast<uint64_t>(lz), 5);

    // Note that if leading == trailing == 0, then sigbits == 64.  But that
    // value doesn't actually fit into the 6 bits we have. Luckily, we never
    // need to encode 0 significant bits, since that would put us in the other
    // case (vdelta == 0). So instead we write out a 0 and adjust it back to 64
    // on unpacking.
    int sigbits =
        64 - static_cast<int>(leading_zero) - static_cast<int>(trailing_zero);
    bstream.write_bits(static_cast<uint64_t>(sigbits), 6);
    bstream.write_bits(delta_vale >> trailing_zero, sigbits);
  }
}

void XORAppender::append(int64_t timestamp, double value) {
  uint64_t current_delta_timestamp = 0;
  int num_samples = base::get_uint16_big_endian(bstream.bytes());

  if (num_samples == 0) {
    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    bstream.write_bits(base::encode_double(value), 64);
  } else if (num_samples == 1) {
    current_delta_timestamp = timestamp - this->timestamp;

    uint8_t temp[base::MAX_VARINT_LEN_64];
    int encoded = base::encode_signed_varint(temp, current_delta_timestamp);
    for (int i = 0; i < encoded; i++) bstream.write_byte(temp[i]);

    write_delta_value(value);
  } else {
    current_delta_timestamp = timestamp - this->timestamp;
    int64_t delta_delta_timestamp =
        current_delta_timestamp - this->delta_timestamp;

    if (delta_delta_timestamp == 0) {
      bstream.write_bit(ZERO);
    } else if (bit_range(delta_delta_timestamp, 14)) {
      bstream.write_bits(0x02, 2);  // 10
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 14);
    } else if (bit_range(delta_delta_timestamp, 17)) {
      bstream.write_bits(0x06, 3);  // 110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 17);
    } else if (bit_range(delta_delta_timestamp, 20)) {
      bstream.write_bits(0x0e, 4);  // 1110
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 20);
    }
    // The above 3 cases need to consider the sign when decoding
    else {
      bstream.write_bits(0x0f, 4);  // 1111
      bstream.write_bits(static_cast<uint64_t>(delta_delta_timestamp), 64);
    }

    write_delta_value(value);
  }

  this->timestamp = timestamp;
  this->value = value;
  base::put_uint16_big_endian(bstream.bytes(), num_samples + 1);
  this->delta_timestamp = current_delta_timestamp;
}

bool XORAppender::bit_range(int64_t delta_delta_timestamp, int num) {
  return ((-((1 << (num - 1)) - 1) <= delta_delta_timestamp) &&
          (delta_delta_timestamp <= 1 << (num - 1)));
}

}  // namespace chunk
}  // namespace tsdb