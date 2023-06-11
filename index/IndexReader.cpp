#include <cstring>
// #include <iostream>
#include <boost/bind.hpp>
// #include <boost/function.hpp>
#include <boost/tokenizer.hpp>

#include "base/Endian.hpp"
#include "base/Logging.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IndexReader.hpp"
#include "index/ListPostings.hpp"
#include "index/Uint32BEPostings.hpp"
#include "index/VectorPostings.hpp"
#include "label/Label.hpp"
#include "tsdbutil/DecBuf.hpp"
#include "tsdbutil/MMapSlice.hpp"

namespace tsdb {
namespace index {

IndexReader::IndexReader(std::shared_ptr<tsdbutil::ByteSlice> b) : err_(false) {
  if (!validate(b)) {
    LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
    err_ = true;
    return;
  }
  this->b = b;
  init();
}

IndexReader::IndexReader(const std::string &filename) : err_(false) {
  std::shared_ptr<tsdbutil::ByteSlice> temp =
      std::shared_ptr<tsdbutil::ByteSlice>(new tsdbutil::MMapSlice(filename));
  if (!validate(temp)) {
    LOG_ERROR << "Fail to create IndexReader, invalid ByteSlice";
    err_ = true;
    return;
  }
  this->b = temp;
  init();
}

void IndexReader::init() {
  std::pair<const uint8_t *, int> p = b->range(b->len() - 12, b->len());
  uint32_t crc1 = base::get_uint32_big_endian(p.first + p.second - 4);
  uint32_t crc2 = base::GetCrc32(p.first, p.second - 4);

  if (crc1 != crc2) {
    LOG_ERROR << "Corrupted TOC: crc not equal";
    b.reset();
    err_ = true;
    return;
  }
  tsdbutil::DecBuf dec(p.first, p.second - 4);
  footer_offset_ = dec.get_BE_uint64();

  footer_ = (b->range(footer_offset_, b->len())).first;
  footer_size_ = b->len() - footer_offset_ - 16;
}

bool IndexReader::validate(const std::shared_ptr<tsdbutil::ByteSlice> &b) {
  if (b->len() < 4) {
    LOG_ERROR << "Length of ByteSlice < 4";
    return false;
  }
  if (base::get_uint32_big_endian((b->range(0, 4)).first) != MAGIC_INDEX) {
    LOG_ERROR << "Not beginning with MAGIC_INDEX";
    return false;
  }
  if (*((b->range(4, 5)).first) != INDEX_VERSION_V1) {
    LOG_ERROR << "Invalid Index Version";
    return false;
  }
  return true;
}

// ┌─────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                           │
// ├─────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────┬──────────────────────────────────────────────────┐ │
// │ │                  │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ c_0.mint <varint>                        │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ c_0.maxt - c_0.mint <uvarint>            │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ ref(c_0.data) <uvarint>                  │     │ │
// │ │      #chunks     │ └──────────────────────────────────────────┘     │ │
// │ │     <uvarint>    │ ┌──────────────────────────────────────────┐     │ │
// │ │                  │ │ c_i.mint - c_i-1.maxt <uvarint>          │     │ │
// │ │                  │ ├──────────────────────────────────────────┤     │ │
// │ │                  │ │ c_i.maxt - c_i.mint <uvarint>            │     │ │
// │ │                  │ ├──────────────────────────────────────────┤ ... │ │
// │ │                  │ │ ref(c_i.data) - ref(c_i-1.data) <varint> │     │ │
// │ │                  │ └──────────────────────────────────────────┘     │ │
// │ └──────────────────┴──────────────────────────────────────────────────┘ │
// ├─────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                              │
// └─────────────────────────────────────────────────────────────────────────┘
bool IndexReader::series(
    uint64_t ref, std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
  if (!b) {
    return false;
  }

  // Binary search.
  uint64_t first = 0, it = 0;
  int count = footer_size_ / 16, step;
  while (count > 0) {
    it = first;
    step = count / 2;
    it += step * 16;
    if (base::get_uint64_big_endian(footer_ + it) < ref) {
      first = it + 16;
      count -= step + 1;
    } else
      count = step;
  }
  if (first >= footer_size_ ||
      base::get_uint64_big_endian(footer_ + first) != ref) {
    // Series not found in this block.
    // LOG_DEBUG << "out of bound " << first << " " << footer_size_;
    return false;
  }

  uint64_t series_off = base::get_uint64_big_endian(footer_ + first + 8);
  const uint8_t *start =
      (b->range(series_off, series_off + base::MAX_VARINT_LEN_64)).first;
  int decoded;
  uint64_t len =
      base::decode_unsigned_varint(start, decoded, base::MAX_VARINT_LEN_64);
  tsdbutil::DecBuf dec_buf(start + decoded, len);

  // Decode the Chunks
  uint64_t num_chunks = dec_buf.get_unsigned_variant();
  if (num_chunks == 0) return true;

  // First chunk meta
  int64_t last_t = dec_buf.get_signed_variant();
  uint64_t delta_t = dec_buf.get_unsigned_variant();
  int64_t last_ref = static_cast<int64_t>(dec_buf.get_unsigned_variant());
  if (dec_buf.err != tsdbutil::NO_ERR) {
    LOG_ERROR << "Fail to read series, fail to read chunk meta 0";
    return false;
  }
  chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
      new chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                           static_cast<int64_t>(delta_t) + last_t)));

  for (int i = 1; i < num_chunks; i++) {
    last_t += static_cast<int64_t>(dec_buf.get_unsigned_variant() + delta_t);
    delta_t = dec_buf.get_unsigned_variant();
    last_ref += dec_buf.get_signed_variant();
    if (dec_buf.err != tsdbutil::NO_ERR) {
      LOG_ERROR << "Fail to read series, fail to read chunk meta " << i;
      return false;
    }
    // LOG_INFO << last_t << " " << delta_t;
    chunks.push_back(std::shared_ptr<chunk::ChunkMeta>(
        new chunk::ChunkMeta(static_cast<uint64_t>(last_ref), last_t,
                             static_cast<int64_t>(delta_t) + last_t)));
  }
  return true;
}

bool IndexReader::error() { return err_; }

uint64_t IndexReader::size() {
  if (!b) return 0;
  return b->len();
}

// uint64_t symbol_table_size(const IndexReader &indexr) {
//   uint64_t size = 0;
//   for (const std::string &s : indexr.symbols_deque()) size += s.length() + 8;
//   return size;
// }

}  // namespace index
}  // namespace tsdb