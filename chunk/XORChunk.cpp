#include "chunk/XORChunk.hpp"

#include <cstring>

#include "base/Endian.hpp"
#include "base/TSDBException.hpp"
#include "chunk/EmptyAppender.hpp"
#include "chunk/XORAppender.hpp"

namespace tsdb {
namespace chunk {

// The first two bytes store the num of samples using big endian
XORChunk::XORChunk() : bstream(2), read_mode(false), rbuf_(nullptr) {}

XORChunk::XORChunk(int header_size)
    : bstream(2, header_size), read_mode(false), rbuf_(nullptr) {}

XORChunk::XORChunk(const uint8_t *stream_ptr, uint64_t size, bool copy)
    : bstream(stream_ptr, size), read_mode(true), size_(size), rbuf_(nullptr) {
  if (copy) {
    rbuf_ = new uint8_t[size];
    memcpy(rbuf_, stream_ptr, size);
    bstream = BitStream(rbuf_, size);
  }
}

XORChunk::~XORChunk() {
  if (rbuf_) delete[] rbuf_;
}

const uint8_t *XORChunk::bytes() {
  if (read_mode)
    return bstream.stream_ptr;
  else
    return bstream.bytes_ptr();
}

uint8_t XORChunk::encoding() { return static_cast<uint8_t>(EncXOR); }

std::unique_ptr<ChunkAppenderInterface> XORChunk::appender() {
  if (read_mode)
    return std::unique_ptr<ChunkAppenderInterface>(new EmptyAppender());
  std::unique_ptr<XORIterator> it = xor_iterator();
  while (it->next()) {
  }
  if (it->error()) {
    // LOG_ERROR << "Broken BitStream in XORChunk";
    throw base::TSDBException("Broken BitStream in XORChunk");
  }

  uint8_t lz = base::get_uint16_big_endian(bstream.bytes()) == 0
                   ? 0xff
                   : it->leading_zero;
  return std::unique_ptr<ChunkAppenderInterface>(
      new XORAppender(bstream, it->timestamp, it->value, it->delta_timestamp,
                      lz, it->trailing_zero));
}

std::unique_ptr<ChunkIteratorInterface> XORChunk::iterator() {
  return std::unique_ptr<ChunkIteratorInterface>(
      new XORIterator(bstream, !read_mode));
  // NOTE(Alec): boost
  // new XORIterator(bstream, false));
}

std::unique_ptr<XORIterator> XORChunk::xor_iterator() {
  // No need to use safe mode here because there can be only one appender at the
  // same time.
  return std::unique_ptr<XORIterator>(new XORIterator(bstream, false));
}

int XORChunk::num_samples() { return base::get_uint16_big_endian(bytes()); }

uint64_t XORChunk::size() {
  if (read_mode)
    return size_;
  else
    return bstream.size() - bstream.header_size;
}

}  // namespace chunk
}  // namespace tsdb