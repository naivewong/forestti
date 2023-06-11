#include "index/IndexWriter.hpp"

#include <stdio.h>

#include <boost/filesystem.hpp>
#include <unordered_map>
#include <vector>

#include "base/Checksum.hpp"
#include "base/Logging.hpp"
#include "label/Label.hpp"
#include "tsdbutil/StringTuples.hpp"

namespace tsdb {
namespace index {

// All the dirs inside filename should be existed.
IndexWriter::IndexWriter(const std::string &filename)
    : pos_(0), buf1_(1 << 22), buf2_(1 << 22), filename_(filename) {
  boost::filesystem::path p(filename);
  if (boost::filesystem::exists(p)) boost::filesystem::remove_all(p);

  // Create parent path if not existed.
  if (p.has_parent_path() && !boost::filesystem::exists(p.parent_path()))
    boost::filesystem::create_directories(p.parent_path());

  f_ = fopen(filename.c_str(), "wb+");
  if (!f_) LOG_ERROR << "cannot open " << filename;

  write_meta();
}

void IndexWriter::write_meta() {
  buf1_.reset();
  buf1_.put_BE_uint32(MAGIC_INDEX);
  buf1_.put_byte(INDEX_VERSION_V1);
  write(buf1_.get_vector(), buf1_.len());
}

void IndexWriter::write(const std::vector<uint8_t> &v, int len) {
  for (int i = 0; i < len; i++) fputc(v[i], f_);
  pos_ += len;
}

void IndexWriter::add_padding(uint64_t padding) {
  uint64_t p = pos_ % padding;
  if (p != 0) {
    for (int i = 0; i < padding - p; i++) fputc(0, f_);
    pos_ += padding - p;
  }
}

// ┌──────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                            │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────────────────────────────────────────────────────────┐ │
// │ │                     chunks count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_0.mint <varint64>                        │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_0.data) <uvarint64>                  │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ └──────────────────────────────────────────────────────────────────────┘ │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                               │
// └──────────────────────────────────────────────────────────────────────────┘
int IndexWriter::add_series(
    uint64_t ref, const label::Labels &l,
    const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
  // if (label::lbs_compare(l, last_series_) < 0) {
  //   LOG_ERROR << "Out of order series";
  //   return OUT_OF_ORDER;
  // }

  // Align each entry to 16 bytes
  add_padding(16);
  ids_.push_back(ref);
  offsets_.push_back(pos_);

  buf2_.reset();
  buf2_.put_unsigned_variant(chunks.size());
  if (chunks.size() > 0) {
    int64_t last_t = chunks[0]->max_time;
    uint64_t last_ref = chunks[0]->ref;
    buf2_.put_signed_variant(chunks[0]->min_time);
    buf2_.put_unsigned_variant(
        static_cast<uint64_t>(chunks[0]->max_time - chunks[0]->min_time));
    buf2_.put_unsigned_variant(chunks[0]->ref);

    for (int i = 1; i < chunks.size(); i++) {
      // LOG_INFO << chunks[i]->min_time - last_t;
      buf2_.put_unsigned_variant(
          static_cast<uint64_t>(chunks[i]->min_time - last_t));
      // LOG_INFO << chunks[i]->max_time - chunks[i]->min_time;
      buf2_.put_unsigned_variant(
          static_cast<uint64_t>(chunks[i]->max_time - chunks[i]->min_time));
      buf2_.put_signed_variant(static_cast<int64_t>(chunks[i]->ref - last_ref));
      last_t = chunks[i]->max_time;
      last_ref = chunks[i]->ref;
    }
  }

  buf1_.reset();
  buf1_.put_unsigned_variant(buf2_.len());           // Len in the beginning
  buf2_.put_BE_uint32(base::GetCrc32(buf2_.get()));  // Crc32 in the end

  write(buf1_.get_vector(), buf1_.len());
  write(buf2_.get_vector(), buf2_.len());

  // last_series_ = l;
  return 0;
}

// ┌─────────────────────────────────────────┐
// │ ┌──────────────────┬──────────────────┐ │
// │ │ ID1 <8b>         │ Off1 <8b>        │ │
// │ ├──────────────────┴──────────────────┤ │
// │ │ ...                                 │ │
// │ ├──────────────────┬──────────────────┤ │
// │ │ ID2 <8b>         │ Offn <8b>        │ │
// │ └──────────────────┴──────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
void IndexWriter::write_ids() {
  footer_offset_ = pos_;

  assert(ids_.size() == offsets_.size());
  buf1_.reset();
  for (size_t i = 0; i < ids_.size(); i++) {
    buf1_.put_BE_uint64(ids_[i]);
    buf1_.put_BE_uint64(offsets_[i]);
  }
  buf1_.put_BE_uint32(base::GetCrc32(buf1_.get()));  // Crc32 in the end

  write(buf1_.get_vector(), buf1_.len());
}

// ┌──────────────────────────┐
// │ ref(footer) <8b>         │
// ├──────────────────────────┤
// │ CRC32 <4b>               │
// └──────────────────────────┘
void IndexWriter::write_TOC() {
  buf1_.reset();
  buf1_.put_BE_uint64(footer_offset_);
  buf1_.put_BE_uint32(base::GetCrc32(buf1_.get()));  // Crc32 in the end

  write(buf1_.get_vector(), buf1_.len());
}

IndexWriter::~IndexWriter() {
  if (f_ == nullptr) return;

  write_ids();
  write_TOC();

  fflush(f_);
  fclose(f_);
}

}  // namespace index
}  // namespace tsdb