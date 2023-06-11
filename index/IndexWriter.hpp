#pragma once

#include <deque>
#include <initializer_list>
#include <set>
#include <unordered_map>
#include <vector>

#include "block/IndexWriterInterface.hpp"
#include "index/HashEntry.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/CacheVector.hpp"
#include "tsdbutil/EncBuf.hpp"

namespace tsdb {
namespace index {

class IndexWriter : public block::IndexWriterInterface {
 private:
  FILE *f_;
  uint64_t pos_;

  tsdbutil::EncBuf buf1_;
  tsdbutil::EncBuf buf2_;

  std::vector<uint64_t> ids_;
  std::vector<uint64_t> offsets_;

  uint64_t footer_offset_;

  label::Labels last_series_;

  std::string filename_;

  void write_meta();

  void write(const std::vector<uint8_t> &v, int len);

  void add_padding(uint64_t padding);

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
  void write_ids();

  // ┌──────────────────────────┐
  // │ ref(footer) <8b>         │
  // ├──────────────────────────┤
  // │ CRC32 <4b>               │
  // └──────────────────────────┘
  void write_TOC();

 public:
  // All the dirs inside filename should be existed.
  IndexWriter(const std::string &filename);
  IndexWriter() : f_(nullptr) {}

  // ┌──────────────────────────────────────────────────────────────────────────┐
  // │ len <uvarint> │
  // ├──────────────────────────────────────────────────────────────────────────┤
  // │ ┌──────────────────────────────────────────────────────────────────────┐
  // │ │ │                     chunks count <uvarint64> │ │ │
  // ├──────────────────────────────────────────────────────────────────────┤ │
  // │ │              ┌────────────────────────────────────────────┐          │
  // │ │ │              │ c_0.mint <varint64>                        │ │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │ c_0.maxt
  // - c_0.mint <uvarint64>            │          │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │
  // ref(c_0.data) <uvarint64>                  │          │ │ │ │
  // └────────────────────────────────────────────┘          │ │ │ │
  // ┌────────────────────────────────────────────┐          │ │ │ │ │ c_i.mint
  // - c_i-1.maxt <uvarint64>          │          │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │ c_i.maxt
  // - c_i.mint <uvarint64>            │          │ │ │ │
  // ├────────────────────────────────────────────┤          │ │ │ │ │
  // ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │ │ │
  // └────────────────────────────────────────────┘          │ │ │ │ ... │ │ │
  // └──────────────────────────────────────────────────────────────────────┘ │
  // ├──────────────────────────────────────────────────────────────────────────┤
  // │ CRC32 <4b> │
  // └──────────────────────────────────────────────────────────────────────────┘
  //
  // Should add id in ascending order.
  //
  // chunks here better to be sorted by time.
  int add_series(uint64_t id, const label::Labels &l,
                 const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks);

  ~IndexWriter();
};

}  // namespace index
}  // namespace tsdb
