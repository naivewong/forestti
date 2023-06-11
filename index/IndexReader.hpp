#pragma once

#include "block/BlockUtils.hpp"
#include "block/IndexReaderInterface.hpp"
#include "index/IndexUtils.hpp"
#include "index/TOC.hpp"
#include "tsdbutil/ByteSlice.hpp"
#include "tsdbutil/SerializedStringTuples.hpp"

namespace tsdb {
namespace index {

class IndexReader : public block::IndexReaderInterface {
 private:
  std::shared_ptr<tsdbutil::ByteSlice> b;

  uint64_t footer_offset_;
  uint64_t footer_size_;
  const uint8_t *footer_;
  bool err_;

  void init();

  bool validate(const std::shared_ptr<tsdbutil::ByteSlice> &b);

 public:
  IndexReader(std::shared_ptr<tsdbutil::ByteSlice> b);
  IndexReader(const std::string &filename);

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
  bool series(uint64_t ref,
              std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) override;

  bool error() override;
  uint64_t size() override;
};

// uint64_t symbol_table_size(const IndexReader &indexr);

}  // namespace index
}  // namespace tsdb
