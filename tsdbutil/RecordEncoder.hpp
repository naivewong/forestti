#ifndef RECORDENCODER_H
#define RECORDENCODER_H

#include <deque>
#include <vector>

#include "base/Error.hpp"
#include "tsdbutil/EncBuf.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WALUtils.hpp"

namespace tsdb {
namespace tsdbutil {

class RecordEncoder {
 public:
  // ┌────────────────────────────────────────────┐
  // │ type = 1 <1b>                              │
  // ├────────────────────────────────────────────┤
  // │ ┌─────────┬──────────────────────────────┐ │
  // │ │ id <8b> │ n = len(labels) <uvarint>    │ │
  // │ ├─────────┴────────────┬─────────────────┤ │
  // │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
  // │ ├──────────────────────┴─────────────────┤ │
  // │ │  ...                                   │ │
  // │ ├───────────────────────┬────────────────┤ │
  // │ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │
  // │ └───────────────────────┴────────────────┘ │
  // │                  . . .                     │
  // └────────────────────────────────────────────┘
  //
  // Series appends the encoded series to b and returns the resulting slice.
  static void series(const std::vector<RefSeries> &refseries,
                     std::vector<uint8_t> &rec);

  // ┌──────────────────────────────────────────────────────────────────┐
  // │ type = 2 <1b>                                                    │
  // ├──────────────────────────────────────────────────────────────────┤
  // │ ┌────────────────────┬───────────────────────────┬─────────────┐ │
  // │ │ id <8b>            │ timestamp <8b>            │ value <8b>  │ │
  // │ └────────────────────┴───────────────────────────┴─────────────┘ │
  // │ ┌────────────────────┬───────────────────────────┬─────────────┐ │
  // │ │ id_delta <varint>  │ timestamp_delta <varint>  │ value <8b>  │ │
  // │ └────────────────────┴───────────────────────────┴─────────────┘ │
  // │                              . . .                               │
  // └──────────────────────────────────────────────────────────────────┘
  //
  // Samples appends the encoded samples to b and returns the resulting slice.
  static void samples(const std::vector<RefSample> &refsamples,
                      std::vector<uint8_t> &rec);

  // ┌─────────────────────────────────────────────────────┐
  // │ type = 3 <1b>                                       │
  // ├─────────────────────────────────────────────────────┤
  // │ ┌─────────┬───────────────────┬───────────────────┐ │
  // │ │ id <8b> │ min_time <varint> │ max_time <varint> │ │
  // │ └─────────┴───────────────────┴───────────────────┘ │
  // │                        . . .                        │
  // └─────────────────────────────────────────────────────┘
  //
  // Tombstones appends the encoded tombstones to b and returns the resulting
  // slice.
  static void tombstones(const std::vector<Stone> &stones,
                         std::vector<uint8_t> &rec);
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif
