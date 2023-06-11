#ifndef RECORDDECODER_H
#define RECORDDECODER_H

#include <deque>
#include <vector>

#include "base/Error.hpp"
#include "tsdbutil/DecBuf.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WALUtils.hpp"

namespace tsdb {
namespace tsdbutil {

// RecordDecoder decodes series, sample, and tombstone records.
// The zero value is ready to use.
class RecordDecoder {
 public:
  static RECORD_ENTRY_TYPE type(const std::vector<uint8_t> &rec);
  static RECORD_ENTRY_TYPE type(const uint8_t *rec, int length);

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
  // Must pass an existed array.
  // Series appends series in rec to the given slice.
  static error::Error series(const std::vector<uint8_t> &rec,
                             std::vector<RefSeries> &refseries);
  static error::Error series(const uint8_t *rec, int length,
                             std::vector<RefSeries> &refseries);

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
  // Samples appends samples in rec to the given slice.
  static error::Error samples(const std::vector<uint8_t> &rec,
                              std::vector<RefSample> &refsamples);
  static error::Error samples(const uint8_t *rec, int length,
                              std::vector<RefSample> &refsamples);

  // ┌─────────────────────────────────────────────────────┐
  // │ type = 3 <1b>                                       │
  // ├─────────────────────────────────────────────────────┤
  // │ ┌─────────┬───────────────────┬───────────────────┐ │
  // │ │ id <8b> │ min_time <varint> │ max_time <varint> │ │
  // │ └─────────┴───────────────────┴───────────────────┘ │
  // │                        . . .                        │
  // └─────────────────────────────────────────────────────┘
  //
  // Tombstones appends tombstones in rec to the given slice.
  static error::Error tombstones(const std::vector<uint8_t> &rec,
                                 std::vector<Stone> &stones);
  static error::Error tombstones(const uint8_t *rec, int length,
                                 std::vector<Stone> &stones);
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif