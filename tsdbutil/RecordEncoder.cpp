#include "tsdbutil/RecordEncoder.hpp"

namespace tsdb {
namespace tsdbutil {

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
void RecordEncoder::series(const std::vector<RefSeries> &refseries,
                           std::vector<uint8_t> &rec) {
  tsdbutil::EncBuf encbuf(10 * refseries.size());
  encbuf.put_byte(RECORD_SERIES);

  for (const RefSeries &r : refseries) {
    encbuf.put_BE_uint64(r.ref);
    encbuf.put_BE_uint64(r.flushed_txn);
    encbuf.put_BE_uint64(r.log_clean_txn);
    const label::Labels *lptr = &r.lset;
    if (r.lset_ptr) lptr = r.lset_ptr;
    encbuf.put_unsigned_variant(lptr->size());

    for (const label::Label &l : *lptr) {
      encbuf.put_uvariant_string(l.label);
      encbuf.put_uvariant_string(l.value);
    }
  }

  rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
}

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
void RecordEncoder::samples(const std::vector<RefSample> &refsamples,
                            std::vector<uint8_t> &rec) {
  if (refsamples.empty()) return;
  tsdbutil::EncBuf encbuf(10 * refsamples.size());
  encbuf.put_byte(RECORD_SAMPLES);

  encbuf.put_BE_uint64(refsamples[0].ref);
  encbuf.put_BE_uint64(refsamples[0].logical_id);
  encbuf.put_BE_uint64(static_cast<uint64_t>(refsamples[0].t));
  encbuf.put_BE_uint64(base::encode_double(refsamples[0].v));
  for (int i = 1; i < refsamples.size(); ++i) {
    encbuf.put_signed_variant(static_cast<int64_t>(refsamples[i].ref) -
                              static_cast<int64_t>(refsamples.front().ref));
    encbuf.put_signed_variant(
        static_cast<int64_t>(refsamples[i].logical_id) -
        static_cast<int64_t>(refsamples.front().logical_id));
    encbuf.put_signed_variant(refsamples[i].t - refsamples.front().t);
    encbuf.put_BE_uint64(base::encode_double(refsamples[i].v));
  }

  rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
}

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
void RecordEncoder::tombstones(const std::vector<Stone> &stones,
                               std::vector<uint8_t> &rec) {
  tsdbutil::EncBuf encbuf(10 * stones.size());
  encbuf.put_byte(RECORD_TOMBSTONES);

  for (const Stone &s : stones) {
    for (const tombstone::Interval &i : s.itvls) {
      encbuf.put_BE_uint64(s.ref);
      encbuf.put_signed_variant(i.min_time);
      encbuf.put_signed_variant(i.max_time);
    }
  }

  rec.insert(rec.end(), encbuf.b.begin(), encbuf.b.begin() + encbuf.index);
}

}  // namespace tsdbutil
}  // namespace tsdb