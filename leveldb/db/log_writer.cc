// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"

#include "base/Endian.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace leveldb {
namespace log {

std::string series(const std::vector<RefSeries>& s) {
  std::string result;
  series(s, &result);
  return result;
}
void series(const std::vector<RefSeries>& series, std::string* result) {
  result->push_back(kSeries);

  for (const RefSeries& r : series) {
    PutFixed64BE(result, r.ref);
    PutFixed64BE(result, r.flushed_txn);
    PutFixed64BE(result, r.log_clean_txn);
    const tsdb::label::Labels* lptr = &r.lset;
    if (r.lset_ptr) lptr = r.lset_ptr;
    PutVarint32(result, lptr->size());

    for (const ::tsdb::label::Label& l : *lptr) {
      PutLengthPrefixedSlice(result, l.label);
      PutLengthPrefixedSlice(result, l.value);
    }
  }
}

void series(const ::tsdb::tsdbutil::RefSeries& r, std::string* result) {
  result->push_back(kSeries);

  PutFixed64BE(result, r.ref);
  PutFixed64BE(result, r.flushed_txn);
  PutFixed64BE(result, r.log_clean_txn);
  const tsdb::label::Labels* lptr = &r.lset;
  if (r.lset_ptr) lptr = r.lset_ptr;
  PutVarint32(result, lptr->size());

  for (const ::tsdb::label::Label& l : *lptr) {
    PutLengthPrefixedSlice(result, l.label);
    PutLengthPrefixedSlice(result, l.value);
  }
}

void series_without_labels(uint64_t ref, int64_t flushed_txn, int64_t log_clean_txn, std::string* result) {
  result->push_back(kSeries);

  PutFixed64BE(result, ref);
  PutFixed64BE(result, flushed_txn);
  PutFixed64BE(result, log_clean_txn);
}

void series_without_labels(const ::tsdb::tsdbutil::RefSeries& r, std::string* result) {
  result->push_back(kSeries);

  PutFixed64BE(result, r.ref);
  PutFixed64BE(result, r.flushed_txn);
  PutFixed64BE(result, r.log_clean_txn);
}

void series(const std::vector<::tsdb::tsdbutil::RefSeries>& series,
            std::string* result) {
  result->push_back(kSeries);

  for (const ::tsdb::tsdbutil::RefSeries& r : series) {
    PutFixed64BE(result, r.ref);
    PutFixed64BE(result, r.flushed_txn);
    PutFixed64BE(result, r.log_clean_txn);
    const tsdb::label::Labels* lptr = &r.lset;
    if (r.lset_ptr) lptr = r.lset_ptr;
    PutVarint32(result, lptr->size());

    for (const ::tsdb::label::Label& l : *lptr) {
      PutLengthPrefixedSlice(result, l.label);
      PutLengthPrefixedSlice(result, l.value);
    }
  }
}

std::string group(const RefGroup& g) {
  std::string result;
  group(g, &result);
  return result;
}
void group(const RefGroup& g, std::string* result) {
  result->push_back(kGroup);

  PutFixed64BE(result, g.ref);
  PutVarint32(result, g.group_lset.size());
  for (const ::tsdb::label::Label& l : g.group_lset) {
    PutLengthPrefixedSlice(result, l.label);
    PutLengthPrefixedSlice(result, l.value);
  }

  PutVarint32(result, g.individual_lsets.size());
  for (const ::tsdb::label::Labels& lset : g.individual_lsets) {
    PutVarint32(result, lset.size());
    for (const ::tsdb::label::Label& l : lset) {
      PutLengthPrefixedSlice(result, l.label);
      PutLengthPrefixedSlice(result, l.value);
    }
  }
}

std::string samples(const std::vector<RefSample>& s) {
  std::string result;
  samples(s, &result);
  return result;
}
void samples(const std::vector<RefSample>& samples, std::string* result) {
  result->push_back(kSample);

  PutFixed64BE(result, samples[0].ref);
  PutFixed64BE(result, samples[0].logical_id);
  PutFixed64BE(result, samples[0].t);
  PutFixed64BE(result, ::tsdb::base::encode_double(samples[0].v));
  PutFixed64BE(result, samples[0].txn);
  for (int i = 1; i < samples.size(); i++) {
    PutVarint64(result,
                ::tsdb::base::encode_signed_varint(
                    (int64_t)(samples[i].ref) - (int64_t)(samples[i - 1].ref)));
    PutVarint64(result, ::tsdb::base::encode_signed_varint(
                            (int64_t)(samples[i].logical_id) -
                            (int64_t)(samples[i - 1].logical_id)));
    PutVarint64(result, ::tsdb::base::encode_signed_varint(samples[i].t -
                                                           samples[i - 1].t));
    PutFixed64BE(result, ::tsdb::base::encode_double(samples[i].v));
    PutVarint64(result, ::tsdb::base::encode_signed_varint(samples[i].txn -
                                                           samples[i - 1].txn));
  }
}

std::string samples(const std::vector<::tsdb::tsdbutil::RefSample>& s) {
  std::string result;
  samples(s, &result);
  return result;
}
void samples(const std::vector<::tsdb::tsdbutil::RefSample>& samples,
             std::string* result) {
  result->push_back(kSample);

  PutFixed64BE(result, samples[0].ref);
  PutFixed64BE(result, samples[0].logical_id);
  PutFixed64BE(result, samples[0].t);
  PutFixed64BE(result, ::tsdb::base::encode_double(samples[0].v));
  PutFixed64BE(result, samples[0].txn);
  for (int i = 1; i < samples.size(); i++) {
    PutVarint64(result,
                ::tsdb::base::encode_signed_varint(
                    (int64_t)(samples[i].ref) - (int64_t)(samples[i - 1].ref)));
    PutVarint64(result, ::tsdb::base::encode_signed_varint(
                            (int64_t)(samples[i].logical_id) -
                            (int64_t)(samples[i - 1].logical_id)));
    PutVarint64(result, ::tsdb::base::encode_signed_varint(samples[i].t -
                                                           samples[i - 1].t));
    PutFixed64BE(result, ::tsdb::base::encode_double(samples[i].v));
    PutVarint64(result, ::tsdb::base::encode_signed_varint(samples[i].txn -
                                                           samples[i - 1].txn));
  }
}

std::string group_sample(const RefGroupSample& g) {
  std::string result;
  group_sample(g, &result);
  return result;
}
void group_sample(const RefGroupSample& g, std::string* result) {
  result->push_back(kGroupSample);

  PutFixed64BE(result, g.ref);
  if (g.individual_lsets.empty()) {
    result->push_back(1);
    if (g.slots.empty())
      PutVarint32(result, ((uint32_t)(1) << 31) | (uint32_t)(g.v.size()));
    else
      PutVarint32(result, g.slots.size());
    for (size_t i = 0; i < g.slots.size(); i++) PutVarint32(result, g.slots[i]);
  } else {
    result->push_back(2);
    PutVarint32(result, g.individual_lsets.size());
    for (const ::tsdb::label::Labels& lset : g.individual_lsets) {
      PutVarint32(result, lset.size());
      for (const ::tsdb::label::Label& l : lset) {
        PutLengthPrefixedSlice(result, l.label);
        PutLengthPrefixedSlice(result, l.value);
      }
    }
  }
  PutFixed64BE(result, g.t);
  for (size_t i = 0; i < g.v.size(); i++)
    PutFixed64BE(result, ::tsdb::base::encode_double(g.v[i]));
  PutFixed64BE(result, g.txn);
}

std::string flush(const RefFlush& f) {
  std::string result;
  flush(f, &result);

  return result;
}
void flush(const RefFlush& f, std::string* result) {
  result->push_back(kFlush);

  PutFixed64BE(result, f.ref);
  PutFixed64BE(result, f.logical_id);
  PutFixed64BE(result, f.txn);
}

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest), block_offset_(0), num_block_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest),
      block_offset_(dest_length % kBlockSize),
      num_block_(dest_length / kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
      num_block_++;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;

  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    // if (s.ok()) {
    //   s = dest_->Flush();
    // }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

void Writer::flush() { dest_->Flush(); }

RandomWriter::RandomWriter(RandomRWFile* dest)
    : dest_(dest), block_offset_(0), num_block_(0) {
  InitTypeCrc(type_crc_);
}

RandomWriter::~RandomWriter() = default;

Status RandomWriter::AddRecord(uint64_t pos, const Slice& slice, bool write_header) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  block_offset_ = pos % kBlockSize;
  num_block_ = pos / kBlockSize;

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      block_offset_ = 0;
      num_block_++;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, write_header);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;

  } while (s.ok() && left > 0);
  return s;
}

Status RandomWriter::EmitPhysicalRecord(RecordType t, const char* ptr,
                                        size_t length, bool write_header) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s;
  if (write_header)
    s = dest_->Write(num_block_ * kBlockSize + block_offset_,
                          Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Write(num_block_ * kBlockSize + block_offset_ + kHeaderSize,
                     Slice(ptr, length));
    // if (s.ok()) {
    //   s = dest_->Flush();
    // }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

void RandomWriter::flush() { dest_->Flush(); }

}  // namespace log
}  // namespace leveldb
