#include "querier/tsdb_querier.h"

#include "base/Logging.hpp"
#include "chunk/XORChunk.hpp"
#include "db/memtable.h"
#include "db/version_set.h"
#include "head/Head.hpp"
#include "head/HeadSeriesSet.hpp"
#include "table/merger.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace tsdb {
namespace querier {

static void DeleteTSSamples(const leveldb::Slice& key, void* value) {
  CachedTSSamples* tf = reinterpret_cast<CachedTSSamples*>(value);
  delete tf->timestamps;
  delete tf->values;
  delete tf;
}

/**********************************************
 *              MemSeriesIterator             *
 **********************************************/
MemSeriesIterator::MemSeriesIterator(leveldb::MemTable* mem, int64_t min_time,
                                     int64_t max_time, uint64_t id,
                                     leveldb::Cache* cache)
    : min_time_(min_time),
      max_time_(max_time),
      tsid_(id),
      t_(nullptr),
      v_(nullptr),
      init_(false),
      cache_(cache),
      handle_(nullptr) {
  iter_.reset(mem->NewIterator());
  if (min_time_ < 0) min_time_ = 0;
}

MemSeriesIterator::~MemSeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void MemSeriesIterator::lookup_cached_ts(const leveldb::Slice& key,
                                                CachedTSSamples** samples,
                                                bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  t_->reserve(32);
  v_->reserve(32);
  *create_ts = true;
}

void MemSeriesIterator::decode_value(const leveldb::Slice& key,
                                     const leveldb::Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);

    lookup_cached_ts(key, &samples, &create_ts);
    if (!create_ts) return;
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
    t_->reserve(32);
    v_->reserve(32);
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  leveldb::Slice tmp_value;
  if (s.data()[0] == 1) {
    uint64_t tmp;
    leveldb::DecodeFixed64BE(s.data() + 1);  // logical_id.
    leveldb::DecodeFixed64BE(s.data() + 9);  // txn.
    tmp_value = leveldb::Slice(s.data() + 17, s.size() - 17);
  } else
    tmp_value = leveldb::Slice(s.data() + 1, s.size() - 1);

  chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()),
                    tmp_value.size());
  auto it = c.xor_iterator();
  while (it->next()) {
    t_->push_back(it->at().first);
    v_->push_back(it->at().second);
  }

  if (cache_ && create_ts)
    handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                             &DeleteTSSamples);
}

bool MemSeriesIterator::seek(int64_t t) const {
  if (err_ || t > max_time_) return false;

  init_ = true;
  std::string key;
  leveldb::encodeKey(&key, tsid_, t);
  leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }
  uint64_t tsid;
  decodeKey(iter_->key(), &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek next2");
        return false;
      }
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> MemSeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool MemSeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    leveldb::encodeKey(&key, tsid_, min_time_);
    leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next seek1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *              L0SeriesIterator              *
 **********************************************/
L0SeriesIterator::L0SeriesIterator(const TSDBQuerier* q, int partition,
                                   uint64_t id, leveldb::Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr) {
  std::vector<leveldb::Iterator*> list;
  q_->current_->AddIterators(leveldb::ReadOptions(), 0,
                             q_->l0_indexes_[partition_], &list, tsid_);

  iter_.reset(leveldb::NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L0SeriesIterator::L0SeriesIterator(const TSDBQuerier* q, uint64_t id,
                                   leveldb::Iterator* it, leveldb::Cache* cache)
    : q_(q),
      tsid_(id),
      iter_(it),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr) {}

L0SeriesIterator::~L0SeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void L0SeriesIterator::lookup_cached_ts(const leveldb::Slice& key,
                                               CachedTSSamples** samples,
                                               bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  t_->reserve(32);
  v_->reserve(32);
  *create_ts = true;
}

void L0SeriesIterator::decode_value(const leveldb::Slice& key,
                                    const leveldb::Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);

    lookup_cached_ts(key, &samples, &create_ts);
    if (!create_ts) return;
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
    t_->reserve(32);
    v_->reserve(32);
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  leveldb::Slice tmp_value;
  if (s.data()[0] == 1) {
    uint64_t tmp;
    leveldb::DecodeFixed64BE(s.data() + 1);  // logical_id.
    leveldb::DecodeFixed64BE(s.data() + 9);  // txn.
    tmp_value = leveldb::Slice(s.data() + 17, s.size() - 17);
  } else
    tmp_value = leveldb::Slice(s.data() + 1, s.size() - 1);

  chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()),
                    tmp_value.size());
  auto it = c.xor_iterator();
  while (it->next()) {
    t_->push_back(it->at().first);
    v_->push_back(it->at().second);
  }

  if (cache_ && create_ts)
    handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                             &DeleteTSSamples);
}

bool L0SeriesIterator::seek(int64_t t) const {
  if (err_ || t > q_->max_time_) return false;

  init_ = true;
  std::string key;
  leveldb::encodeKey(&key, tsid_, t);
  leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }

  uint64_t tsid;
  decodeKey(iter_->key(), &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek next2");
        return false;
      }
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> L0SeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool L0SeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    leveldb::encodeKey(&key, tsid_, q_->min_time_);
    leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next next1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    int64_t st;
    decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > q_->max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= q_->min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > q_->max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > q_->max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *              L1SeriesIterator              *
 **********************************************/
L1SeriesIterator::L1SeriesIterator(const TSDBQuerier* q, int partition,
                                   uint64_t id, leveldb::Cache* cache)
    : q_(q),
      partition_(partition),
      tsid_(id),
      t_(nullptr),
      v_(nullptr),
      sub_idx_(0),
      init_(false),
      cache_(cache),
      handle_(nullptr) {
  std::vector<leveldb::Iterator*> list;
  q_->current_->AddIterators(leveldb::ReadOptions(), 1,
                             q_->l1_indexes_[partition_], &list, tsid_);

  iter_.reset(leveldb::NewMergingIterator(q_->cmp_, &list[0], list.size()));
}

L1SeriesIterator::~L1SeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void L1SeriesIterator::lookup_cached_ts(const leveldb::Slice& key,
                                               CachedTSSamples** samples,
                                               bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  t_->reserve(32);
  v_->reserve(32);
  *create_ts = true;
}

void L1SeriesIterator::decode_value(const leveldb::Slice& key,
                                    const leveldb::Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false, create_times = false, create_vals = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);

    lookup_cached_ts(key, &samples, &create_ts);
    if (!create_ts) return;
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
    t_->reserve(32);
    v_->reserve(32);
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  leveldb::Slice tmp_value;
  if (s.data()[0] == 1) {
    uint64_t tmp;
    leveldb::DecodeFixed64BE(s.data() + 1);  // logical_id.
    leveldb::DecodeFixed64BE(s.data() + 9);  // txn.
    tmp_value = leveldb::Slice(s.data() + 17, s.size() - 17);
  } else
    tmp_value = leveldb::Slice(s.data() + 1, s.size() - 1);

  chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()),
                    tmp_value.size());
  auto it = c.xor_iterator();
  while (it->next()) {
    t_->push_back(it->at().first);
    v_->push_back(it->at().second);
  }

  if (cache_ && create_ts)
    handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                             &DeleteTSSamples);
}

bool L1SeriesIterator::seek(int64_t t) const {
  if (err_ || t > q_->max_time_) return false;

  init_ = true;
  std::string key;
  leveldb::encodeKey(&key, tsid_, t);
  leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
  iter_->Seek(lk.internal_key());
  if (!iter_->Valid()) {
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(lk.internal_key());
  }

  uint64_t tsid;
  decodeKey(iter_->key(), &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      err_.set("error seek next1");
      return false;
    }
    decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek seek2");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> L1SeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool L1SeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    leveldb::encodeKey(&key, tsid_, q_->min_time_);
    leveldb::LookupKey lk(key, leveldb::kMaxSequenceNumber);
    iter_->Seek(lk.internal_key());
    if (!iter_->Valid()) {
      err_.set("error next seek1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(lk.internal_key());
    }

    uint64_t tsid;
    decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next2");
        return false;
      }
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > q_->max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= q_->min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        decodeKey(iter_->key(), &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > q_->max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > q_->max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *            LevelDBSeriesIterator           *
 **********************************************/
LevelDBSeriesIterator::LevelDBSeriesIterator(leveldb::Iterator* iter,
                                             int64_t min_time, int64_t max_time,
                                             uint64_t id, leveldb::Cache* cache)
    : min_time_(min_time < 0 ? 0 : min_time),
      max_time_(max_time),
      tsid_(id),
      iter_(iter),
      t_(nullptr),
      v_(nullptr),
      init_(false),
      cache_(cache),
      handle_(nullptr) {}

LevelDBSeriesIterator::~LevelDBSeriesIterator() {
  if (handle_) cache_->Release(handle_);
  if (!cache_) {
    if (t_) delete t_;
    if (v_) delete v_;
  }
}

inline void LevelDBSeriesIterator::lookup_cached_ts(const leveldb::Slice& key,
                                                    CachedTSSamples** samples,
                                                    bool* create_ts) const {
  handle_ = cache_->Lookup(key);
  if (handle_) {
    *samples = reinterpret_cast<CachedTSSamples*>(cache_->Value(handle_));
    t_ = (*samples)->timestamps;
    v_ = (*samples)->values;
    return;
  }
  *samples = new CachedTSSamples();
  (*samples)->timestamps = new std::vector<int64_t>();
  (*samples)->values = new std::vector<double>();
  t_ = (*samples)->timestamps;
  v_ = (*samples)->values;
  t_->reserve(32);
  v_->reserve(32);
  *create_ts = true;
}

void LevelDBSeriesIterator::decode_value(const leveldb::Slice& key,
                                         const leveldb::Slice& s) const {
  CachedTSSamples* samples = nullptr;
  bool create_ts = false;
  if (cache_) {
    if (handle_)  // We need to release the previous one.
      cache_->Release(handle_);

    lookup_cached_ts(key, &samples, &create_ts);
    if (!create_ts) return;
  } else if (!t_) {
    t_ = new std::vector<int64_t>();
    v_ = new std::vector<double>();
    t_->reserve(32);
    v_->reserve(32);
  } else {
    t_->clear();
    v_->clear();
  }

  // Decode the concatenating bit streams.
  uint32_t tmp_size;
  leveldb::Slice tmp_value;
  if (s.data()[0] == 1) {
    uint64_t tmp;
    leveldb::DecodeFixed64BE(s.data() + 1);  // logical_id.
    leveldb::DecodeFixed64BE(s.data() + 9);  // txn.
    tmp_value = leveldb::Slice(s.data() + 17, s.size() - 17);
  } else
    tmp_value = leveldb::Slice(s.data() + 1, s.size() - 1);

  chunk::XORChunk c(reinterpret_cast<const uint8_t*>(tmp_value.data()),
                    tmp_value.size());
  auto it = c.xor_iterator();
  while (it->next()) {
    t_->push_back(it->at().first);
    v_->push_back(it->at().second);
  }

  if (cache_ && create_ts)
    handle_ = cache_->Insert(key, samples, (samples->timestamps->size()) << 4,
                             &DeleteTSSamples);
}

bool LevelDBSeriesIterator::seek(int64_t t) const {
  if (err_ || t > max_time_) return false;

  init_ = true;
  std::string key;
  leveldb::encodeKey(&key, tsid_, t);
  iter_->Seek(key);
  if (!iter_->Valid()) {
    // std::cout << "LevelDBSeriesIterator error seek seek1\n";
    err_.set("error seek seek1");
    return false;
  }
  iter_->Prev();
  if (!iter_->Valid()) {
    // It is the first element, seek again.
    iter_->Seek(key);
  }
  uint64_t tsid;
  leveldb::decodeKey(iter_->key(), &tsid, nullptr);
  if (tsid != tsid_) {
    iter_->Next();
    if (!iter_->Valid()) {
      // std::cout << "LevelDBSeriesIterator error seek next1\n";
      err_.set("error seek next1");
      return false;
    }
    leveldb::decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) return false;
  }

  sub_idx_ = 0;
  decode_value(iter_->key(), iter_->value());
  while (true) {
    while (sub_idx_ < t_->size()) {
      if (t_->at(sub_idx_) >= t) {
        return true;
      }
      sub_idx_++;
    }
    if (sub_idx_ >= t_->size()) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error seek next2");
        return false;
      }
      leveldb::decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());
    }
  }
  return false;
}

std::pair<int64_t, double> LevelDBSeriesIterator::at() const {
  if (sub_idx_ < t_->size()) return {t_->at(sub_idx_), v_->at(sub_idx_)};
  return {0, 0};
}

bool LevelDBSeriesIterator::next() const {
  if (err_) return false;

  if (!init_) {
    std::string key;
    leveldb::encodeKey(&key, tsid_, min_time_);
    iter_->Seek(key);
    if (!iter_->Valid()) {
      // std::cout << "LevelDBSeriesIterator error next seek1\n";
      err_.set("error next seek1");
      return false;
    }
    iter_->Prev();
    if (!iter_->Valid()) {
      // It is the first element, seek again.
      iter_->Seek(key);
    }

    uint64_t tsid;
    leveldb::decodeKey(iter_->key(), &tsid, nullptr);
    if (tsid != tsid_) {
      iter_->Next();
      if (!iter_->Valid()) {
        // std::cout << "LevelDBSeriesIterator error next next2\n";
        err_.set("error next next2");
        return false;
      }
      leveldb::decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;
    }

    sub_idx_ = 0;
    decode_value(iter_->key(), iter_->value());
    init_ = true;

    // Iterate until exceeding min_time.
    while (true) {
      while (sub_idx_ < t_->size()) {
        if (t_->at(sub_idx_) > max_time_) {
          err_.set("error next2");
          return false;
        } else if (t_->at(sub_idx_) >= min_time_)
          return true;
        sub_idx_++;
      }
      if (sub_idx_ >= t_->size()) {
        iter_->Next();
        if (!iter_->Valid()) {
          err_.set("error next next3");
          return false;
        }
        leveldb::decodeKey(iter_->key(), &tsid, nullptr);
        if (tsid != tsid_) return false;

        sub_idx_ = 0;
        decode_value(iter_->key(), iter_->value());
      }
    }
    return true;
  }

  sub_idx_++;
  if (sub_idx_ >= t_->size()) {
    while (true) {
      iter_->Next();
      if (!iter_->Valid()) {
        err_.set("error next next4");
        return false;
      }
      uint64_t tsid;
      leveldb::decodeKey(iter_->key(), &tsid, nullptr);
      if (tsid != tsid_) return false;

      sub_idx_ = 0;
      decode_value(iter_->key(), iter_->value());

      if (sub_idx_ >= t_->size())
        continue;
      else if (t_->at(sub_idx_) > max_time_) {
        err_.set("error next3");
        return false;
      }
      break;
    }
  } else if (t_->at(sub_idx_) > max_time_) {
    err_.set("error next4");
    return false;
  }
  return true;
}

/**********************************************
 *         ChainedMergeSeriesIterator         *
 **********************************************/
ChainedMergeSeriesIterator::~ChainedMergeSeriesIterator() {
  for (::tsdb::querier::SeriesIteratorInterface* it : iters_) delete it;
}

bool ChainedMergeSeriesIterator::seek(int64_t t) const {
  if (err_) return false;
  while (idx_ < iters_.size()) {
    if (!iters_[idx_]->seek(t)) {
      // if ((err_ = iters_[idx_]->error()))
      //   return false;
      idx_++;
      continue;
    }
    return true;
  }
  return false;
}

bool ChainedMergeSeriesIterator::next() const {
  if (err_ || idx_ >= iters_.size()) return false;
  if (iters_[idx_]->next()) return true;
  // if ((err_ = iters_[idx_]->error())) return false;

  while (++idx_ < iters_.size()) {
    // std::cout << "idx_ " << idx_ << std::endl;
    if (iters_[idx_]->next()) return true;
  }
  return false;
}

/**********************************************
 *                   TSDBSeries                 *
 **********************************************/
const ::tsdb::label::Labels& TSDBSeries::labels() const {
  if (init_) return lset_;
  q_->head_->series(logical_id_, lset_, &head_chunk_contents_);
  init_ = true;
  return lset_;
}

std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>
TSDBSeries::iterator() {
  if (!init_) {
    q_->head_->series(logical_id_, lset_, &head_chunk_contents_);
    init_ = true;
  }

  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters;
  iters.reserve(q_->l1_indexes_.size() + q_->l0_indexes_.size() +
                q_->imms_.size() + 2);
  // iters.push_back(new LevelDBSeriesIterator(iter_, q_->min_time_,
  // q_->max_time_, physical_id_, q_->cache_));

  // Add L1 iterators.
  for (int i = 0; i < q_->l1_indexes_.size(); i++)
    iters.push_back(new L1SeriesIterator(q_, i, physical_id_, q_->cache_));

  // Add L0 iterators.
  // NOTE(Alec): SSTables in the same L0 partition may still be overlapping.
  for (int i = 0; i < q_->l0_indexes_.size(); i++) {
    ::tsdb::querier::MergeSeriesIterator* mit =
        new ::tsdb::querier::MergeSeriesIterator();
    std::vector<leveldb::Iterator*> list;
    q_->current_->AddIterators(leveldb::ReadOptions(), 0, q_->l0_indexes_[i],
                               &list, physical_id_);
    for (leveldb::Iterator* it : list)
      mit->push_back(new L0SeriesIterator(q_, physical_id_, it, q_->cache_));
    iters.push_back(mit);
  }

  // Add mem iterators.
  for (int i = 0; i < q_->imms_.size(); i++)
    iters.push_back(new MemSeriesIterator(
        q_->imms_[i], q_->min_time_, q_->max_time_, physical_id_, q_->cache_));
  if (q_->mem_)
    iters.push_back(new MemSeriesIterator(
        q_->mem_, q_->min_time_, q_->max_time_, physical_id_, q_->cache_));

  // Add head iterator.
  if (!head_chunk_contents_.empty())
    iters.push_back(new ::tsdb::head::HeadIterator(
        &head_chunk_contents_, q_->min_time_, q_->max_time_));

  return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
      new ::tsdb::querier::MergeSeriesIterator(iters));
}

std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>
TSDBSeries::chain_iterator() {
  if (!init_) {
    q_->head_->series(logical_id_, lset_, &head_chunk_contents_);
    init_ = true;
  }

  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters;
  iters.reserve(q_->l1_indexes_.size() + q_->l0_indexes_.size() +
                q_->imms_.size() + 2);
  // iters.push_back(new LevelDBSeriesIterator(iter_, q_->min_time_,
  // q_->max_time_, physical_id_, q_->cache_));

  // Add L1 iterators.
  for (int i = 0; i < q_->l1_indexes_.size(); i++)
    iters.push_back(new L1SeriesIterator(q_, i, physical_id_, q_->cache_));

  // Add L0 iterators.
  // NOTE(Alec): SSTables in the same L0 partition may still be overlapping.
  for (int i = 0; i < q_->l0_indexes_.size(); i++) {
    std::vector<leveldb::Iterator*> list;
    q_->current_->AddIterators(leveldb::ReadOptions(), 0, q_->l0_indexes_[i],
                               &list, physical_id_);
    for (leveldb::Iterator* it : list)
      iters.push_back(new L0SeriesIterator(q_, physical_id_, it, q_->cache_));
  }

  // Add mem iterators.
  for (int i = 0; i < q_->imms_.size(); i++)
    iters.push_back(new MemSeriesIterator(
        q_->imms_[i], q_->min_time_, q_->max_time_, physical_id_, q_->cache_));
  if (q_->mem_)
    iters.push_back(new MemSeriesIterator(
        q_->mem_, q_->min_time_, q_->max_time_, physical_id_, q_->cache_));

  // Add head iterator.
  if (!head_chunk_contents_.empty())
    iters.push_back(new ::tsdb::head::HeadIterator(
        &head_chunk_contents_, q_->min_time_, q_->max_time_));

  return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(
      new ChainedMergeSeriesIterator(iters));
}

/**********************************************
 *                TSDBSeriesSet               *
 **********************************************/
TSDBSeriesSet::TSDBSeriesSet(
    const TSDBQuerier* q,
    const std::vector<::tsdb::label::MatcherInterface*>& l)
    : q_(q), matchers_(l) {
  auto p = q->head_->select(l);
  if (!p) {
    err_.set("error TSDBSeriesSet postings_for_matchers");
    LOG_DEBUG << "error TSDBSeriesSet postings_for_matchers";
    p_.reset();
  } else
    p_ = std::move(p);
}

bool TSDBSeriesSet::next() const {
  if (err_) return false;

  if (!p_->next()) return false;

  logical_id_ = p_->at();
  physical_id_ = q_->head_->physical_id(p_->at());
  return true;
}

std::unique_ptr<::tsdb::querier::SeriesInterface> TSDBSeriesSet::at() {
  if (err_) nullptr;
  return std::unique_ptr<::tsdb::querier::SeriesInterface>(
      new TSDBSeries(q_, logical_id_, physical_id_));
}

/**********************************************
 *                 TSDBQuerier                *
 **********************************************/
TSDBQuerier::TSDBQuerier(leveldb::DB* db, ::tsdb::head::Head* head,
                         int64_t min_time, int64_t max_time,
                         leveldb::Cache* cache)
    : db_(db),
      head_(head),
      need_unref_current_(true),
      min_time_(min_time),
      max_time_(max_time),
      cmp_(db->internal_comparator()),
      cache_(cache) {
  if (min_time_ < 0) min_time_ = 0;
  leveldb::MutexLock l(db_->mutex());
  register_mem_partitions();
  register_disk_partitions();
}

TSDBQuerier::~TSDBQuerier() {
  if (need_unref_current_) {
    leveldb::MutexLock l(db_->mutex());
    current_->Unref();
  }
}

void TSDBQuerier::register_mem_partitions() {
  mem_ = db_->mem();
  imms_ = *db_->imms();
}

void TSDBQuerier::register_disk_partitions() {
  current_ = db_->current();
  current_->Ref();
  current_->OverlappingPartitions(0, min_time_, max_time_, &l0_partitions_,
                                  &l0_indexes_);
  current_->OverlappingPartitions(1, min_time_, max_time_, &l1_partitions_,
                                  &l1_indexes_);
}

std::unique_ptr<::tsdb::querier::SeriesSetInterface> TSDBQuerier::select(
    const std::vector<::tsdb::label::MatcherInterface*>& l) const {
  return std::unique_ptr<::tsdb::querier::SeriesSetInterface>(
      new TSDBSeriesSet(this, l));
}

std::vector<std::string> TSDBQuerier::label_values(const std::string& s) const {
  return head_->label_values(s);
}

std::vector<std::string> TSDBQuerier::label_names() const {
  return head_->label_names();
}

}  // namespace querier
}  // namespace tsdb