#pragma once

#include <memory>
#include <vector>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "querier/QuerierInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {

namespace head {
class Head;
}

namespace querier {

class TSDBQuerier;

struct CachedTSSamples {
  std::vector<int64_t>* timestamps;
  std::vector<double>* values;
};

class MemSeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  int64_t min_time_;
  int64_t max_time_;
  uint64_t tsid_;
  leveldb::Status s_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<leveldb::Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  leveldb::Cache* cache_;
  mutable leveldb::Cache::Handle* handle_;

  void decode_value(const leveldb::Slice& key, const leveldb::Slice& s) const;
  void lookup_cached_ts(const leveldb::Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;

 public:
  MemSeriesIterator(leveldb::MemTable* mem, int64_t min_time, int64_t max_time,
                    uint64_t id, leveldb::Cache* cache = nullptr);
  ~MemSeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class L0SeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  const TSDBQuerier* q_;
  int partition_;
  uint64_t tsid_;
  int slot_;
  leveldb::Status s_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<leveldb::Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  leveldb::Cache* cache_;
  mutable leveldb::Cache::Handle* handle_;

  void decode_value(const leveldb::Slice& key, const leveldb::Slice& s) const;
  void lookup_cached_ts(const leveldb::Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;

 public:
  L0SeriesIterator(const TSDBQuerier* q, int partition, uint64_t id,
                   leveldb::Cache* cache = nullptr);
  L0SeriesIterator(const TSDBQuerier* q, uint64_t id, leveldb::Iterator* it,
                   leveldb::Cache* cache = nullptr);
  ~L0SeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class L1SeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  const TSDBQuerier* q_;
  int partition_;
  uint64_t tsid_;
  mutable ::tsdb::error::Error err_;

  std::unique_ptr<leveldb::Iterator> iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  leveldb::Cache* cache_;
  mutable leveldb::Cache::Handle* handle_;

  void decode_value(const leveldb::Slice& key, const leveldb::Slice& s) const;
  void lookup_cached_ts(const leveldb::Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;

 public:
  L1SeriesIterator(const TSDBQuerier* q, int partition, uint64_t id,
                   leveldb::Cache* cache = nullptr);
  ~L1SeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class LevelDBSeriesIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  int64_t min_time_;
  int64_t max_time_;
  uint64_t tsid_;
  leveldb::Status s_;
  mutable ::tsdb::error::Error err_;

  leveldb::Iterator* iter_;

  mutable std::vector<int64_t>* t_;
  mutable std::vector<double>* v_;
  mutable int sub_idx_;
  mutable bool init_;

  leveldb::Cache* cache_;
  mutable leveldb::Cache::Handle* handle_;

  void decode_value(const leveldb::Slice& key, const leveldb::Slice& s) const;
  void lookup_cached_ts(const leveldb::Slice& key, CachedTSSamples** samples,
                        bool* create_ts) const;

 public:
  LevelDBSeriesIterator(leveldb::Iterator* iter, int64_t min_time,
                        int64_t max_time, uint64_t id,
                        leveldb::Cache* cache = nullptr);
  ~LevelDBSeriesIterator();
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return err_; }
};

class ChainedMergeSeriesIterator
    : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  std::vector<::tsdb::querier::SeriesIteratorInterface*> iters_;
  mutable int idx_;
  mutable bool err_;

 public:
  ChainedMergeSeriesIterator(
      const std::vector<::tsdb::querier::SeriesIteratorInterface*>& iters)
      : iters_(iters), idx_(0), err_(false) {}
  ~ChainedMergeSeriesIterator();

  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const { return iters_[idx_]->at(); }
  bool next() const;
  bool error() const { return err_; }
};

class TSDBSeries : public ::tsdb::querier::SeriesInterface {
 private:
  const TSDBQuerier* q_;
  // leveldb::Iterator* iter_;
  uint64_t logical_id_;
  uint64_t physical_id_;
  mutable ::tsdb::label::Labels lset_;
  mutable std::string head_chunk_contents_;
  mutable bool init_;

 public:
  TSDBSeries(const TSDBQuerier* q, uint64_t logical_id, uint64_t physical_id)
      : q_(q),
        logical_id_(logical_id),
        physical_id_(physical_id),
        init_(false) {}

  const ::tsdb::label::Labels& labels() const override;
  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> iterator() override;
  std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> chain_iterator()
      override;
};

class TSDBSeriesSet : public ::tsdb::querier::SeriesSetInterface {
 private:
  const TSDBQuerier* q_;
  std::unique_ptr<::tsdb::index::PostingsInterface> p_;
  mutable uint64_t logical_id_;
  mutable uint64_t physical_id_;
  ::tsdb::error::Error err_;
  std::vector<::tsdb::label::MatcherInterface*> matchers_;
  // std::unique_ptr<leveldb::Iterator> iter_;

 public:
  TSDBSeriesSet(const TSDBQuerier* q,
                const std::vector<::tsdb::label::MatcherInterface*>& l);

  bool next() const override;
  std::unique_ptr<::tsdb::querier::SeriesInterface> at() override;
  uint64_t current_tsid() override { return logical_id_; }

  bool error() const override { return err_; }
};

class TSDBQuerier : public ::tsdb::querier::QuerierInterface {
 private:
  friend class MemSeries;
  friend class MemSeriesIterator;
  friend class L0Series;
  friend class L0SeriesIterator;
  friend class L1Series;
  friend class L1SeriesIterator;
  friend class TSDBSeries;
  friend class TSDBSeriesSet;

  mutable leveldb::DB* db_;
  mutable ::tsdb::head::Head* head_;
  mutable leveldb::Version* current_;
  bool need_unref_current_;
  int64_t min_time_;
  int64_t max_time_;
  const leveldb::Comparator* cmp_;
  mutable ::tsdb::error::Error err_;
  leveldb::Status s_;

  leveldb::MemTable* mem_;
  std::vector<leveldb::MemTable*> imms_;

  std::vector<std::pair<int64_t, int64_t>> l0_partitions_;
  std::vector<int> l0_indexes_;
  std::vector<std::pair<int64_t, int64_t>> l1_partitions_;
  std::vector<int> l1_indexes_;

  leveldb::Cache* cache_;

  // Needs to be protected by lock.
  void register_mem_partitions();
  void register_disk_partitions();

 public:
  TSDBQuerier(leveldb::DB* db, ::tsdb::head::Head* head, int64_t min_time,
              int64_t max_time, leveldb::Cache* cache = nullptr);

  ~TSDBQuerier();

  int64_t mint() { return min_time_; }
  int64_t maxt() { return max_time_; }

  // Note(Alec), currently may only support equal matcher.
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> select(
      const std::vector<::tsdb::label::MatcherInterface*>& l) const override;

  std::vector<std::string> label_values(const std::string& s) const override;

  std::vector<std::string> label_names() const override;

  ::tsdb::error::Error error() const override { return err_; }
};

}  // namespace querier
}  // namespace tsdb