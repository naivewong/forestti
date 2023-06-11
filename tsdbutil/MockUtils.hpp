#ifndef MOCKUTIlS_H
#define MOCKUTIlS_H
/*
                This header file defines some mocking classes which are useful
                in unit tests. The mocking class will store the results in
   memory.
*/

#include <stdint.h>

#include <deque>
#include <limits>
#include <set>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "block/BlockInterface.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/ChunkWriterInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "block/IndexWriterInterface.hpp"
#include "chunk/ChunkMeta.hpp"
#include "index/IndexUtils.hpp"
#include "index/ListPostings.hpp"
#include "index/VectorPostings.hpp"
#include "label/Label.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/ListStringTuples.hpp"

namespace tsdb {
namespace tsdbutil {

class Sample {
 public:
  int64_t t;
  double v;
  Sample() : t(0), v(0) {}
  Sample(int64_t t, double v) : t(t), v(v) {}
  Sample(const std::pair<int64_t, double> &p) : t(p.first), v(p.second) {}
  bool operator==(const Sample &s) const { return t == s.t && v == s.v; }
  bool operator!=(const Sample &s) const { return !(*this == s); }
};

class Series {
 public:
  label::Labels lset;
  std::deque<std::shared_ptr<chunk::ChunkMeta>> chunks;
};

class SeriesSamples {
  // This just represent one time series.
 public:
  std::unordered_map<std::string, std::string> lset;
  std::deque<std::deque<Sample>> chunks;

  SeriesSamples() = default;
  SeriesSamples(const std::unordered_map<std::string, std::string> &lset,
                const std::deque<std::deque<Sample>> &chunks)
      : lset(lset), chunks(chunks) {}
  SeriesSamples(const std::unordered_map<std::string, std::string> &lset)
      : lset(lset) {}

  bool operator==(const SeriesSamples &s) const {
    if (label::lbs_compare(label::lbs_from_map(lset),
                           label::lbs_from_map(s.lset)) != 0)
      return false;
    if (chunks.size() != s.chunks.size()) return false;
    for (int i = 0; i < chunks.size(); ++i) {
      if (chunks[i].size() != s.chunks[i].size()) return false;
      for (int j = 0; j < chunks[i].size(); ++j) {
        if (chunks[i][j] != s.chunks[i][j]) return false;
      }
    }
    return true;
  }
};

void print_seriessamples(const SeriesSamples &s);

class MockIndexReader : public block::IndexReaderInterface {
 public:
  std::unordered_map<uint64_t, Series> series_;
  std::unordered_map<std::string, std::deque<std::string>> label_index;
  std::unordered_map<label::Label, std::deque<uint64_t>, label::LabelHasher>
      postings_;
  std::set<std::string> symbols_;
  mutable std::deque<std::string> symbols_deque_;

  std::set<std::string> symbols() { return symbols_; }
  const std::deque<std::string> &symbols_deque() const {
    symbols_deque_.assign(symbols_.cbegin(), symbols_.cend());
    return symbols_deque_;
  }

  // Return empty SerializedTuples when error
  std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(
      const std::initializer_list<std::string> &names) {
    if (names.size() != 1) return nullptr;
    return std::unique_ptr<tsdbutil::StringTuplesInterface>(
        new tsdbutil::ListStringTuples(
            std::vector<std::string>(label_index[*names.begin()].begin(),
                                     label_index[*names.begin()].end())));
  }
  std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string &name, const std::string &value) {
    if (name == label::ALL_POSTINGS_KEYS.label) {
      auto v = new index::VectorPostings();
      std::set<uint64_t> s;
      for (auto &pp : postings_) {
        for (auto &p : pp.second) s.insert(p);
      }
      for (auto p : s) v->push_back(p);
      return {std::unique_ptr<index::PostingsInterface>(v), true};
    }
    return {std::unique_ptr<index::PostingsInterface>(
                new index::ListPostings(postings_[label::Label(name, value)])),
            true};
  }
  bool series(uint64_t ref, label::Labels &lset,
              std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
    if (series_.find(ref) == series_.end()) return false;
    for (auto const &l : series_[ref].lset) lset.push_back(l);
    for (auto const &c : series_[ref].chunks) chunks.push_back(c);
    return true;
  }
  std::deque<std::string> label_names() {
    std::deque<std::string> names;
    for (auto const &p : label_index) names.push_back(p.first);
    std::sort(names.begin(), names.end());
    return names;
  }

  bool error() { return false; }
  uint64_t size() { return 0; }

  std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface> &&p) {
    std::deque<uint64_t> ep;
    while (p->next()) ep.push_back(p->at());
    std::sort(ep.begin(), ep.end(), [this](int i, int j) {
      return label::lbs_compare(this->series_[i].lset, this->series_[j].lset) <
             0;
    });
    auto v = new index::VectorPostings(ep.size());
    for (auto i : ep) v->push_back(i);
    return std::unique_ptr<index::PostingsInterface>(v);
  }
};

class MockChunkReader : public block::ChunkReaderInterface {
 public:
  std::unordered_map<uint64_t, std::shared_ptr<chunk::ChunkInterface>> chunks;

  std::pair<std::shared_ptr<chunk::ChunkInterface>, bool> chunk(uint64_t ref) {
    if (chunks.find(ref) == chunks.end()) return {nullptr, false};
    return {chunks[ref], true};
  }
  bool error() { return false; }
  uint64_t size() { return 0; }
};

class MockIndexWriter : public block::IndexWriterInterface {
 public:
  std::deque<SeriesSamples> series;

  int add_symbols(const std::unordered_set<std::string> &sym) { return 0; }
  int add_series(uint64_t ref, const label::Labels &l,
                 const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
    int i = -1;
    for (int j = 0; j < series.size(); ++j) {
      if (label::lbs_compare(label::lbs_from_map(series[j].lset), l) == 0) {
        i = j;
        break;
      }
    }
    if (i == -1) {
      series.push_back(SeriesSamples(label::lbs_map(l)));
      i = series.size() - 1;
    }

    for (auto const &cm : chunks) {
      std::deque<Sample> samples;

      auto it = cm->chunk->iterator();
      while (it->next()) {
        samples.emplace_back(it->at().first, it->at().second);
      }
      if (it->error()) return -1;
      series[i].chunks.push_back(samples);
    }
    return 0;
  }
  int write_label_index(const std::deque<std::string> &names,
                        const std::deque<std::string> &values) {
    return 0;
  }
  int write_postings(const std::string &name, const std::string &value,
                     const index::PostingsInterface *values) {
    return 0;
  }
};

class NopChunkWriter : public block::ChunkWriterInterface {
 public:
  void write_chunks(
      const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {}
  void close() {}
};

class MockBlockReader : public block::BlockInterface {
 public:
  std::shared_ptr<block::IndexReaderInterface> ir;
  std::shared_ptr<block::ChunkReaderInterface> cr;
  int64_t min_time;
  int64_t max_time;

  MockBlockReader()
      : min_time(std::numeric_limits<int64_t>::max()),
        max_time(std::numeric_limits<int64_t>::min()) {}
  MockBlockReader(const std::shared_ptr<block::IndexReaderInterface> &ir,
                  const std::shared_ptr<block::ChunkReaderInterface> &cr,
                  int64_t min_time, int64_t max_time)
      : ir(ir), cr(cr), min_time(min_time), max_time(max_time) {}

  // index returns an IndexReader over the block's data, succeed or not.
  std::pair<std::shared_ptr<block::IndexReaderInterface>, bool> index() const {
    return {ir, true};
  }

  // chunks returns a ChunkReader over the block's data, succeed or not.
  std::pair<std::shared_ptr<block::ChunkReaderInterface>, bool> chunks() const {
    return {cr, true};
  }

  // tombstones returns a TombstoneReader over the block's deleted data, succeed
  // or not.
  std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
  tombstones() const {
    return {std::shared_ptr<tombstone::TombstoneReaderInterface>(
                new tombstone::MemTombstones()),
            true};
  }

  int64_t MaxTime() const { return max_time; }

  int64_t MinTime() const { return min_time; }

  error::Error error() const { return error::Error(); }
};

std::tuple<std::shared_ptr<block::IndexReaderInterface>,
           std::shared_ptr<block::ChunkReaderInterface>, int64_t, int64_t>
create_idx_chk_readers(std::deque<SeriesSamples> &tc);

}  // namespace tsdbutil
}  // namespace tsdb

#endif