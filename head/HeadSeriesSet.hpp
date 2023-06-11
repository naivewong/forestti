#pragma once

#include "base/Error.hpp"
#include "chunk/XORChunk.hpp"
#include "head/Head.hpp"
#include "label/MatcherInterface.hpp"
#include "querier/QuerierInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesIteratorInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace head {

class HeadIterator : public ::tsdb::querier::SeriesIteratorInterface {
 private:
  chunk::XORChunk chunk_;
  std::unique_ptr<chunk::XORIterator> iter_;
  mutable bool end_;
  int64_t min_time_;
  int64_t max_time_;
  mutable bool init_;

 public:
  HeadIterator(std::string* s, int64_t mint, int64_t maxt);
  bool seek(int64_t t) const;
  std::pair<int64_t, double> at() const;
  bool next() const;
  bool error() const { return end_; }
};

// class HeadSeries: public ::tsdb::querier::SeriesInterface {
// private:
//   HeadType* head_;
//   uint64_t tsid_;
//   mutable ::tsdb::label::Labels lset_;
//   mutable std::string chunk_contents_;
//   mutable bool init_;
//   mutable bool err_;
//   int64_t min_time_;
//   int64_t max_time_;

// public:
//   HeadSeries(HeadType* head, uint64_t id, int64_t mint, int64_t maxt)
//       : head_(head), tsid_(id), init_(false), err_(false), min_time_(mint),
//       max_time_(maxt) {}

//   void init() const {
//     err_ = !head_->series(tsid_, lset_, &chunk_contents_);
//   }

//   const ::tsdb::label::Labels &labels() const {
//     if (!init_)
//       init();

//     return lset_;
//   }
//   std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> iterator() {
//     if (!init_)
//       init();

//     if (err_)
//       return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(new
//       ::tsdb::querier::EmptySeriesIterator());
//     return std::unique_ptr<::tsdb::querier::SeriesIteratorInterface>(new
//     HeadIterator(&chunk_contents_, min_time_, max_time_));
//   }

//   querier::SeriesType type() { return querier::kTypeSeries; }
//   bool next() { return false; }
// };

// class HeadSeriesSet: public ::tsdb::querier::SeriesSetInterface {
// private:
//   HeadType* head_;
//   std::unique_ptr<::tsdb::index::PostingsInterface> p_;
//   ::tsdb::error::Error err_;

//   mutable uint64_t cur_;
//   int64_t min_time_;
//   int64_t max_time_;
//   std::vector<::tsdb::label::MatcherInterface*> matchers_;

// public:
//   HeadSeriesSet(HeadType* head, const
//   std::vector<::tsdb::label::MatcherInterface*>& l, int64_t mint, int64_t
//   maxt);

//   bool next() const;
//   std::unique_ptr<::tsdb::querier::SeriesInterface> at();
//   uint64_t current_tsid() { return cur_; }
//   bool error() const { return err_; }
// };

// class HeadQuerier: public ::tsdb::querier::QuerierInterface {
// private:
//   HeadType* head_;
//   int64_t min_time_;
//   int64_t max_time_;

// public:
//   HeadQuerier(HeadType* head, int64_t mint, int64_t maxt): head_(head),
//   min_time_(mint), max_time_(maxt) {}

//   std::unique_ptr<::tsdb::querier::SeriesSetInterface> select(
//       const std::vector<::tsdb::label::MatcherInterface*>& l)
//       const {
//     return std::unique_ptr<::tsdb::querier::SeriesSetInterface>(new
//     HeadSeriesSet(head_, l, min_time_, max_time_));
//   }

//   std::vector<std::string> label_values(const std::string &s) const {
//     return head_->label_values(s);
//   }

//   std::vector<std::string> label_names() const {
//     return head_->label_names();
//   }

//   ::tsdb::error::Error error() const { return ::tsdb::error::Error(); }
// };

}  // namespace head.
}  // namespace tsdb.