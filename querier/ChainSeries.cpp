#include "querier/ChainSeries.hpp"

#include "querier/ChainSeriesIterator.hpp"

namespace tsdb {
namespace querier {

// chainedSeries implements a series for a list of time-sorted series.
// They all must have the same labels.
//
// NOTICE
// Never pass a temporary variable to it
ChainSeries::ChainSeries(const std::shared_ptr<Series> &series)
    : series(series) {}

const label::Labels &ChainSeries::labels() const {
  for (int i = 0; i < series->size(); i++) {
    if (!series->at(i)->labels().empty()) return series->at(i)->labels();
  }
  return {};
}

uint64_t ChainSeries::tsid() const {
  if (series->size() > 0) return series->at(0)->tsid();
  return 0;
}

std::unique_ptr<SeriesIteratorInterface> ChainSeries::iterator() {
  return std::unique_ptr<SeriesIteratorInterface>(
      new ChainSeriesIterator(series));
}

}  // namespace querier
}  // namespace tsdb