#ifndef CHAINSERIES_H
#define CHAINSERIES_H

#include "label/Label.hpp"
#include "querier/QuerierUtils.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

// chainedSeries implements a series for a list of time-sorted series.
// They all must have the same labels.
//
// NOTICE
// Never pass a temporary variable to it
class ChainSeries : public SeriesInterface {
 private:
  std::shared_ptr<Series> series;

 public:
  ChainSeries(const std::shared_ptr<Series> &series);

  virtual const label::Labels &labels() const override;

  virtual uint64_t tsid() const override;

  virtual std::unique_ptr<SeriesIteratorInterface> iterator() override;
};

}  // namespace querier
}  // namespace tsdb

#endif