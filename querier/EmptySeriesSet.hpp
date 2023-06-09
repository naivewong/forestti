#ifndef EMPTYSERIESSET_H
#define EMPTYSERIESSET_H

#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace querier {

class EmptySeriesSet : public SeriesSetInterface {
 public:
  bool next() const { return false; }
  std::shared_ptr<SeriesInterface> at() {
    return std::shared_ptr<SeriesInterface>();
  }
  bool error() const { return true; }
};

}  // namespace querier
}  // namespace tsdb

#endif