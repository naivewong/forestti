#ifndef SERIESINTERFACE_H
#define SERIESINTERFACE_H

#include "label/Label.hpp"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

class SeriesInterface {
 public:
  virtual const label::Labels &labels() const = 0;
  virtual uint64_t tsid() const { return 0; }

  virtual std::unique_ptr<SeriesIteratorInterface> iterator() = 0;

  virtual std::unique_ptr<SeriesIteratorInterface> chain_iterator() {
    return nullptr;
  }

  virtual ~SeriesInterface() = default;
};

}  // namespace querier
}  // namespace tsdb

#endif