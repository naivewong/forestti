#ifndef QUERIERINTERFACE_H
#define QUERIERINTERFACE_H

#include <deque>
#include <memory>
#include <vector>

#include "base/Error.hpp"
#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"
#include "label/MatcherInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace querier {

class QuerierInterface {
 public:
  // Return nullptr when no series match.
  virtual std::unique_ptr<SeriesSetInterface> select(
      const std::vector<label::MatcherInterface *> &l) const {
    return nullptr;
  }
  virtual std::unique_ptr<SeriesSetInterface> select(
      const std::vector<label::MatcherInterface *> &l,
      std::unique_ptr<index::PostingsInterface> &&p) const {
    return nullptr;
  }

  // LabelValues returns all SORTED values for a label name.
  virtual std::vector<std::string> label_values(const std::string &s) const = 0;

  // label_values_for returns all potential values for a label name.
  // under the constraint of another label.
  // virtual std::deque<boost::string_ref> label_values_for(const std::string &
  // s, const label::Label & label) const=0;

  // label_names returns all the unique label names present in the block in
  // sorted order.
  virtual std::vector<std::string> label_names() const = 0;

  virtual error::Error error() const = 0;
  virtual ~QuerierInterface() = default;

  virtual bool contains_global_index() const { return false; }
};

}  // namespace querier
}  // namespace tsdb

#endif