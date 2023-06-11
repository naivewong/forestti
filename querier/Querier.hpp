#ifndef QUERIER_H
#define QUERIER_H

#include <vector>

#include "block/IndexReaderInterface.hpp"
#include "querier/QuerierInterface.hpp"

namespace tsdb {
namespace querier {

class Querier : public QuerierInterface {
 private:
  std::vector<std::shared_ptr<QuerierInterface>> queriers;
  std::shared_ptr<block::IndexReaderInterface> ir_;

 public:
  Querier() = default;
  Querier(const std::initializer_list<std::shared_ptr<QuerierInterface>> &list);
  Querier(const std::vector<std::shared_ptr<QuerierInterface>> &queriers,
          const std::shared_ptr<block::IndexReaderInterface> &ir);

  std::shared_ptr<SeriesSetInterface> select(
      const std::vector<std::shared_ptr<label::MatcherInterface>> &l) const;

  // LabelValues returns all potential values for a label name.
  std::vector<std::string> label_values(const std::string &label) const;

  // label_names returns all the unique label names present in the block in
  // sorted order.
  std::vector<std::string> label_names() const;

  error::Error error() const;
};

}  // namespace querier
}  // namespace tsdb

#endif