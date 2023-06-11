#include "querier/Querier.hpp"

#include <set>

#include "base/Logging.hpp"
#include "index/VectorPostings.hpp"
#include "querier/EmptySeriesSet.hpp"
#include "querier/MergedSeriesSet.hpp"

namespace tsdb {
namespace querier {

Querier::Querier(
    const std::initializer_list<std::shared_ptr<QuerierInterface>> &list)
    : queriers(list.begin(), list.end()) {}
Querier::Querier(const std::vector<std::shared_ptr<QuerierInterface>> &queriers,
                 const std::shared_ptr<block::IndexReaderInterface> &ir)
    : queriers(queriers), ir_(ir) {}

std::shared_ptr<SeriesSetInterface> Querier::select(
    const std::vector<std::shared_ptr<label::MatcherInterface>> &l) const {
  std::shared_ptr<std::vector<uint64_t>> shared_vec =
      std::make_shared<std::vector<uint64_t>>();
  auto p = postings_for_matchers(ir_, l);
  if (!p.second) {
    // LOG_DEBUG << "postings_for_matchers not found";
    return nullptr;
  }
  while (p.first->next()) shared_vec->push_back(p.first->at());

  if (queriers.size() == 1)
    return queriers[0]->select(
        l, std::unique_ptr<index::PostingsInterface>(
               new index::SharedVectorPostings(shared_vec)));
  std::shared_ptr<SeriesSets> ss(new SeriesSets());
  for (auto const &querier : queriers) {
    auto i =
        querier->select(l, std::unique_ptr<index::PostingsInterface>(
                               new index::SharedVectorPostings(shared_vec)));
    if (i) ss->push_back(i);
  }
  if (!ss->empty()) {
    // LOG_INFO << "Create MergedSeriesSet, num of SeriesSetInterface: " <<
    // ss->size();
    return std::make_shared<MergedIDSeriesSet>(ss);
  } else
    return nullptr;
}

// LabelValues returns all potential values for a label name.
std::vector<std::string> Querier::label_values(const std::string &label) const {
  std::set<std::string> s;
  for (auto const &querier : queriers) {
    std::vector<std::string> temp = querier->label_values(label);
    s.insert(temp.begin(), temp.end());
  }
  return std::vector<std::string>(s.begin(), s.end());
}

// label_names returns all the unique label names present in the block in sorted
// order.
std::vector<std::string> Querier::label_names() const {
  std::set<std::string> s;
  for (auto const &querier : queriers) {
    std::vector<std::string> temp = querier->label_names();
    s.insert(temp.begin(), temp.end());
  }
  return std::vector<std::string>(s.begin(), s.end());
}

error::Error Querier::error() const {
  std::string err;
  for (auto const &q : queriers) err += q->error().error();
  return error::Error(err);
}

}  // namespace querier
}  // namespace tsdb