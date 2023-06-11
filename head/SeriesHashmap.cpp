#include "head/SeriesHashmap.hpp"

namespace tsdb {
namespace head {

std::shared_ptr<MemSeries> SeriesHashmap::get(uint64_t hash,
                                              const label::Labels &lset) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    return nullptr;
  else {
    auto it2 = std::find_if(it1->second.begin(), it1->second.end(),
                            [&lset](const std::shared_ptr<MemSeries> &s) {
                              return label::lbs_compare(s->labels, lset) == 0;
                            });
    if (it2 == it1->second.end())
      return nullptr;
    else
      return *it2;
  }
}

void SeriesHashmap::set(uint64_t hash, const std::shared_ptr<MemSeries> &s) {
  auto it1 = map.find(hash);
  if (it1 == map.end())
    map.insert({hash, {s}});
  else {
    auto it2 =
        std::find_if(it1->second.begin(), it1->second.end(),
                     [&s](const std::shared_ptr<MemSeries> &s1) {
                       return label::lbs_compare(s->labels, s1->labels) == 0;
                     });
    if (it2 == it1->second.end())
      it1->second.push_back(s);
    else
      *it2 = s;
  }
}

std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator
SeriesHashmap::del(uint64_t hash, const label::Labels &lset) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if([&lset](const std::shared_ptr<MemSeries> &s) {
      return label::lbs_compare(s->labels, lset) == 0;
    });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

std::unordered_map<uint64_t, std::list<std::shared_ptr<MemSeries>>>::iterator
SeriesHashmap::del(uint64_t hash, uint64_t ref) {
  auto it1 = map.find(hash);
  if (it1 != map.end())
    it1->second.remove_if(
        [&ref](const std::shared_ptr<MemSeries> &s) { return s->ref == ref; });
  if (it1->second.empty()) return map.erase(it1);
  return it1;
}

// void SeriesHashmap::gc(int64_t min_time, std::unordered_set<uint64_t>&
// rm_series, int& rm_chunks) {
//   for (auto& hash_item : map) {
//     auto it = hash_item.second.begin();
//     while (it != hash_item.second.end()) {
//       (*it)->write_lock();
//       rm_chunks += (*it)->truncate_chunk_before(min_time);

//       if (!(*it)->chunks.empty() ||
//           (*it)->pending_commit) {
//         ++it;
//         (*it)->write_unlock();
//         continue;
//       }

//       (*it)->write_unlock();
//       rm_series.insert((*it)->ref);
//       it = hash_item.second.erase(it);
//     }
//   }
// }

}  // namespace head
}  // namespace tsdb