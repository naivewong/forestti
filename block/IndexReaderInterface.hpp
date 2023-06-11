#ifndef INDEXREADERINTERFACE_H
#define INDEXREADERINTERFACE_H

#include <stdint.h>

#include <boost/function.hpp>
#include <deque>
#include <initializer_list>
#include <set>
// #include <boost/utility/string_ref.hpp>

#include "chunk/ChunkMeta.hpp"
#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace block {

class SerializedTuples;

class IndexReaderInterface {
 public:
  virtual std::set<std::string> symbols() { return {}; }
  virtual const std::deque<std::string> &symbols_deque() const { return {}; }

  // Return empty SerializedTuples when error
  virtual std::vector<std::string> label_values(
      const std::initializer_list<std::string> &names) {
    return {};
  }
  virtual std::vector<std::string> label_values(const std::string &name) {
    return {};
  }
  virtual std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string &name, const std::string &value) {
    return std::make_pair(nullptr, false);
  }

  virtual bool series(uint64_t ref, label::Labels &lset,
                      std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
    return false;
  }
  virtual bool series(uint64_t ref, label::Labels &lset,
                      const std::shared_ptr<chunk::ChunkMeta> &chunk) {
    return false;
  }
  virtual bool series(uint64_t ref,
                      std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) {
    return false;
  }

  virtual std::vector<std::string> label_names() { return {}; }

  virtual bool error() = 0;
  virtual uint64_t size() = 0;

  virtual std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface> &&p) {
    return nullptr;
  }

  virtual std::unique_ptr<index::PostingsInterface> sorted_id_postings(
      std::unique_ptr<index::PostingsInterface> &&p) {
    return nullptr;
  }

  virtual ~IndexReaderInterface() = default;
};

}  // namespace block
}  // namespace tsdb

#endif