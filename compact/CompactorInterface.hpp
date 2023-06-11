#ifndef COMPACTORINTERFACE_H
#define COMPACTORINTERFACE_H

#include <deque>

#include "base/Error.hpp"
#include "block/BlockInterface.hpp"
#include "block/BlockUtils.hpp"

namespace tsdb {
namespace compact {

class CompactorInterface {
 public:
  // Plan returns a set of directories that can be compacted concurrently.
  // The directories can be overlapping.
  // Results returned when compactions are in progress are undefined.
  virtual std::pair<std::deque<std::string>, error::Error> plan(
      const std::string &dir) = 0;

  // Write persists a Block into a directory.
  // No Block is written when resulting Block has 0 samples, and returns empty
  // ulid.ULID{}.
  virtual std::pair<ulid::ULID, error::Error> write(
      const std::string &dest, const std::shared_ptr<block::BlockInterface> &b,
      int64_t min_time, int64_t max_time,
      const std::shared_ptr<block::BlockMeta> &parent) = 0;

  // Compact runs compaction against the provided directories. Must
  // only be called concurrently with results of Plan().
  // Can optionally pass a list of already open blocks,
  // to avoid having to reopen them.
  // When resulting Block has 0 samples
  //  * No block is written.
  //  * The source dirs are marked Deletable.
  //  * Returns empty ulid.ULID{}.
  virtual std::pair<ulid::ULID, error::Error> compact(
      const std::string &dest, const std::deque<std::string> &dirs,
      const std::shared_ptr<block::Blocks> &open) = 0;
  virtual error::Error error() const = 0;
};

}  // namespace compact
}  // namespace tsdb

#endif