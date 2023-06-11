#ifndef WAL_CHECKPOINT_H
#define WAL_CHECKPOINT_H

#include <boost/function.hpp>

#include "wal/WAL.hpp"

namespace tsdb {
namespace wal {

class CheckpointStats {
 public:
  int dropped_series;
  int dropped_samples;
  int dropped_tombstones;
  int total_series;      // Processed series including dropped ones.
  int total_samples;     // Processed samples including dropped ones.
  int total_tombstones;  // Processed tombstones including dropped ones.
  CheckpointStats()
      : dropped_series(0),
        dropped_samples(0),
        dropped_tombstones(0),
        total_series(0),
        total_samples(0),
        total_tombstones(0) {}
};

extern const std::string CHECKPOINT_PREFIX;

// last_checkpoint returns the directory name and index of the most recent
// checkpoint. If dir does not contain any checkpoints, ErrNotFound is returned.
std::pair<std::pair<std::string, int>, error::Error> last_checkpoint(
    const std::string &dir);

// delete_checkpoints deletes all checkpoints in a directory below a given
// index.
error::Error delete_checkpoints(const std::string &dir, int max_index);

// checkpoint creates a compacted checkpoint of segments in range [first, last]
// in the given WAL. It includes the most recent checkpoint if it exists. All
// series not satisfying keep and samples below mint are dropped.
//
// The checkpoint is stored in a directory named checkpoint.N in the same
// segmented format as the original WAL itself.
// This makes it easy to read it through the WAL package and concatenate
// it with the original WAL.
std::pair<CheckpointStats, error::Error> checkpoint(
    WAL *wal, int from, int to, const boost::function<bool(uint64_t)> &keep,
    int64_t mint);

}  // namespace wal
}  // namespace tsdb

#endif