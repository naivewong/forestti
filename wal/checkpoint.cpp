#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/range/iterator_range.hpp>
// #include <iostream>

#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/checkpoint.hpp"

namespace tsdb {
namespace wal {

const std::string CHECKPOINT_PREFIX = "checkpoint.";

// last_checkpoint returns the directory name and index of the most recent
// checkpoint. If dir does not contain any checkpoints, ErrNotFound is returned.
std::pair<std::pair<std::string, int>, error::Error> last_checkpoint(
    const std::string &dir) {
  std::string r;
  int idx = -1;

  boost::filesystem::path p(dir);
  if (!boost::filesystem::exists(p) || !boost::filesystem::is_directory(p))
    return {{"", 0}, error::Error("dir not existed")};
  for (auto const &entry : boost::make_iterator_range(
           boost::filesystem::directory_iterator(p), {})) {
    if (boost::filesystem::is_directory(entry.path()) &&
        entry.path().filename().string().length() >=
            CHECKPOINT_PREFIX.length() &&
        entry.path().filename().string().substr(
            0, CHECKPOINT_PREFIX.length()) == CHECKPOINT_PREFIX) {
      std::string temp =
          entry.path().filename().string().substr(CHECKPOINT_PREFIX.length());
      if (tsdbutil::is_number(temp)) {
        int temp_idx = std::stoi(temp);
        if (temp_idx > idx) {
          idx = temp_idx;
          r = entry.path().string();
        }
      }
    }
  }
  if (idx == -1)
    return {{"", 0}, error::Error("not found")};
  else
    return {{r, idx}, error::Error()};
}

// delete_checkpoints deletes all checkpoints in a directory below a given
// index.
error::Error delete_checkpoints(const std::string &dir, int max_index) {
  std::deque<std::string> rms;

  boost::filesystem::path p(dir);
  if (!boost::filesystem::exists(p) || !boost::filesystem::is_directory(p))
    return error::Error(std::string("dir not existed"));
  for (auto const &entry : boost::make_iterator_range(
           boost::filesystem::directory_iterator(p), {})) {
    if (boost::filesystem::is_directory(entry.path()) &&
        entry.path().filename().string().length() >=
            CHECKPOINT_PREFIX.length() &&
        entry.path().filename().string().substr(
            0, CHECKPOINT_PREFIX.length()) == CHECKPOINT_PREFIX) {
      std::string temp =
          entry.path().filename().string().substr(CHECKPOINT_PREFIX.length());
      if (tsdbutil::is_number(temp)) {
        int temp_idx = std::stoi(temp);
        if (temp_idx < max_index) {
          rms.push_back(entry.path().string());
        }
      }
    }
  }
  for (const std::string &s : rms) boost::filesystem::remove_all(s);
  return error::Error();
}

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
    int64_t mint) {
  CheckpointStats stats;
  std::deque<SegmentRange> seg_ranges;
  std::pair<std::pair<std::string, int>, error::Error> lp =
      last_checkpoint(wal->dir());
  if (lp.second && lp.second != "not found")
    return {CheckpointStats(), error::wrap(lp.second, "find last checkpoint")};
  int last = lp.first.second + 1;
  if (!lp.second) {
    if (from > last)
      return {CheckpointStats(),
              error::Error("unexpected gap to last checkpoint. expected:" +
                           std::to_string(last) +
                           ", requested:" + std::to_string(from))};
    // Ignore WAL files below the checkpoint. They shouldn't exist to begin
    // with.
    from = last;
    seg_ranges.emplace_back(lp.first.first, -1, -1);
  }
  seg_ranges.emplace_back(wal->dir(), from, to);
  SegmentReader reader(seg_ranges);
  if (reader.error())
    return {CheckpointStats(),
            error::wrap(reader.error(), "create segment reader")};

  std::string cpdir = tsdbutil::filepath_join(
      wal->dir(), (boost::format(CHECKPOINT_PREFIX + "%06d") % to).str());
  std::string cpdirtmp = cpdir + ".tmp";

  {
    WAL cp_wal(cpdirtmp, wal->pool());
    if (cp_wal.error())
      return {CheckpointStats(),
              error::wrap(cp_wal.error(), "open checkpoint")};

    std::vector<tsdbutil::RefSeries> series;
    std::vector<tsdbutil::RefSample> samples;
    std::vector<tsdbutil::Stone> stones;

    int count = 0;
    std::vector<std::vector<uint8_t>> recs;
    while (reader.next()) {
      series.clear();
      samples.clear();
      stones.clear();

      std::pair<uint8_t *, int> rec = reader.record();
      tsdbutil::RECORD_ENTRY_TYPE type =
          tsdbutil::RecordDecoder::type(rec.first, rec.second);
      if (type == tsdbutil::RECORD_SERIES) {
        error::Error err =
            tsdbutil::RecordDecoder::series(rec.first, rec.second, series);
        if (err) return {CheckpointStats(), error::wrap(err, "decode series")};
        stats.total_series += series.size();
        int rm_count = 0;
        auto it = series.begin();
        while (it != series.end()) {
          if (!keep(it->ref)) {
            ++rm_count;
            it = series.erase(it);
          } else
            ++it;
        }
        if (!series.empty()) {
          recs.push_back(std::vector<uint8_t>());
          tsdbutil::RecordEncoder::series(series, recs.back());
          count += recs.back().size();
        }
        stats.dropped_series += rm_count;
      } else if (type == tsdbutil::RECORD_SAMPLES) {
        error::Error err =
            tsdbutil::RecordDecoder::samples(rec.first, rec.second, samples);
        if (err) return {CheckpointStats(), error::wrap(err, "decode samples")};
        stats.total_samples += samples.size();
        int rm_count = 0;
        auto it = samples.begin();
        while (it != samples.end()) {
          if (!keep(it->ref) || it->t < mint) {
            ++rm_count;
            it = samples.erase(it);
          } else
            ++it;
        }
        if (!samples.empty()) {
          recs.push_back(std::vector<uint8_t>());
          tsdbutil::RecordEncoder::samples(samples, recs.back());
          count += recs.back().size();
        }
        stats.dropped_samples += rm_count;
      } else if (type == tsdbutil::RECORD_TOMBSTONES) {
        error::Error err =
            tsdbutil::RecordDecoder::tombstones(rec.first, rec.second, stones);
        if (err)
          return {CheckpointStats(), error::wrap(err, "decode tombstones")};
        stats.total_tombstones += stones.size();
        int rm_count = 0;
        auto it = stones.begin();
        while (it != stones.end()) {
          if (!keep(it->ref)) {
            ++rm_count;
            it = stones.erase(it);
            continue;
          }
          bool cover = false;
          for (auto const &itvl : it->itvls) {
            if (itvl.max_time >= mint) {
              cover = true;
              break;
            }
          }
          if (!cover) {
            ++rm_count;
            it = stones.erase(it);
          } else
            ++it;
        }
        if (!stones.empty()) {
          recs.push_back(std::vector<uint8_t>());
          tsdbutil::RecordEncoder::tombstones(stones, recs.back());
          count += recs.back().size();
        }
        stats.dropped_tombstones += rm_count;
      } else
        return {CheckpointStats(), error::Error("invalid record type")};

      if (count > 1 * 1024 * 1024) {
        error::Error err = cp_wal.log(recs);
        if (err) return {CheckpointStats(), error::wrap(err, "flush records")};
        count = 0;
        recs.clear();
      }
    }
    // If we hit any corruption during checkpointing, repairing is not an
    // option. The head won't know which series records are lost.
    error::Error err = cp_wal.log(recs);
    if (err) return {CheckpointStats(), error::wrap(err, "flush records")};
  }
  boost::filesystem::rename(cpdirtmp, cpdir);

  return {stats, error::Error()};
}

}  // namespace wal
}  // namespace tsdb