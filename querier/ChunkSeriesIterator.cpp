#include "querier/ChunkSeriesIterator.hpp"

#include <iostream>

#include "chunk/DeleteIterator.hpp"

namespace tsdb {
namespace querier {

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks
// which belong to EXACTLY ONE SERIES.
//
// NOTICE(Alec), remember to always initial err_ to false first, otherwise err_
// can be whatever.
ChunkSeriesIterator::ChunkSeriesIterator(
    const std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks,
    const tombstone::Intervals &intervals, int64_t min_time, int64_t max_time)
    : chunks(chunks),
      i(0),
      min_time(min_time),
      max_time(max_time),
      intervals(intervals),
      err_(false) {
  if (chunks.empty()) {
    err_ = true;
    return;
  }
  std::unique_ptr<chunk::ChunkIteratorInterface> temp =
      chunks.front()->chunk->iterator();
  if (!intervals.empty())
    cur.reset(new chunk::DeleteIterator(std::move(temp), intervals.cbegin(),
                                        intervals.cend()));
  else {
    cur.reset();
    cur = std::move(temp);
  }
}

// This method is not available in ChunkIterator
bool ChunkSeriesIterator::seek(int64_t t) const {
  if (err_) return false;
  if (t > max_time) return false;

  // Seek to the first valid value after t.
  if (t < min_time) t = min_time;

  int last = i;

  while (t > chunks[i]->max_time) {
    if (i == chunks.size() - 1) return false;
    ++i;
  }

  if (last != i) {
    std::unique_ptr<chunk::ChunkIteratorInterface> temp =
        chunks[i]->chunk->iterator();
    if (intervals.size() > 0)
      cur.reset(new chunk::DeleteIterator(std::move(temp), intervals.cbegin(),
                                          intervals.cend()));
    else {
      cur.reset();
      cur = std::move(temp);
    }
  }

  while (cur->next()) {
    if (cur->at().first >= t) return true;
  }
  return false;
}

std::pair<int64_t, double> ChunkSeriesIterator::at() const { return cur->at(); }

bool ChunkSeriesIterator::next() const {
  if (cur->next()) {
    std::pair<int64_t, double> p = cur->at();

    // Compare with min_time
    if (p.first < min_time) {
      if (!seek(min_time)) return false;

      return cur->at().first <= max_time;
    }

    // Compare with max_time
    if (p.first > max_time) return false;

    return true;
  }

  if (cur->error()) {
    err_ = true;
    return false;
  }

  if (i == chunks.size() - 1) return false;

  ++i;
  std::unique_ptr<chunk::ChunkIteratorInterface> temp =
      chunks[i]->chunk->iterator();
  if (intervals.size() > 0)
    cur.reset(new chunk::DeleteIterator(std::move(temp), intervals.cbegin(),
                                        intervals.cend()));
  else {
    cur.reset();
    cur = std::move(temp);
  }

  return next();
}

bool ChunkSeriesIterator::error() const { return err_; }

}  // namespace querier
}  // namespace tsdb