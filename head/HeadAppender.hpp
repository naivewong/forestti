#pragma once

#include <iostream>
#include <vector>

#include "base/TimeStamp.hpp"
#include "db/AppenderInterface.hpp"
#include "head/Head.hpp"
#include "head/HeadUtils.hpp"
#include "leveldb/db/log_format.h"
#include "leveldb/db/log_writer.h"
#include "mem/go_art.h"
#include "tsdbutil/RecordEncoder.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace head {

class HeadAppender : public db::AppenderInterface {
 private:
  Head* head;
  leveldb::DB* db;
  int64_t min_time;
  int64_t max_time;

  std::vector<tsdbutil::RefSeries> series;
  std::vector<tsdbutil::RefSample> samples;

  std::string rec_buf;

 public:
  HeadAppender(Head* head, leveldb::DB* db) : head(head), db(db) {}

  // return logical id.
  virtual std::pair<uint64_t, leveldb::Status> add(const label::Labels& lset, int64_t t,
                                           double v, uint64_t epoch = 0) override {
    std::pair<MemSeries*, bool> s = head->get_or_create(lset, epoch);
    if (s.second) series.emplace_back(s.first->ref, lset, s.first);

    return std::make_pair(s.first->logical_id,
                          add_fast(s.first->logical_id, t, v));
  }

  virtual std::pair<uint64_t, leveldb::Status> add(label::Labels&& lset, int64_t t,
                                           double v, uint64_t epoch = 0) override {
    std::pair<MemSeries*, bool> s = head->get_or_create(std::move(lset), epoch);
    if (s.second) series.emplace_back(s.first->ref, &s.first->labels, s.first);

    return std::make_pair(s.first->logical_id,
                          add_fast(s.first->logical_id, t, v));
  }

  virtual leveldb::Status add_fast(uint64_t logical_id, int64_t t, double v) override {
    int64_t txn;
    MemSeries* ms;
    uint64_t series_pointer = head->indirection_manager_.read_slot(logical_id);
  AGAIN:
    if ((series_pointer >> 63) == 0)
      ms = (MemSeries*)((uintptr_t)series_pointer);
    else
      ms = head->reload_mem_series(logical_id, series_pointer);
    uint32_t version;
    if (!mem::read_lock_or_restart(ms, &version)) goto AGAIN;
    txn = ++ms->flushed_txn_;
    if (!mem::read_unlock_or_restart(ms, version)) goto AGAIN;

    samples.emplace_back(ms->ref, t, v, txn);
    samples.back().logical_id = logical_id;
    return leveldb::Status::OK();
  }

  virtual leveldb::Status commit(bool release_labels = false) override {
    leveldb::Status s = log(release_labels);
    if (!s.ok()) return leveldb::Status::IOError("log");

    uint32_t version;
    for (tsdbutil::RefSample& s : samples) {
      MemSeries* ms;
    AGAIN:
      uint64_t series_pointer =
          head->indirection_manager_.read_slot(s.logical_id);
      if ((series_pointer >> 63) == 0)
        ms = (MemSeries*)((uintptr_t)series_pointer);
      else
        ms = head->reload_mem_series(s.logical_id, series_pointer);

      // The MemSeries may be gc in Head::memseries_gc().
      if (!mem::read_lock_or_restart(ms, &version)) goto AGAIN;
      ms->lock();
      ms->append(db, s.t, s.v, s.txn);
      ms->unlock();
      if (!mem::read_unlock_or_restart(ms, version)) goto AGAIN;
    }

    series.clear();
    samples.clear();
    return leveldb::Status::OK();
  }

  virtual leveldb::Status rollback() override {
    // Series are created in the head memory regardless of rollback. Thus we
    // have to log them to the WAL in any case.
    samples.clear();
    return log();
  }

  leveldb::Status log(bool rl = false) {
    if (!series.empty() && head->tags_log_writer_) {
      ::leveldb::Status s;
      head->tags_log_lock_.lock();
      // NOTE(Alec): we do not group series together for future pointer
      // swizzling.
      for (size_t i = 0; i < series.size(); i++) {
        series[i].series_ptr->lock();
        series[i].series_ptr->log_pos =
            ((uint64_t)(head->cur_index_log_seq_.load()) << 32) |
            head->tags_log_writer_->write_off();
        if (rl)
          series[i].series_ptr->release_labels();
        series[i].series_ptr->unlock();
        rec_buf.clear();
        ::leveldb::log::series(series[i], &rec_buf);
        s = head->tags_log_writer_->AddRecord(rec_buf);
        if (!s.ok()) {
          head->tags_log_lock_.unlock();
          return s;
        }
      }

      s = head->try_extend_tags_logs();
      head->tags_log_lock_.unlock();
      if (!s.ok()) return s;
    }
    if (!samples.empty() && head->samples_log_writer_ && !head->no_log()) {
      head->samples_log_lock_.lock();
      rec_buf.clear();
      ::leveldb::log::samples(samples, &rec_buf);
      ::leveldb::Status s = head->samples_log_writer_->AddRecord(rec_buf);
      if (!s.ok()) {
        head->samples_log_lock_.unlock();
        return s;
      }

      s = head->try_extend_samples_logs();
      head->samples_log_lock_.unlock();
      if (!s.ok()) return s;
    }
    return leveldb::Status::OK();
  }
};

}  // namespace head
}  // namespace tsdb
