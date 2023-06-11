#include "head/Head.hpp"

#include <sys/wait.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <future>
#include <iostream>
#include <limits>
#include <thread>

#include "base/Logging.hpp"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "head/HeadAppender.hpp"
#include "index/IntersectPostings.hpp"
#include "index/VectorPostings.hpp"
#include "querier/ChunkSeriesIterator.hpp"
#include "querier/QuerierUtils.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/RecordDecoder.hpp"
#include "tsdbutil/RecordEncoder.hpp"
#include "util/mutexlock.h"
#include "wal/checkpoint.hpp"

namespace tsdb {
namespace head {

std::string HEAD_INDEX_LOG_NAME = "head_index";
std::string HEAD_SAMPLES_LOG_NAME = "head_samples";
std::string HEAD_FLUSHES_LOG_NAME = "head_flushes";
size_t MAX_HEAD_SAMPLES_LOG_SIZE = 512 * 1024 * 1024;
uint64_t MEM_TO_DISK_MIGRATION_THRESHOLD = 8lu * 1024lu * 1024lu * 1024lu;

std::string HEAD_INDEX_SNAPSHOT_NAME = "head_index_snapshot";
size_t HEAD_SAMPLES_LOG_CLEANING_THRES = 10;

uint64_t _garbage_counter = 0;
std::atomic<uint64_t> _ms_gc_counter(0);

// TODO: make it thread safe.
void art_int_incr_helper(mem::art_tree* tree, const std::string& key) {
  // void* ptr = mem::art_search(tree, reinterpret_cast<const unsigned
  // char*>(key.c_str()), key.size()); if (ptr)
  //   mem::art_insert(tree, reinterpret_cast<const unsigned
  //   char*>(key.c_str()), key.size(), (void*)((uintptr_t)(ptr) + 1));
  // else
  mem::art_insert(tree, reinterpret_cast<const unsigned char*>(key.c_str()),
                  key.size(), (void*)(1));
}

void art_int_decr_helper(mem::art_tree* tree, const std::string& key) {
  void* ptr = mem::art_search(
      tree, reinterpret_cast<const unsigned char*>(key.c_str()), key.size());
  if (ptr) {
    if ((uintptr_t)(ptr) > uintptr_t(1))
      mem::art_insert(tree, reinterpret_cast<const unsigned char*>(key.c_str()),
                      key.size(), (void*)((uintptr_t)(ptr)-1));
    else
      mem::art_delete(tree, reinterpret_cast<const unsigned char*>(key.c_str()),
                      key.size());
  }
}

Head::Head(const std::string& dir, const std::string& snapshot_dir,
           leveldb::DB* db, bool sync)
    : cur_index_log_seq_(0),
      cur_samples_log_seq_(0),
      cur_flushes_log_seq_(0),
      active_samples_logs_(0),
      last_series_id(0),
      hash_shards_(STRIPE_SIZE),
      dir_(dir),
      db_(db),
      sync_api_(sync),
      concurrency_enabled_(false),
      mem_to_disk_migration_threshold_(MEM_TO_DISK_MIGRATION_THRESHOLD),
      migration_enabled_(false),
      no_log_(false) {
  min_time.getAndSet(std::numeric_limits<int64_t>::max());
  max_time.getAndSet(std::numeric_limits<int64_t>::min());

  hash_locks_ =
      (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock) * STRIPE_SIZE);
  for (int i = 0; i < STRIPE_SIZE; i++) hash_locks_[i].reset();

  mem::art_tree_init(&symbols_);

  posting_list = std::unique_ptr<mem::InvertedIndex>(
      new mem::InvertedIndex(dir, snapshot_dir));
  if (!sync_api_) posting_list->start_all_stages();

  std::unordered_map<uint64_t, MemSeries*> id_map;
  leveldb::Status s;
  if (snapshot_dir.empty()) {
    s = recover_index_from_log(&id_map);
    if (!s.ok()) {
      LOG_ERROR << s.ToString();
      abort();
    }
  } else {
    indirection_manager_.recover_from_snapshot(snapshot_dir);
    s = enable_tags_logs();
    if (!s.ok()) LOG_ERROR << s.ToString();
    recover_memseries_from_indirection(&id_map);
  }
  s = recover_samples_from_log(&id_map);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    abort();
  }
}

Head::~Head() {
  if (!sync_api_) posting_list->stop_all_stages();
  for (size_t i = 0; i < tags_log_readers_.size(); i++)
    delete tags_log_readers_[i];
  for (size_t i = 0; i < tags_log_writers_.size(); i++)
    delete tags_log_writers_[i];
  for (size_t i = 0; i < tags_log_files_.size(); i++) delete tags_log_files_[i];

  auto clean_mem_series = [](uint64_t v) {
    if ((v >> 63) == 0) delete (MemSeries*)(v);
  };
  indirection_manager_.iter(clean_mem_series);

  free(hash_locks_);

  mem::art_tree_destroy(&symbols_);

  if (concurrency_enabled_) {
    global_running_->set(false);
  }
  if (migration_enabled_) {
    migration_running_->set(false);
  }
  wg_.wait();
  if (concurrency_enabled_) {
    for (int i = 0; i < MAX_WORKER_NUM; i++) {
      free(local_epochs_[i]);
      free(spinlocks_[i]);
      free(running_[i]);
    }
    free(global_epoch_);
    free(global_running_);
  }
  if (migration_enabled_) free(migration_running_);
}

void Head::recover_memseries_from_indirection(
    std::unordered_map<uint64_t, MemSeries*>* id_map) {
  std::vector<std::pair<uint64_t, uint64_t>> ids =
      indirection_manager_.get_ids();
  for (auto& p : ids) {
    MemSeries* s = reload_mem_series(p.first, p.second);
    if (s) (*id_map)[s->ref] = s;
  }
}

leveldb::Status Head::enable_tags_logs() {
  std::vector<std::string> existing_logs;
  boost::filesystem::path p(dir_);
  boost::filesystem::directory_iterator end_itr;

  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > HEAD_INDEX_LOG_NAME.size() &&
          memcmp(current_file.c_str(), HEAD_INDEX_LOG_NAME.c_str(),
                 HEAD_INDEX_LOG_NAME.size()) == 0)
        existing_logs.push_back(current_file);
    }
  }
  std::sort(existing_logs.begin(), existing_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_INDEX_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_INDEX_LOG_NAME.size()));
            });

  leveldb::Env* env = leveldb::Env::Default();
  leveldb::Status s;
  uint64_t fsize = 0;
  for (size_t i = 0; i < existing_logs.size(); i++) {
    leveldb::RandomRWFile* rf;
    s = env->NewRandomRWFile(dir_ + std::string("/") + existing_logs[i], &rf);
    if (!s.ok()) {
      delete rf;
      return s;
    }
    tags_log_files_.push_back(rf);
    tags_log_readers_.push_back(new leveldb::log::RandomReader(rf));
    tags_log_writers_.push_back(new leveldb::log::RandomWriter(rf));
  }

  if (existing_logs.empty())
    cur_index_log_seq_ = 0;
  else
    cur_index_log_seq_ =
        std::stoi(existing_logs.back().substr(HEAD_INDEX_LOG_NAME.size())) + 1;

  leveldb::WritableFile* f;
  s = env->NewAppendableFile(
      dir_ + "/" + HEAD_INDEX_LOG_NAME + std::to_string(cur_index_log_seq_),
      &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  tags_log_file_.reset(f);
  tags_log_writer_.reset(new leveldb::log::Writer(f, fsize));

  leveldb::RandomRWFile* rf;
  s = env->NewRandomRWFile(dir_ + std::string("/") + HEAD_INDEX_LOG_NAME +
                               std::to_string(cur_index_log_seq_),
                           &rf);
  if (!s.ok()) {
    delete rf;
    return s;
  }
  tags_log_files_.push_back(rf);
  tags_log_readers_.push_back(new leveldb::log::RandomReader(rf));
  tags_log_writers_.push_back(new leveldb::log::RandomWriter(rf));

  return s;
}

leveldb::Status Head::recover_index_from_log(
    std::unordered_map<uint64_t, MemSeries*>* id_map) {
  leveldb::Env* env = leveldb::Env::Default();
  leveldb::Status s;
  uint64_t fsize = 0;

  std::vector<std::string> existing_logs;
  boost::filesystem::path p(dir_);
  boost::filesystem::directory_iterator end_itr;

  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > HEAD_INDEX_LOG_NAME.size() &&
          memcmp(current_file.c_str(), HEAD_INDEX_LOG_NAME.c_str(),
                 HEAD_INDEX_LOG_NAME.size()) == 0)
        existing_logs.push_back(current_file);
    }
  }
  std::sort(existing_logs.begin(), existing_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_INDEX_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_INDEX_LOG_NAME.size()));
            });

  // LOG_DEBUG << "#index files:" << existing_logs.size();
  for (size_t i = 0; i < existing_logs.size(); i++) {
    leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(dir_ + std::string("/") + existing_logs[i], &sf);
    if (!s.ok()) {
      delete sf;
      return s;
    }

    leveldb::RandomRWFile* rf;
    s = env->NewRandomRWFile(dir_ + std::string("/") + existing_logs[i], &rf);
    if (!s.ok()) {
      delete rf;
      return s;
    }
    tags_log_files_.push_back(rf);
    tags_log_readers_.push_back(new leveldb::log::RandomReader(rf));
    tags_log_writers_.push_back(new leveldb::log::RandomWriter(rf));

    leveldb::log::Reader r(sf, nullptr, false, 0);
    leveldb::Slice record;
    std::string scratch;
    leveldb::log::RefFlush f;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kSeries) {
        std::vector<leveldb::log::RefSeries> rs;
        bool success = leveldb::log::series(record, &rs);
        if (!success)
          return leveldb::Status::Corruption("series recover_index_from_log");

        // Recover index.
        for (size_t j = 0; j < rs.size(); j++) {
          auto p = get_or_create_with_id(rs[j].ref, rs[j].lset);
          if (rs[j].ref > last_series_id) last_series_id = rs[j].ref;
          (*id_map)[rs[j].ref] = p.first;
          // Set log_pos.
          p.first->log_pos = r.LastRecordOffset();
        }
      }
    }
    delete sf;
  }

  if (existing_logs.empty())
    cur_index_log_seq_ = 0;
  else
    cur_index_log_seq_ =
        std::stoi(existing_logs.back().substr(HEAD_INDEX_LOG_NAME.size())) + 1;

  leveldb::WritableFile* f;
  s = env->NewAppendableFile(
      dir_ + "/" + HEAD_INDEX_LOG_NAME + std::to_string(cur_index_log_seq_),
      &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  tags_log_file_.reset(f);
  tags_log_writer_.reset(new leveldb::log::Writer(f, fsize));

  leveldb::RandomRWFile* rf;
  s = env->NewRandomRWFile(dir_ + std::string("/") + HEAD_INDEX_LOG_NAME +
                               std::to_string(cur_index_log_seq_),
                           &rf);
  if (!s.ok()) {
    delete rf;
    return s;
  }
  tags_log_files_.push_back(rf);
  tags_log_readers_.push_back(new leveldb::log::RandomReader(rf));
  tags_log_writers_.push_back(new leveldb::log::RandomWriter(rf));
  return leveldb::Status::OK();
}

leveldb::Status Head::recover_samples_from_log(
    std::unordered_map<uint64_t, MemSeries*>* id_map) {
  leveldb::Env* env = leveldb::Env::Default();
  leveldb::Status s;
  uint64_t fsize = 0;

  std::vector<std::string> existing_logs;
  std::vector<std::string> existing_flushes_logs;
  boost::filesystem::path p(dir_);
  boost::filesystem::directory_iterator end_itr;

  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > HEAD_SAMPLES_LOG_NAME.size() &&
          memcmp(current_file.c_str(), HEAD_SAMPLES_LOG_NAME.c_str(),
                 HEAD_SAMPLES_LOG_NAME.size()) == 0)
        existing_logs.push_back(current_file);
      else if (current_file.size() > HEAD_FLUSHES_LOG_NAME.size() &&
               memcmp(current_file.c_str(), HEAD_FLUSHES_LOG_NAME.c_str(),
                      HEAD_FLUSHES_LOG_NAME.size()) == 0)
        existing_flushes_logs.push_back(current_file);
    }
  }
  std::sort(existing_logs.begin(), existing_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_SAMPLES_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_SAMPLES_LOG_NAME.size()));
            });
  std::sort(existing_flushes_logs.begin(), existing_flushes_logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(HEAD_FLUSHES_LOG_NAME.size())) <
                     std::stoi(r.substr(HEAD_FLUSHES_LOG_NAME.size()));
            });

  // First round to load the newest flushing txn.
  for (size_t i = 0; i < existing_flushes_logs.size(); i++) {
    leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(
        dir_ + std::string("/") + existing_flushes_logs[i], &sf);
    if (!s.ok()) {
      delete sf;
      return s;
    }
    leveldb::log::Reader r(sf, nullptr, false, 0);
    leveldb::Slice record;
    std::string scratch;
    leveldb::log::RefFlush f;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kFlush) {
        leveldb::log::flush(record, &f);
        if (id_map->find(f.ref) == id_map->end()) {
          LOG_ERROR << f.ref << " not found";
          abort();
        }
        // Update flushing txn.
        ((*id_map)[f.ref])->flushed_txn_ = f.txn;
      }
    }
    delete sf;
  }

  // Secpnd round to load the unflushed samples.
  for (size_t i = 0; i < existing_logs.size(); i++) {
    leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(dir_ + std::string("/") + existing_logs[i], &sf);
    if (!s.ok()) {
      delete sf;
      return s;
    }
    leveldb::log::Reader r(sf, nullptr, false, 0);
    leveldb::Slice record;
    std::string scratch;
    std::vector<leveldb::log::RefSample> rs;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kSample) {
        rs.clear();
        leveldb::log::samples(record, &rs);

        for (size_t j = 0; j < rs.size(); j++) {
          if (id_map->find(rs[j].ref) == id_map->end()) {
            LOG_ERROR << rs[j].ref << " not found";
            abort();
          }
          MemSeries* s = (*id_map)[rs[j].ref];
          if (s && rs[j].txn > s->flushed_txn_) {
            s->append(db_, rs[j].t, rs[j].v);
          }
        }
      }
    }
    delete sf;
  }

  // Init samples log writer.
  if (existing_logs.empty())
    cur_samples_log_seq_ = 0;
  else
    cur_samples_log_seq_ =
        std::stoi(existing_logs.back().substr(HEAD_SAMPLES_LOG_NAME.size())) +
        1;
  leveldb::WritableFile* f;
  s = env->NewAppendableFile(dir_ + std::string("/") + HEAD_SAMPLES_LOG_NAME +
                                 std::to_string(cur_samples_log_seq_),
                             &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  samples_log_file_.reset(f);
  samples_log_writer_.reset(new leveldb::log::Writer(f));
  active_samples_logs_ = existing_logs.size();

  // Init flushes log writer.
  if (existing_flushes_logs.empty())
    cur_flushes_log_seq_ = 0;
  else
    cur_flushes_log_seq_ = std::stoi(existing_flushes_logs.back().substr(
                               HEAD_FLUSHES_LOG_NAME.size())) +
                           1;
  s = env->NewAppendableFile(dir_ + std::string("/") + HEAD_FLUSHES_LOG_NAME +
                                 std::to_string(cur_flushes_log_seq_),
                             &f);
  if (!s.ok()) {
    delete f;
    return s;
  }
  flushes_log_file_.reset(f);
  flushes_log_writer_.reset(new leveldb::log::Writer(f));

  return s;
}

leveldb::Status Head::clean_samples_logs() {
  std::vector<std::vector<leveldb::log::RefSample>> refsamples;
  leveldb::Status s;
  leveldb::Env* env = leveldb::Env::Default();
  MemSeries* ms;

  // ROUND1: find the max txn in the logs for each TS/Group.
  int end = cur_flushes_log_seq_ - 1;
  for (int i = 0; i <= end; i++) {
    std::string fname =
        dir_ + std::string("/") + HEAD_FLUSHES_LOG_NAME + std::to_string(i);
    if (!env->FileExists(fname)) continue;

    leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(fname, &sf);
    if (!s.ok()) {
      std::cout << "NewSequentialFile " << s.ToString() << std::endl;
      return s;
    }
    leveldb::log::Reader r(sf, nullptr, false, 0);
    leveldb::Slice record;
    std::string scratch;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kFlush) {
        leveldb::log::RefFlush flush;
        leveldb::log::flush(record, &flush);

        // Update flushing txn.
        uint64_t series_pointer =
            indirection_manager_.read_slot(flush.logical_id);
        if ((series_pointer >> 63) == 0) {
          if (flush.txn > ((MemSeries*)(series_pointer))->log_clean_txn_)
            ((MemSeries*)(series_pointer))->log_clean_txn_ = flush.txn;
        } else {
          // Reload the memory object.
          ms = reload_mem_series(flush.logical_id, series_pointer);
          if (flush.txn > ms->log_clean_txn_) ms->log_clean_txn_ = flush.txn;
        }
      }
    }
    delete sf;

    s = env->DeleteFile(fname);
    if (!s.ok()) return s;
    LOG_DEBUG << "[REMOVE] " << fname;
  }

  // ROUND2: rewrite the sampels > clean log txn.
  end = cur_samples_log_seq_ - 1;
  for (int i = 0; i <= end; i++) {
    std::string fname =
        dir_ + std::string("/") + HEAD_SAMPLES_LOG_NAME + std::to_string(i);
    if (!env->FileExists(fname)) continue;

    leveldb::SequentialFile* sf;
    s = env->NewSequentialFile(fname, &sf);
    if (!s.ok()) {
      std::cout << "NewSequentialFile " << s.ToString() << std::endl;
      return s;
    }
    leveldb::log::Reader r(sf, nullptr, false, 0);
    leveldb::Slice record;
    std::string scratch;
    while (r.ReadRecord(&record, &scratch)) {
      if (record.data()[0] == leveldb::log::kSample) {
        std::vector<leveldb::log::RefSample> rs;
        std::vector<leveldb::log::RefSample> reserve;
        leveldb::log::samples(record, &rs);

        for (size_t j = 0; j < rs.size(); j++) {
          // The logical id in logs may mismatch current TS.
          uint64_t series_pointer =
              indirection_manager_.check_read_slot(rs[j].logical_id);
          if (series_pointer == std::numeric_limits<uint64_t>::max()) {
            LOG_DEBUG << rs[j].logical_id;
            continue;
          }
          if ((series_pointer >> 63) == 0)
            ms = ((MemSeries*)(series_pointer));
          else
            ms = reload_mem_series(rs[j].logical_id, series_pointer);
          if (ms == nullptr || ms->ref != rs[j].ref) {
            std::cout << rs[j].logical_id << " " << series_pointer << " " << ms->ref << " " << rs[j].ref << std::endl;
            continue;
          }

          if (rs[j].txn > ms->log_clean_txn_) reserve.push_back(rs[j]);
        }
        if (!reserve.empty()) refsamples.push_back(reserve);
      }
    }
    delete sf;

    s = env->DeleteFile(fname);
    if (!s.ok()) return s;
    LOG_DEBUG << "[REMOVE] " << fname;

    if (!refsamples.empty()) {
      leveldb::WritableFile* f;
      s = env->NewAppendableFile(fname, &f);
      if (!s.ok()) {
        std::cout << "NewAppendableFile " << s.ToString() << std::endl;
        return s;
      }
      leveldb::log::Writer* w = new leveldb::log::Writer(f);

      for (size_t j = 0; j < refsamples.size(); j++) {
        std::string rec = leveldb::log::samples(refsamples[j]);
        s = w->AddRecord(rec);
        if (!s.ok()) return s;
      }

      refsamples.clear();
      delete f;
      delete w;
      LOG_DEBUG << "[ADD] " << fname;
    } else
      active_samples_logs_--;
  }
  return s;
}

void Head::bg_clean_samples_logs() {
  // Start the thread for periodical log cleaning.
  bg_clean_samples_logs_.store(true);
  std::thread bg_log_cleaning([this]() {
    while (bg_clean_samples_logs_.load()) {
      if (active_samples_logs_ > HEAD_SAMPLES_LOG_CLEANING_THRES) {
        int current_active_samples_logs = active_samples_logs_;
        this->clean_samples_logs();
        if (active_samples_logs_ >= current_active_samples_logs) sleep(20);
      }
      sleep(1);
    }
    printf("bg_clean_samples_logs exits\n");
  });
  bg_log_cleaning.detach();
}

leveldb::Status Head::try_extend_tags_logs() {
  leveldb::Status s;
  if (tags_log_writer_ &&
      tags_log_writer_->write_off() > MAX_HEAD_SAMPLES_LOG_SIZE) {
    cur_index_log_seq_++;
    leveldb::WritableFile* f;
    leveldb::Env* env = leveldb::Env::Default();
    s = env->NewAppendableFile(
        dir_ + "/" + HEAD_INDEX_LOG_NAME + std::to_string(cur_index_log_seq_),
        &f);
    if (!s.ok()) {
      delete f;
      return s;
    }
    tags_log_file_.reset(f);
    tags_log_writer_.reset(new leveldb::log::Writer(f));

    leveldb::RandomRWFile* rf;
    s = env->NewRandomRWFile(
        dir_ + "/" + HEAD_INDEX_LOG_NAME + std::to_string(cur_index_log_seq_),
        &rf);
    if (!s.ok()) {
      delete rf;
      return s;
    }
    tags_log_files_.push_back(rf);
    tags_log_readers_.push_back(new leveldb::log::RandomReader(rf));
    tags_log_writers_.push_back(new leveldb::log::RandomWriter(rf));
  }
  return s;
}

leveldb::Status Head::try_extend_samples_logs() {
  leveldb::Status s;
  if (samples_log_writer_ &&
      samples_log_writer_->write_off() > MAX_HEAD_SAMPLES_LOG_SIZE) {
    cur_samples_log_seq_++;
    leveldb::WritableFile* f;
    leveldb::Env* env = leveldb::Env::Default();
    s = env->NewAppendableFile(dir_ + "/" + HEAD_SAMPLES_LOG_NAME +
                                   std::to_string(cur_samples_log_seq_),
                               &f);
    if (!s.ok()) {
      delete f;
      return s;
    }
    samples_log_file_.reset(f);
    samples_log_writer_.reset(new leveldb::log::Writer(f));

    active_samples_logs_++;
  }
  return s;
}

leveldb::Status Head::try_extend_flushes_logs() {
  leveldb::Status s;
  if (flushes_log_writer_ &&
      flushes_log_writer_->write_off() > MAX_HEAD_SAMPLES_LOG_SIZE) {
    cur_flushes_log_seq_++;
    leveldb::WritableFile* f;
    leveldb::Env* env = leveldb::Env::Default();
    s = env->NewAppendableFile(dir_ + "/" + HEAD_FLUSHES_LOG_NAME +
                                   std::to_string(cur_flushes_log_seq_),
                               &f);
    if (!s.ok()) {
      delete f;
      return s;
    }
    flushes_log_file_.reset(f);
    flushes_log_writer_.reset(new leveldb::log::Writer(f));
  }
  return s;
}

leveldb::Status Head::load_mem_series(uint64_t pos, uint64_t* physical_id,
                                      label::Labels* lset, int64_t* flushed_txn,
                                      int64_t* log_clean_txn) {
  uint64_t idx1 = pos >> 32;
  uint64_t idx2 = pos & 0xffffffff;

  if (idx1 == cur_index_log_seq_) {
    tags_log_lock_.lock();
    tags_log_writer_->flush();
    tags_log_lock_.unlock();
  }

  leveldb::Slice record;
  std::string scratch;
  tags_log_lock_.lock();
  bool ok = tags_log_readers_[idx1]->ReadRecord(idx2, &record, &scratch);
  if (!ok) {
    // Flush and try again.
    // NOTE(Alec): because writer uses buffer, the buffered data may not be
    // flushed.
    tags_log_file_->Flush();
    ok = tags_log_readers_[idx1]->ReadRecord(idx2, &record, &scratch);
    if (!ok) {
      tags_log_lock_.unlock();
      return leveldb::Status::IOError("ReadRecord fid:" + std::to_string(idx1) +
                                      " off:" + std::to_string(idx2));
    }
  }
  leveldb::log::RefSeries rs;
  ok = leveldb::log::series(record, &rs);
  if (!ok) {
    tags_log_lock_.unlock();
    return leveldb::Status::IOError(
        "Decode RefSeries fid:" + std::to_string(idx1) + " off:" +
        std::to_string(idx2) + " record size:" + std::to_string(record.size()));
  }
  tags_log_lock_.unlock();
  if (physical_id) *physical_id = rs.ref;
  *lset = std::move(rs.lset);
  if (flushed_txn) *flushed_txn = rs.flushed_txn;
  if (log_clean_txn) *log_clean_txn = rs.log_clean_txn;
  return leveldb::Status::OK();
}

MemSeries* Head::reload_mem_series(uint64_t logical_id,
                                   uint64_t series_pointer) {
  MemSeries* s = new MemSeries();
  s->log_pos = series_pointer & 0x7fffffffffffffff;
  int64_t flushed_txn;
  leveldb::Status st = load_mem_series(s->log_pos, &s->ref, &s->labels,
                                       &flushed_txn, &s->log_clean_txn_);
  if (!st.ok()) {
    LOG_ERROR << st.ToString();
    delete s;
    return nullptr;
  }
  s->flushed_txn_ = flushed_txn;
  s->logical_id = logical_id;
  s->update_access_epoch();
  if (indirection_manager_.cas_slot(logical_id, series_pointer,
                                    (uintptr_t)((uintptr_t)s)))
    return s;

  // Another thread wins.
  delete s;
  series_pointer = indirection_manager_.read_slot(logical_id);
  if ((series_pointer >> 63) == 0)
    return ((MemSeries*)(series_pointer));
  else
    return reload_mem_series(logical_id, series_pointer);
}

MemSeries* Head::check_reload_mem_series(uint64_t physical_id,
                                         uint64_t logical_id,
                                         uint64_t series_pointer) {
  MemSeries* s = new MemSeries();
  s->log_pos = series_pointer & 0x7fffffffffffffff;
  int64_t flushed_txn;
  leveldb::Status st = load_mem_series(s->log_pos, &s->ref, &s->labels,
                                       &flushed_txn, &s->log_clean_txn_);
  if (!st.ok()) {
    LOG_ERROR << st.ToString();
    delete s;
    abort();
  }
  if (s->ref != physical_id) return nullptr;
  s->flushed_txn_ = flushed_txn;
  s->logical_id = logical_id;
  if (indirection_manager_.cas_slot(logical_id, series_pointer,
                                    (uintptr_t)((uintptr_t)s)))
    return s;

  // Another thread wins.
  series_pointer = indirection_manager_.read_slot(logical_id);
  if ((series_pointer >> 63) == 0)
    return ((MemSeries*)(series_pointer));
  else
    return reload_mem_series(logical_id, series_pointer);
}

leveldb::Status Head::write_flush_marks(
    const std::vector<leveldb::log::RefFlush>& marks) {
  leveldb::Status st;
  if (flushes_log_writer_) {
    flushes_log_lock_.lock();
    for (const auto& mark : marks) {
      std::string rec = leveldb::log::flush(mark);
      st = flushes_log_writer_->AddRecord(rec);
      if (!st.ok()) {
        flushes_log_lock_.unlock();
        return st;
      }

      uint64_t series_pointer = indirection_manager_.read_slot(mark.logical_id);
      if ((series_pointer >> 63) == 0) {
        ((MemSeries*)(series_pointer))->log_clean_txn_ = mark.txn;
      } else {
        // Reload the memory object.
        MemSeries* s = reload_mem_series(mark.logical_id, series_pointer);
        s->log_clean_txn_ = mark.txn;
      }
    }

    try_extend_flushes_logs();
    flushes_log_lock_.unlock();
  }
  return st;
}

void Head::update_min_max_time(int64_t mint, int64_t maxt) {
  while (true) {
    int64_t lt = min_time.get();
    if (mint >= lt || valid_time.get() >= mint) break;
    if (min_time.cas(lt, mint)) break;
  }
  while (true) {
    int64_t ht = max_time.get();
    if (maxt <= ht) break;
    if (max_time.cas(ht, maxt)) break;
  }
}

std::unique_ptr<db::AppenderInterface> Head::head_appender() {
  return std::unique_ptr<db::AppenderInterface>(
      new HeadAppender(const_cast<Head*>(this), db_));
}

std::unique_ptr<db::AppenderInterface> Head::appender() {
  return head_appender();
}

// init_time initializes a head with the first timestamp. This only needs to be
// called for a completely fresh head with an empty WAL. Returns true if the
// initialization took an effect.
bool Head::init_time(int64_t t) {
  if (!min_time.cas(std::numeric_limits<int64_t>::max(), t)) return false;
  // Ensure that max time is initialized to at least the min time we just set.
  // Concurrent appenders may already have set it to a higher value.
  max_time.cas(std::numeric_limits<int64_t>::min(), t);
  return true;
}

// Double hashing (logical id -> hash2)
// MemSeries* Head::get_by_hash(uint64_t hash1, uint64_t hash2, const
// label::Labels &lset) {
//   MemSeries* s = nullptr;
//   uint64_t idx = hash1 % STRIPE_SIZE;
//   hash_locks_[idx].lock();
//   for (auto& p : hash_shards_[idx]) {
//     if (p.second != hash2)
//       continue;

//     uint64_t series_pointer = indirection_manager_.read_slot(p.first);
//     if ((series_pointer >> 63) == 0) {
//       if (label::lbs_compare(lset, ((MemSeries*)(series_pointer))->labels) ==
//       0) {
//         s = (MemSeries*)(series_pointer);
//         break;
//       }
//     }
//     else {
//       label::Labels disk_lset;
//       leveldb::Status st = load_mem_series(series_pointer &
//       0x7fffffffffffffff, nullptr, &disk_lset, nullptr, nullptr); if
//       (!st.ok()) {
//         LOG_ERROR << st.ToString();
//         abort();
//       }
//       if (label::lbs_compare(lset, disk_lset) == 0) {
//         s = reload_mem_series(p.first, series_pointer);
//         break;
//       }
//     }
//   }
//   hash_locks_[idx].unlock();
//   return s;
// }

// void Head::set_by_hash(uint64_t hash1, uint64_t hash2, uint64_t logical_id) {
//   uint64_t idx = hash1 % STRIPE_SIZE;
//   hash_locks_[idx].lock();
//   hash_shards_[idx][logical_id] = hash2;
//   hash_locks_[idx].unlock();
// }

// Double hashing (hash2 -> {logical id, ...})
// MemSeries* Head::get_by_hash(uint64_t hash1, uint64_t hash2, const
// label::Labels &lset) {
//   MemSeries* s = nullptr;
//   uint64_t idx = hash1 % STRIPE_SIZE;
//   hash_locks_[idx].lock();
//   auto p = hash_shards_[idx].find(hash2);
//   if (p != hash_shards_[idx].end()) {
//     for (uint64_t logical_id : p->second) {
//       uint64_t series_pointer = indirection_manager_.read_slot(logical_id);
//       if ((series_pointer >> 63) == 0) {
//         if (label::lbs_compare(lset, ((MemSeries*)(series_pointer))->labels)
//         == 0) {
//           s = (MemSeries*)(series_pointer);
//           break;
//         }
//       }
//       else {
//         label::Labels disk_lset;
//         leveldb::Status st = load_mem_series(series_pointer &
//         0x7fffffffffffffff, nullptr, &disk_lset, nullptr, nullptr); if
//         (!st.ok()) {
//           LOG_ERROR << st.ToString();
//           abort();
//         }
//         if (label::lbs_compare(lset, disk_lset) == 0) {
//           s = reload_mem_series(logical_id, series_pointer);
//           break;
//         }
//       }
//     }
//   }
//   hash_locks_[idx].unlock();
//   return s;
// }

// void Head::set_by_hash(uint64_t hash1, uint64_t hash2, uint64_t logical_id) {
//   uint64_t idx = hash1 % STRIPE_SIZE;
//   hash_locks_[idx].lock();
//   auto p = hash_shards_[idx].find(hash2);
//   if (p != hash_shards_[idx].end())
//     hash_shards_[idx][hash2].push_back(logical_id);
//   else
//     hash_shards_[idx][hash2] = std::vector<uint64_t>({logical_id});
//   hash_locks_[idx].unlock();
// }

MemSeries* Head::get_by_hash(uint64_t hash, const label::Labels& lset) {
  MemSeries* s = nullptr;
  uint64_t idx = hash % STRIPE_SIZE;
  hash_locks_[idx].lock();
  auto p = hash_shards_[idx].find(hash);
  if (p != hash_shards_[idx].end()) {
    for (uint64_t logical_id : p->second) {
      uint64_t series_pointer = indirection_manager_.read_slot(logical_id);
      if ((series_pointer >> 63) == 0) {
        MemSeries* ms = (MemSeries*)(series_pointer);
        ms->lock();
        if (ms->labels.empty()) {
          label::Labels disk_lset;
          leveldb::Status st =
              load_mem_series(ms->log_pos, nullptr, &disk_lset, nullptr, nullptr);
          if (!st.ok()) {
            LOG_ERROR << st.ToString();
            abort();
          }
          if (label::lbs_compare(lset, disk_lset) == 0) {
            s = ms;
            s->update_access_epoch();
            ms->unlock();
            break;
          }
        }
        else if (label::lbs_compare(lset, ms->labels) ==
                 0) {
          s = ms;
          s->update_access_epoch();
          ms->unlock();
          break;
        }
        ms->unlock();
      } else {
        label::Labels disk_lset;
        leveldb::Status st =
            load_mem_series(series_pointer & 0x7fffffffffffffff, nullptr,
                            &disk_lset, nullptr, nullptr);
        if (!st.ok()) {
          LOG_ERROR << st.ToString();
          abort();
        }
        if (label::lbs_compare(lset, disk_lset) == 0) {
          s = reload_mem_series(logical_id, series_pointer);
          break;
        }
      }
    }
  }
  hash_locks_[idx].unlock();
  return s;
}

void Head::set_by_hash(uint64_t hash, uint64_t logical_id) {
  uint64_t idx = hash % STRIPE_SIZE;
  hash_locks_[idx].lock();
  auto p = hash_shards_[idx].find(hash);
  if (p != hash_shards_[idx].end())
    hash_shards_[idx][hash].push_back(logical_id);
  else
    hash_shards_[idx][hash] = std::vector<uint64_t>({logical_id});
  hash_locks_[idx].unlock();
}

// Current assumption: no two threads will insert the same tags at the same
// time.
std::pair<MemSeries*, bool> Head::get_or_create(const label::Labels& lset,
                                                uint64_t epoch) {
  // std::vector<std::vector<uint64_t>> pls(lset.size());
  // if (!sync_api_) {
  //   std::promise<bool> p;
  //   std::future<bool> f = p.get_future();
  //   posting_list->async_get(&lset, &pls, [](void* arg){
  //     ((std::promise<bool>*)(arg))->set_value(true);
  //   }, &p);
  //   f.wait();
  // }
  // else {
  //   posting_list->get(lset, &pls);
  // }

  // bool not_existed = false;
  // for (size_t i = 0; i < pls.size(); i++) {
  //   if (pls[i].empty()) {
  //     not_existed = true;
  //     break;
  //   }
  // }

  // if (!not_existed) {
  //   index::IntersectPostings3 pl(std::move(pls));
  //   if (pl.next()) {
  //     uint64_t series_pointer = indirection_manager_.read_slot(pl.at());
  //     if ((series_pointer >> 63) == 0)
  //       return std::make_pair((MemSeries*)(series_pointer), false);
  //     MemSeries* s = reload_mem_series(pl.at(), series_pointer);
  //     return std::make_pair(s, false);
  //   }
  // }

  uint64_t hash1 = label::lbs_hash(lset);
  // uint64_t hash2 = label::lbs_hash(lset, 19960514);
  MemSeries* s = get_by_hash(hash1, lset);
  if (s) {
    s->update_access_epoch();
    return std::make_pair(s, false);
  }

  uint64_t id = last_series_id.fetch_add(1) + 1;

  s = new MemSeries(lset, id);
  s->update_access_epoch();

  // Allocate a slot in indirection manager.
  uint64_t logical_id = indirection_manager_.alloc_slot();
  s->logical_id = logical_id;
  indirection_manager_.set_slot(logical_id, (uint64_t)((uintptr_t)s));

  if (!sync_api_) {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_add(
        logical_id, &lset,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  } else {
    if (concurrency_enabled_)
      posting_list->_add_job(logical_id, &lset, epoch, nullptr, nullptr,
                             &gc_requests_, nullptr, nullptr);
    else
      posting_list->add(logical_id, lset);
  }

  set_by_hash(hash1, logical_id);

  // base::RWLockGuard lock(mutex_, 1);
  for (const label::Label& l : lset) {
    // art_int_incr_helper(&label_names_, l.label);
    // art_int_incr_helper(&label_values_, l.label + label::HEAD_LABEL_SEP +
    // l.value);

    art_int_incr_helper(&symbols_, l.label);
    art_int_incr_helper(&symbols_, l.value);
  }

  return std::make_pair(s, true);
}

std::pair<MemSeries*, bool> Head::get_or_create(label::Labels&& lset,
                                                uint64_t epoch) {
  uint64_t hash1 = label::lbs_hash(lset);
  // uint64_t hash2 = label::lbs_hash(lset, 19960514);
  MemSeries* s = get_by_hash(hash1, lset);
  if (s) {
    s->update_access_epoch();
    return std::make_pair(s, false);
  }

  uint64_t id = last_series_id.fetch_add(1) + 1;

  s = new MemSeries(std::move(lset), id);
  s->update_access_epoch();

  // Allocate a slot in indirection manager.
  uint64_t logical_id = indirection_manager_.alloc_slot();
  s->logical_id = logical_id;
  indirection_manager_.set_slot(logical_id, (uint64_t)((uintptr_t)s));

  if (!sync_api_) {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_add(
        logical_id, &s->labels,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  } else {
    if (concurrency_enabled_)
      posting_list->_add_job(logical_id, &s->labels, epoch, nullptr, nullptr,
                             &gc_requests_, nullptr, nullptr);
    else
      posting_list->add(logical_id, s->labels);
  }

  set_by_hash(hash1, logical_id);

  // base::RWLockGuard lock(mutex_, 1);
  for (const label::Label& l : s->labels) {
    // art_int_incr_helper(&label_names_, l.label);
    // art_int_incr_helper(&label_values_, l.label + label::HEAD_LABEL_SEP +
    // l.value);

    art_int_incr_helper(&symbols_, l.label);
    art_int_incr_helper(&symbols_, l.value);
  }

  return std::make_pair(s, true);
}

std::pair<MemSeries*, bool> Head::get_or_create_with_id(
    uint64_t id, const label::Labels& lset, uint64_t epoch) {
  // std::vector<std::vector<uint64_t>> pls(lset.size());
  // if (!sync_api_) {
  //   std::promise<bool> p;
  //   std::future<bool> f = p.get_future();
  //   posting_list->async_get(&lset, &pls, [](void* arg){
  //     ((std::promise<bool>*)(arg))->set_value(true);
  //   }, &p);
  //   f.wait();
  // }
  // else {
  //   posting_list->get(lset, &pls);
  // }

  // bool not_existed = false;
  // for (size_t i = 0; i < pls.size(); i++) {
  //   if (pls[i].empty()) {
  //     not_existed = true;
  //     break;
  //   }
  // }

  // if (!not_existed) {
  //   index::IntersectPostings3 pl(std::move(pls));
  //   if (pl.next()) {
  //     uint64_t series_pointer = indirection_manager_.read_slot(pl.at());
  //     if ((series_pointer >> 63) == 0)
  //       return std::make_pair((MemSeries*)(series_pointer), false);
  //     MemSeries* s = reload_mem_series(pl.at(), series_pointer);
  //     return std::make_pair(s, false);
  //   }
  // }

  uint64_t hash1 = label::lbs_hash(lset);
  // uint64_t hash2 = label::lbs_hash(lset, 19960514);
  MemSeries* s = get_by_hash(hash1, lset);
  if (s) {
    s->update_access_epoch();
    return std::make_pair(s, false);
  }

  s = new MemSeries(lset, id);
  s->update_access_epoch();

  // Allocate a slot in indirection manager.
  uint64_t logical_id = indirection_manager_.alloc_slot();
  s->logical_id = logical_id;
  indirection_manager_.set_slot(logical_id, (uint64_t)((uintptr_t)s));

  if (!sync_api_) {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_add(
        logical_id, &lset,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  } else {
    if (concurrency_enabled_)
      posting_list->_add_job(logical_id, &lset, epoch, nullptr, nullptr,
                             &gc_requests_, nullptr, nullptr);
    else
      posting_list->add(logical_id, lset);
  }

  set_by_hash(hash1, logical_id);

  // base::RWLockGuard lock(mutex_, 1);
  for (const label::Label& l : lset) {
    // art_int_incr_helper(&label_names_, l.label);
    // art_int_incr_helper(&label_values_, l.label + label::HEAD_LABEL_SEP +
    // l.value);

    art_int_incr_helper(&symbols_, l.label);
    art_int_incr_helper(&symbols_, l.value);
  }

  return std::make_pair(s, true);
}

std::set<std::string> Head::symbols() {
  std::set<std::string> r;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    ((std::set<std::string>*)(data))
        ->insert(std::string(reinterpret_cast<const char*>(key), key_len));
    return false;
  };

  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  mem::art_range(&symbols_, left, 1, right, 1, true, true, cb, &r);
  return r;
}

std::vector<std::string> Head::label_values(const std::string& name) {
  std::vector<std::string> vec;
  posting_list->get_values(name, &vec);
  return vec;
}

std::pair<std::unique_ptr<index::PostingsInterface>, bool> Head::postings(
    const std::string& name, const std::string& value) const {
  std::vector<uint64_t> pls;
  if (!sync_api_) {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_get_single(
        &name, &value, &pls,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  } else
    posting_list->get(name, value, &pls);

  if (!pls.empty())
    return std::make_pair(std::unique_ptr<index::PostingsInterface>(
                              new index::VectorPostings(std::move(pls))),
                          true);
  else
    return std::make_pair(nullptr, false);
}

std::unique_ptr<index::PostingsInterface> Head::select(
    const std::vector<::tsdb::label::MatcherInterface*>& l) {
  if (!sync_api_) {
    std::vector<std::vector<uint64_t>> pls(l.size());
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_select(
        &l, &pls,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();

    for (size_t i = 0; i < pls.size(); i++)
      if (pls[i].empty()) return nullptr;

    return std::unique_ptr<index::PostingsInterface>(
        new index::IntersectPostings3(std::move(pls)));
  } else
    return posting_list->select(l);
}

bool Head::series(uint64_t ref, label::Labels& lset,
                  std::string* chunk_contents) {
  uint64_t series_pointer = indirection_manager_.read_slot(ref);
  if ((series_pointer >> 63) == 0) {
    MemSeries* s = (MemSeries*)series_pointer;
    s->lock();
    if (s->labels.empty()) {
      leveldb::Status st = load_mem_series(s->log_pos, nullptr, &(s->labels), nullptr, nullptr);
      if (!st.ok())
        LOG_DEBUG << st.ToString();
    }
    lset.insert(lset.end(), s->labels.begin(), s->labels.end());
    chunk_contents->append(reinterpret_cast<const char*>(s->chunk_.bytes()),
                           s->chunk_.size());
    s->unlock();
    s->update_access_epoch();
  } else
    load_mem_series(series_pointer & 0x7fffffffffffffff, nullptr, &lset,
                    nullptr, nullptr);
  return true;
}

std::vector<std::string> Head::label_names() const {
  std::vector<std::string> r;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    ((std::vector<std::string>*)(data))
        ->push_back(std::string(reinterpret_cast<const char*>(key), key_len));
    return false;
  };

  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  mem::art_range(const_cast<mem::art_tree*>(posting_list->get_tag_keys_trie()),
                 left, 1, right, 1, true, true, cb, &r);
  return r;
}

std::unique_ptr<index::PostingsInterface> Head::sorted_postings(
    std::unique_ptr<index::PostingsInterface>&& p) {
  struct SortPair {
    label::Labels lset;
    uint64_t id;
    SortPair() = default;
    SortPair(const label::Labels& ls, uint64_t v) : lset(ls), id(v) {}
  };

  std::vector<SortPair> pairs;
  while (p->next()) {
    uint64_t series_pointer = indirection_manager_.read_slot(p->at());
    if ((series_pointer >> 63) == 0)
      pairs.emplace_back(((MemSeries*)(series_pointer))->labels, p->at());
    else {
      pairs.emplace_back();
      load_mem_series(series_pointer & 0x7fffffffffffffff, nullptr,
                      &pairs.back().lset, nullptr, nullptr);
      pairs.back().id = p->at();
    }
  }

  std::sort(pairs.begin(), pairs.end(),
            [](const SortPair& l, const SortPair& r) {
              return label::lbs_compare(l.lset, r.lset) < 0;
            });

  index::VectorPostings* vp = new index::VectorPostings(pairs.size());
  for (auto const& s : pairs) vp->push_back(s.id);
  return std::unique_ptr<index::PostingsInterface>(vp);
}

uint64_t Head::physical_id(uint64_t logical_id) {
  uint64_t series_pointer = indirection_manager_.read_slot(logical_id);
  if ((series_pointer >> 63) == 0) return ((MemSeries*)(series_pointer))->ref;
  return reload_mem_series(logical_id, series_pointer)->ref;
}

leveldb::Status Head::snapshot_index() {
  pid_t child_pid = fork();
  if (child_pid == 0) {
    // Snapshot indirection manager.
    // We cannot use async call here because threads are not copied.
    auto p = posting_list->get(label::ALL_POSTINGS_KEYS.label,
                               label::ALL_POSTINGS_KEYS.value);
    if (p == nullptr) {
      LOG_ERROR << "no all postings";
      abort();
    }
    while (p->next()) {
      uint64_t series_pointer = indirection_manager_.read_slot(p->at());
      if ((series_pointer >> 63) == 0)
        indirection_manager_.set_slot(
            p->at(),
            ((MemSeries*)(series_pointer))->log_pos | 0x8000000000000000);
    }

    // Snapshot index.
    leveldb::Status st = posting_list->snapshot_index(dir_ + "/snapshot");
    LOG_DEBUG << "InvertedIndex::snapshot_index " << st.ToString();

    st = indirection_manager_.snapshot(dir_ + "/snapshot");
    LOG_DEBUG << "IndirectionManager::snapshot " << st.ToString();
    exit(0);
  } else if (child_pid < 0) {
    return leveldb::Status::IOError("snapshot_index() fork fails!");
  } else {
    int rstatus;
    waitpid(child_pid, &rstatus, 0);
    if (rstatus == 0)
      return leveldb::Status::OK();
    else if (rstatus == -1)
      return leveldb::Status::IOError(
          "snapshot_index() child terminates with error!");
    else
      return leveldb::Status::OK();
  }
}

int Head::memseries_gc(int percentage, uint64_t epoch,
                       moodycamel::ConcurrentQueue<mem::GCRequest>* gc_queue) {
  std::vector<uint64_t> pls;
  if (!sync_api_) {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_get_single(
        &label::ALL_POSTINGS_KEYS.label, &label::ALL_POSTINGS_KEYS.value, &pls,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  } else
    posting_list->get(label::ALL_POSTINGS_KEYS.label,
                      label::ALL_POSTINGS_KEYS.value, &pls);

  std::vector<uint64_t> mem_ids;
  mem_ids.reserve(pls.size());
  for (size_t i = 0; i < pls.size(); i++) {
    if ((indirection_manager_.read_slot(pls[i]) >> 63) == 0)
      mem_ids.push_back(pls[i]);
  }
  std::sort(mem_ids.begin(), mem_ids.end(), [&](uint64_t lhs, uint64_t rhs) {
    uint64_t lp = indirection_manager_.read_slot(lhs);
    uint64_t rp = indirection_manager_.read_slot(rhs);
    return ((MemSeries*)(lp))->access_epoch.load() >
           ((MemSeries*)(rp))->access_epoch.load();
  });

  // Preflush
  for (int i = 0; i < mem_ids.size() * percentage / 100; i++) {
    uint64_t p = indirection_manager_.read_slot(mem_ids[i]);
    MemSeries* ms = (MemSeries*)(p);
    mem::write_lock_or_restart(ms);
    ms->lock();
    // We need to clean the data samples.
    if (ms->num_samples_ > 0) ms->_flush(db_, ms->flushed_txn_.load());
    ms->unlock();
    mem::write_unlock(ms);
  }

  // Wait for imm being compacted.
  reinterpret_cast<leveldb::DBImpl*>(db_)->TEST_CompactMemTable();

  // Update indirection manager.
  // TODO: protect ms being reloaded again by the appender.
  std::string buf;
  std::set<int> need_flush;
  for (int i = 0; i < mem_ids.size() * percentage / 100; i++) {
    uint64_t p = indirection_manager_.read_slot(mem_ids[i]);
    MemSeries* ms = (MemSeries*)(p);

    // if (ms->labels.empty()) {
    //   leveldb::Status st =
    //       load_mem_series(ms->log_pos, nullptr, &(ms->labels), nullptr, nullptr);
    //   if (!st.ok()) {
    //     LOG_ERROR << st.ToString();
    //     abort();
    //   }
    // }

    // Update log clean txn.
    uint64_t idx1 = ms->log_pos >> 32;
    uint64_t idx2 = ms->log_pos & 0xffffffff;
    buf.clear();
    tags_log_lock_.lock();
    ::leveldb::log::series_without_labels(ms->ref, ms->flushed_txn_.load(), ms->log_clean_txn_, &buf);
    if (idx1 == tags_log_writers_.size() - 1) tags_log_file_->Flush();
    tags_log_writers_[idx1]->AddRecord(idx2, buf, false);
    need_flush.insert(idx1);
    tags_log_lock_.unlock();

    ++_ms_gc_counter;

    indirection_manager_.set_slot(mem_ids[i], 0x8000000000000000 | ms->log_pos);

    if (gc_queue)
      gc_queue->enqueue(mem::GCRequest(epoch, (void*)(ms), mem::MEMSERIES));
  }

  for (int i : need_flush)
    tags_log_writers_[i]->flush();

  return mem_ids.size() * percentage / 100;
}

int Head::memseries_gc_preflush(int percentage) {
  std::vector<uint64_t> pls;
  if (!sync_api_) {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    posting_list->async_get_single(
        &label::ALL_POSTINGS_KEYS.label, &label::ALL_POSTINGS_KEYS.value, &pls,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  } else
    posting_list->get(label::ALL_POSTINGS_KEYS.label,
                      label::ALL_POSTINGS_KEYS.value, &pls);

  std::vector<uint64_t> mem_ids;
  mem_ids.reserve(pls.size());
  for (size_t i = 0; i < pls.size(); i++) {
    if ((indirection_manager_.read_slot(pls[i]) >> 63) == 0)
      mem_ids.push_back(pls[i]);
  }
  std::sort(mem_ids.begin(), mem_ids.end(), [&](uint64_t lhs, uint64_t rhs) {
    uint64_t lp = indirection_manager_.read_slot(lhs);
    uint64_t rp = indirection_manager_.read_slot(rhs);
    return ((MemSeries*)(lp))->access_epoch.load() >
           ((MemSeries*)(rp))->access_epoch.load();
  });

  for (int i = 0; i < mem_ids.size() * percentage / 100; i++) {
    uint64_t p = indirection_manager_.read_slot(mem_ids[i]);
    MemSeries* ms = (MemSeries*)(p);
    mem::write_lock_or_restart(ms);
    ms->lock();
    // We need to clean the data samples.
    if (ms->num_samples_ > 0) ms->_flush(db_, ms->flushed_txn_.load());
    ms->unlock();
    mem::write_unlock(ms);
  }

  return mem_ids.size() * percentage / 100;
}

/******************** Concurrency ********************/
void Head::enable_migration() {
  migration_enabled_ = true;
  migration_running_ =
      (mem::AlignedBool*)aligned_alloc(64, sizeof(mem::AlignedBool));
  migration_running_->reset();

  migration_running_->set(true);
  std::thread migration(_head_mem_to_disk_migration, this);
  migration.detach();
}

void Head::disable_migration() { migration_running_->set(false); }

void Head::full_migrate() {
  uint64_t current_epoch = global_epoch_->load();
  posting_list->full_migrate(current_epoch, &gc_requests_);
  memseries_gc(100, current_epoch, &gc_requests_);
  posting_list->art_gc(100, current_epoch, nullptr, &gc_requests_);
}

void _head_mem_to_disk_migration(Head* h) {
  uint64_t current_epoch;
  double vm, rss;
  h->wg_.add(1);
  while (h->migration_running_->get()) {
    mem_usage(vm, rss);
    if (rss * 1024 > h->mem_to_disk_migration_threshold_.load()) {
      current_epoch = h->global_epoch_->load();
      h->posting_list->try_migrate(current_epoch, &h->gc_requests_);
      // h->memseries_gc_preflush(10);
      // // Wait till no imm.
      // while (true) {
      //   leveldb::MutexLock l(h->db_->mutex());
      //   if (h->db_->imms()->empty())
      //     break;
      //   sleep(1);
      // }
      h->memseries_gc(10, current_epoch, &h->gc_requests_);

      h->posting_list->art_gc(10, current_epoch, nullptr, &h->gc_requests_);
    }

    usleep(GLOBAL_EPOCH_INCR_INTERVAL * 20);
  }
  printf("_head_mem_to_disk_migration exits\n");
  h->wg_.done();
}

/******************** Concurrency ********************/
void Head::enable_concurrency() {
  concurrency_enabled_ = true;
  for (int i = 0; i < MAX_WORKER_NUM; i++) {
    local_epochs_[i] = (mem::Epoch*)aligned_alloc(64, sizeof(mem::Epoch));
    local_epochs_[i]->reset();
    spinlocks_[i] = (mem::SpinLock*)aligned_alloc(64, sizeof(mem::SpinLock));
    spinlocks_[i]->reset();
    running_[i] =
        (mem::AlignedBool*)aligned_alloc(64, sizeof(mem::AlignedBool));
    running_[i]->reset();
  }
  global_epoch_ = (mem::Epoch*)aligned_alloc(64, sizeof(mem::Epoch));
  global_epoch_->reset();
  // gc_running_ = (mem::AlignedBool*)aligned_alloc(64,
  // sizeof(mem::AlignedBool)); gc_running_->reset();
  global_running_ =
      (mem::AlignedBool*)aligned_alloc(64, sizeof(mem::AlignedBool));
  global_running_->reset();

  global_running_->set(true);
  std::thread global_gc(_head_global_garbage_collection, this);
  global_gc.detach();
}

leveldb::Status Head::register_thread(int* idx) {
  for (int i = 0; i < MAX_WORKER_NUM; i++) {
    if (running_[i]->cas(false, true)) {
      *idx = i;
      return leveldb::Status::OK();
    }
  }
  return leveldb::Status::NotFound("no available slot");
}

void Head::deregister_thread(int idx) {
  running_[idx]->set(false);
  local_epochs_[idx]->store(0x8000000000000000);
}

void Head::update_local_epoch(int idx) {
  local_epochs_[idx]->store(global_epoch_->load());
}

void _head_global_garbage_collection(Head* h) {
  h->wg_.add(1);

  mem::GCRequest garbage;
  uint64_t min_local_epoch;
  while (h->global_running_->get()) {
    if (h->global_epoch_->load() == 0xFFFFFFFFFFFFFFFFul) {
      // Need to wrap around.
      h->global_epoch_->store(0);
      bool ready = false;
      while (!ready) {
        ready = true;
        for (int i = 0; i < MAX_WORKER_NUM; i++) {
          if (h->running_[i]->get() && h->local_epochs_[i]->load() != 0) {
            ready = false;
            break;
          }
        }
        usleep(GLOBAL_EPOCH_INCR_INTERVAL);
      }

      // Lock all running threads.
      for (int i = 0; i < MAX_WORKER_NUM; i++) {
        if (h->running_[i]->get()) h->spinlocks_[i]->lock();
      }

      // Clean the garbage from the last term.
      void* g = nullptr;
      while (h->gc_requests_.try_dequeue(garbage)) {
        if (g == garbage.garbage_) {
          h->gc_requests_.enqueue(garbage);
          break;
        }
        if (garbage.epoch_ != 0) {
          delete garbage.garbage_;  // TODO: garbage handling.
          ++_garbage_counter;
        } else {
          if (g == nullptr) g = garbage.garbage_;
          h->gc_requests_.enqueue(garbage);
        }
      }

      for (int i = 0; i < MAX_WORKER_NUM; i++) {
        if (h->running_[i]->get()) h->spinlocks_[i]->unlock();
      }
    } else {
      min_local_epoch = h->global_epoch_->load();
      h->global_epoch_->increment();

      for (int i = 0; i < MAX_WORKER_NUM; i++) {
        if (h->running_[i]->get() &&
            h->local_epochs_[i]->load() < min_local_epoch)
          min_local_epoch = h->local_epochs_[i]->load();
      }

      // LOG_DEBUG << "min_local_epoch:" << min_local_epoch << " global_epoch_:"
      // << h->global_epoch_->load();

      void* g = nullptr;
      while (h->gc_requests_.try_dequeue(garbage)) {
        if (g == garbage.garbage_) {
          h->gc_requests_.enqueue(garbage);
          break;
        }
        // LOG_DEBUG << "type:" << garbage.type_ << " epoch:" << garbage.epoch_
        // << " min_local_epoch:" << min_local_epoch << " ptr:" <<
        // (uint64_t)((uintptr_t)(garbage.garbage_));
        if (garbage.epoch_ < min_local_epoch) {
          delete garbage.garbage_;  // TODO: garbage handling.
          ++_garbage_counter;
        } else {
          if (g == nullptr) g = garbage.garbage_;
          h->gc_requests_.enqueue(garbage);
        }
      }
    }
    usleep(GLOBAL_EPOCH_INCR_INTERVAL);
  }
  h->wg_.done();
}

}  // namespace head
}  // namespace tsdb