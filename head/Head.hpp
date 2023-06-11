#ifndef HEAD_HPP
#define HEAD_HPP

#include <stdint.h>

#include <unordered_map>
#include <unordered_set>

#include "base/Atomic.hpp"
#include "base/Error.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "base/WaitGroup.hpp"
#include "db/AppenderInterface.hpp"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "disk/log_manager.h"
#include "index/MemPostings.hpp"
#include "leveldb/db.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/env.h"
#include "mem/go_art.h"
#include "mem/indirection_manager.h"
#include "mem/inverted_index.h"
#include "third_party/art.h"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace head {

extern std::string HEAD_INDEX_LOG_NAME;
extern std::string HEAD_SAMPLES_LOG_NAME;
extern std::string HEAD_FLUSHES_LOG_NAME;
extern size_t MAX_HEAD_SAMPLES_LOG_SIZE;

extern std::string HEAD_INDEX_SNAPSHOT_NAME;

extern uint64_t MEM_TO_DISK_MIGRATION_THRESHOLD;
extern size_t HEAD_SAMPLES_LOG_CLEANING_THRES;

extern uint64_t _garbage_counter;
extern std::atomic<uint64_t> _ms_gc_counter;

class Head;
void _head_mem_to_disk_migration(Head* h);
void _head_global_garbage_collection(Head* h);

// Head handles reads and writes of time series data within a time window.
// TODO(Alec), add metrics to monitor Head.
class Head {
 private:
  friend class HeadAppender;
  friend class HeadChunkReader;
  friend class HeadIndexReader;
  friend class HeadIndexWriter;

  std::atomic<int> cur_index_log_seq_;
  std::unique_ptr<leveldb::WritableFile> tags_log_file_;
  std::unique_ptr<leveldb::log::Writer> tags_log_writer_;

  std::atomic<int> cur_samples_log_seq_;
  std::unique_ptr<leveldb::WritableFile> samples_log_file_;
  std::unique_ptr<leveldb::log::Writer> samples_log_writer_;
  disk::SpinLock samples_log_lock_;

  // For flush marks.
  std::atomic<int> cur_flushes_log_seq_;
  std::unique_ptr<leveldb::WritableFile> flushes_log_file_;
  std::unique_ptr<leveldb::log::Writer> flushes_log_writer_;
  disk::SpinLock flushes_log_lock_;

  std::vector<leveldb::RandomRWFile*> tags_log_files_;
  std::vector<leveldb::log::RandomReader*> tags_log_readers_;
  std::vector<leveldb::log::RandomWriter*> tags_log_writers_;
  disk::SpinLock tags_log_lock_;

  std::atomic<int> active_samples_logs_;  // number of active samples logs.

  base::AtomicInt64 min_time;
  base::AtomicInt64 max_time;
  base::AtomicInt64 valid_time;  // Shouldn't be lower than the max_time of the
                                 // last persisted block
  std::atomic<uint64_t> last_series_id;

  mem::IndirectionManager indirection_manager_;

  base::RWMutexLock mutex_;
  mem::art_tree symbols_;

  // std::unique_ptr<index::MemPostings> posting_list;
  std::unique_ptr<mem::InvertedIndex> posting_list;

  // std::vector<std::unordered_map<uint64_t, uint64_t>> hash_shards_;
  std::vector<std::unordered_map<uint64_t, std::vector<uint64_t>>> hash_shards_;
  mem::SpinLock* hash_locks_;

  error::Error err_;

  std::string dir_;
  leveldb::DB* db_;

  bool sync_api_;
  bool no_log_;

  std::atomic<bool> bg_clean_samples_logs_;

  leveldb::Status recover_index_from_log(
      std::unordered_map<uint64_t, MemSeries*>* id_map);
  leveldb::Status recover_samples_from_log(
      std::unordered_map<uint64_t, MemSeries*>* id_map);
  leveldb::Status enable_tags_logs();
  void recover_memseries_from_indirection(
      std::unordered_map<uint64_t, MemSeries*>* id_map);

  leveldb::Status load_mem_series(uint64_t pos, uint64_t* physical_id,
                                  label::Labels* lset, int64_t* flushed_txn,
                                  int64_t* log_clean_txn);
  MemSeries* reload_mem_series(uint64_t logical_id, uint64_t series_pointer);
  MemSeries* check_reload_mem_series(uint64_t physical_id, uint64_t logical_id,
                                     uint64_t series_pointer);

  MemSeries* get_by_hash(uint64_t hash, const label::Labels& lset);
  void set_by_hash(uint64_t hash, uint64_t logical_id);

  /******************** Concurrency ********************/
  bool concurrency_enabled_;
  mem::Epoch* local_epochs_[MAX_WORKER_NUM];
  mem::SpinLock* spinlocks_[MAX_WORKER_NUM];
  mem::AlignedBool* running_[MAX_WORKER_NUM];
  mem::Epoch* global_epoch_;
  moodycamel::ConcurrentQueue<mem::GCRequest> gc_requests_;
  // mem::AlignedBool* gc_running_;
  mem::AlignedBool* global_running_;
  base::WaitGroup wg_;
  friend void _head_global_garbage_collection(Head* h);

  /******************** Memory to disk migration ********************/
  std::atomic<uint64_t> mem_to_disk_migration_threshold_;
  bool migration_enabled_;
  mem::AlignedBool* migration_running_;
  friend void _head_mem_to_disk_migration(Head* h);

 public:
  Head(const std::string& dir, const std::string& snapshot_dir = "",
       leveldb::DB* db = nullptr, bool sync = false);

  void update_min_max_time(int64_t mint, int64_t maxt);

  std::unique_ptr<db::AppenderInterface> head_appender();

  std::unique_ptr<db::AppenderInterface> appender();

  int64_t MinTime() const {
    return const_cast<base::AtomicInt64*>(&min_time)->get();
  }
  int64_t MaxTime() const {
    return const_cast<base::AtomicInt64*>(&max_time)->get();
  }

  // init_time initializes a head with the first timestamp. This only needs to
  // be called for a completely fresh head with an empty WAL. Returns true if
  // the initialization took an effect.
  bool init_time(int64_t t);

  bool overlap_closed(int64_t mint, int64_t maxt) const {
    // The block itself is a half-open interval
    // [pb.meta.MinTime, pb.meta.MaxTime).
    return MinTime() <= maxt && mint < MaxTime();
  }

  std::pair<MemSeries*, bool> get_or_create(const label::Labels& lset,
                                            uint64_t epoch = 0);
  std::pair<MemSeries*, bool> get_or_create(label::Labels&& lset,
                                            uint64_t epoch = 0);

  std::pair<MemSeries*, bool> get_or_create_with_id(uint64_t id,
                                                    const label::Labels& lset,
                                                    uint64_t epoch = 0);

  error::Error error() const { return err_; }

  leveldb::Status write_flush_marks(
      const std::vector<::leveldb::log::RefFlush>& marks);

  leveldb::Status clean_samples_logs();
  void bg_clean_samples_logs();
  void bg_stop_clean_samples_logs() { bg_clean_samples_logs_.store(false); }

  leveldb::Status try_extend_tags_logs();
  leveldb::Status try_extend_samples_logs();
  leveldb::Status try_extend_flushes_logs();

  leveldb::Status snapshot_index();

  int memseries_gc(
      int percentage, uint64_t epoch = 0,
      moodycamel::ConcurrentQueue<mem::GCRequest>* gc_queue = nullptr);
  int memseries_gc_preflush(int percentage);

  void full_migrate();

  void set_inverted_index_gc_threshold(uint64_t mem_threshold) {
    posting_list->set_mem_threshold(mem_threshold);
  }
  int inverted_index_gc() { return posting_list->try_migrate(); }
  mem::InvertedIndex* inverted_index() { return posting_list.get(); }

  void set_mergeset_manager(const std::string& dir, leveldb::Options opts) {
    posting_list->set_mergeset_manager(dir, opts);
  }

  ~Head();

  void set_no_log(bool v) { no_log_ = v; }
  bool no_log() { return no_log_; }

  mem::IndirectionManager* indirection_manager() { return &indirection_manager_; }

  /******************** index reader ********************/
  std::set<std::string> symbols();
  std::vector<std::string> label_values(const std::string& name);
  std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string& name, const std::string& value) const;

  // ref is logical id.
  bool series(uint64_t ref, label::Labels& lset, std::string* chunk_contents);
  std::vector<std::string> label_names() const;
  std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface>&& p);

  std::unique_ptr<index::PostingsInterface> select(
      const std::vector<::tsdb::label::MatcherInterface*>& l);
  uint64_t physical_id(uint64_t logical_id);

  /******************** Concurrency ********************/
  void enable_concurrency();
  leveldb::Status register_thread(int* idx);
  void deregister_thread(int idx);
  void update_local_epoch(int idx);
  uint64_t get_epoch(int idx) { return local_epochs_[idx]->load(); }

  /******************** Memory to disk migration ********************/
  void set_mem_to_disk_migration_threshold(size_t s) {
    mem_to_disk_migration_threshold_.store(s);
    posting_list->set_mem_threshold(s);
  }
  void enable_migration();
  void disable_migration();
};

}  // namespace head
}  // namespace tsdb

#endif