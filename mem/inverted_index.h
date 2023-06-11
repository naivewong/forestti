#pragma once

#include <atomic>
#include <queue>

#include "base/WaitGroup.hpp"
#include "db/db_impl.h"
#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"
#include "label/MatcherInterface.hpp"
#include "leveldb/db.h"
#include "mem/mem_postings.h"
#include "third_party/moodycamel/blockingconcurrentqueue.h"
#include "third_party/moodycamel/concurrentqueue.h"

#define MAX_WORKER_NUM 8
#define MAX_MIGRATION_WORKER_NUM 4
#define MAX_GC_WORKER_NUM 4

// Controller will add a new thread when exceeding this threshold.
#define WORKER_REQUEST_QUEUE_THRES 64

#define GLOBAL_EPOCH_INCR_INTERVAL 1000  // Microseconds

#define MEMORY_THRESHOLD 1 * 1024 * 1024 * 1024  // 1 GB

namespace tsdb {
namespace mem {

extern std::atomic<uint64_t> _pl_gc_counter;
extern std::atomic<uint64_t> _art_gc_counter;

/************************* For SEDA. *************************/
class InvertedIndex;
enum WorkerRequestType {
  GET = 1,
  GET_SINGLE,
  SELECT,
  PUT,
  OWN_PUT,
  DEL,
  OWN_DEL
};

struct WorkerRequest {
  WorkerRequestType type_;
};

struct WorkerGetRequest {
  WorkerRequest wr_;
  const label::Labels* matchers_;
  std::vector<std::vector<uint64_t>>* lists_;
  void (*callback_)(void* arg);
  void* callback_arg_;
};

struct WorkerGetSingleRequest {
  WorkerRequest wr_;
  const std::string* name_;
  const std::string* value_;
  std::vector<uint64_t>* list_;
  void (*callback_)(void* arg);
  void* callback_arg_;
};

struct WorkerSelectRequest {
  WorkerRequest wr_;
  const std::vector<label::MatcherInterface*>* matchers_;
  std::vector<std::vector<uint64_t>>* lists_;
  void (*callback_)(void* arg);
  void* callback_arg_;
};

struct WorkerPutRequest {
  WorkerRequest wr_;
  uint64_t id_;
  const label::Labels* lset_;
  void (*callback_)(void* arg);
  void* callback_arg_;
};

struct WorkerOwnPutRequest {
  WorkerRequest wr_;
  uint64_t id_;
  label::Labels lset_;
  void (*callback_)(void* arg);
  void* callback_arg_;

  WorkerOwnPutRequest(uint64_t id, label::Labels&& lset,
                      void (*callback)(void*), void* arg)
      : id_(id),
        lset_(std::move(lset)),
        callback_(callback),
        callback_arg_(arg) {
    wr_.type_ = OWN_PUT;
  }
};

struct WorkerDelRequest {
  WorkerRequest wr_;
  uint64_t id_;
  const label::Label* l_;
  void (*callback_)(void* arg);
  void* callback_arg_;
};

struct WorkerOwnDelRequest {
  WorkerRequest wr_;
  uint64_t id_;
  label::Label l_;
  void (*callback_)(void* arg);
  void* callback_arg_;

  WorkerOwnDelRequest(uint64_t id, label::Label&& l, void (*callback)(void*),
                      void* arg)
      : id_(id), l_(std::move(l)), callback_(callback), callback_arg_(arg) {
    wr_.type_ = OWN_DEL;
  }
};

enum GCRequestType {
  ART_NODE4 = 1,
  ART_NODE16,
  ART_NODE48,
  ART_NODE256,
  ART_LEAF,
  FLAT_NODE,
  ART_NODE,
  POSTINGS,
  MEMSERIES
};

struct GCRequest {
  uint64_t epoch_;
  void* garbage_;
  GCRequestType type_;
  GCRequest() = default;
  GCRequest(uint64_t e, void* g, GCRequestType t)
      : epoch_(e), garbage_(g), type_(t) {}
};

struct alignas(64) Epoch {
  std::atomic<uint64_t> epoch_;
  Epoch() : epoch_(0) {}
  inline void reset() { epoch_ = 0; }
  inline void store(uint64_t e) { epoch_.store(e); }
  inline uint64_t load() { return epoch_.load(); }
  inline void increment() { ++epoch_; }
};

struct alignas(64) SpinLock {
  std::atomic<bool> lock_;

  SpinLock() : lock_(false) {}
  void lock() noexcept;
  void unlock() noexcept;
  inline void reset() { lock_ = false; }
};

struct alignas(64) AlignedBool {
  std::atomic<bool> v_;
  AlignedBool() : v_(false) {}
  inline void set(bool v) { v_.store(v); }
  inline bool get() { return v_.load(); }
  inline void reset() { v_ = false; }
  inline bool cas(bool exp, bool desired) {
    return v_.compare_exchange_weak(exp, desired);
  }
};

struct alignas(128) AlignedQueue {
  std::queue<void*> q_;
};

struct alignas(256) AlignedLFQueue {
  moodycamel::BlockingConcurrentQueue<void*> q_;
};

void worker_thread(InvertedIndex* db, Epoch* global_epoch, Epoch* local_epoch,
                   SpinLock* sl, AlignedBool* running,
                   moodycamel::BlockingConcurrentQueue<WorkerRequest*>* rq,
                   moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                   mem::ARTObjectPool* art_object_pool,
                   mem::MemPostingsObjectPool* m_pool,
                   bool keep_running = false);
void gc_absorb_thread(AlignedBool* running, SpinLock* sl,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                      moodycamel::ConcurrentQueue<GCRequest>** current,
                      moodycamel::ConcurrentQueue<GCRequest>** next,
                      base::WaitGroup* wg);
void gc_handle_thread(AlignedBool* running, Epoch** local_epochs, SpinLock* sl,
                      moodycamel::ConcurrentQueue<GCRequest>** current,
                      mem::ARTObjectPool* art_object_pool,
                      mem::MemPostingsObjectPool* m_pool, base::WaitGroup* wg);
void global_gc_thread(AlignedBool* running, Epoch* global_epoch,
                      Epoch** local_epochs,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_requests,
                      moodycamel::ConcurrentQueue<GCRequest>** current,
                      moodycamel::ConcurrentQueue<GCRequest>** next,
                      SpinLock** worker_locks, SpinLock** gc_absorb_locks,
                      SpinLock** gc_handle_locks,
                      mem::ARTObjectPool* art_object_pool,
                      mem::MemPostingsObjectPool* m_pool, base::WaitGroup* wg);

void normal_worker_controller(
    InvertedIndex* db, AlignedBool* running, Epoch* global_epoch,
    Epoch** local_epoch, SpinLock** sl, AlignedBool* worker_running,
    moodycamel::BlockingConcurrentQueue<WorkerRequest*>* rq,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
    mem::ARTObjectPool* art_object_pool, mem::MemPostingsObjectPool* m_pool);

void migration_worker(InvertedIndex* db, AlignedBool* running);

class InvertedIndex {
 public:
  InvertedIndex(const std::string& dir, const std::string& snapshot_dir = "",
                uint64_t mem_threshold = MEMORY_THRESHOLD);
  ~InvertedIndex();

  void add(uint64_t id, const label::Labels& ls);
  void del(uint64_t id, const label::Label& l);

  void set_mem_threshold(uint64_t mem_threshold) {
    mem_threshold_ = mem_threshold;
  }

  void set_mergeset_manager(const std::string& dir, leveldb::Options opts) {
    mem_store_.set_mergeset_manager(dir, opts);
  }

  // For postings.
  int try_migrate(uint64_t epoch = 0,
                  moodycamel::ConcurrentQueue<GCRequest>* gc_queue = nullptr);
  // Used for testing.
  int full_migrate(uint64_t epoch = 0,
                  moodycamel::ConcurrentQueue<GCRequest>* gc_queue = nullptr);

  int art_gc(double percentage, uint64_t epoch = 0,
             ARTObjectPool* art_object_pool = nullptr,
             moodycamel::ConcurrentQueue<GCRequest>* gc_queue = nullptr) {
    int c = mem_store_.art_gc(percentage, epoch, art_object_pool, gc_queue);
    _art_gc_counter.fetch_add(c);
    return c;
  }

  std::unique_ptr<index::PostingsInterface> get(const std::string& name,
                                                const std::string& value);
  std::unique_ptr<index::PostingsInterface> get(const label::Labels& lset);
  void get(const std::string& name, const std::string& value,
           std::vector<uint64_t>* list);
  void get(const label::Labels& lset,
           std::vector<std::vector<uint64_t>>* lists);
  std::unique_ptr<index::PostingsInterface> select(
      const std::vector<label::MatcherInterface*>& l);
  void select(const std::vector<label::MatcherInterface*>& l,
              std::vector<std::vector<uint64_t>>* lists);
  void select(const std::vector<label::MatcherInterface*>& l,
              std::vector<uint64_t>* list);  // Pre-intersection.

  mem::MemPostings* mem_postings() { return &mem_store_; }

  leveldb::Status snapshot_index(const std::string& dir) {
    return mem_store_.snapshot_index(dir);
  }

  ART_NS::art_tree* get_tag_keys_trie() {
    return mem_store_.get_tag_keys_trie();
  }
  void get_values(const std::string& key, std::vector<std::string>* values) {
    return mem_store_.get_values(key, values);
  }

  /************************* For SEDA & GC *************************/
  // Async APIs for users.
  void async_add(uint64_t id, const label::Labels* ls,
                 void (*callback)(void* arg) = nullptr, void* arg = nullptr);
  void async_del(uint64_t id, const label::Label* l,
                 void (*callback)(void* arg) = nullptr, void* arg = nullptr);
  void async_add(uint64_t id, label::Labels&& ls,
                 void (*callback)(void* arg) = nullptr, void* arg = nullptr);
  void async_del(uint64_t id, label::Label&& l,
                 void (*callback)(void* arg) = nullptr, void* arg = nullptr);
  void async_get(const label::Labels* lset,
                 std::vector<std::vector<uint64_t>>* lists,
                 void (*callback)(void* arg) = nullptr, void* arg = nullptr);
  void async_get_single(const std::string* name, const std::string* value,
                        std::vector<uint64_t>* list,
                        void (*callback)(void* arg) = nullptr,
                        void* arg = nullptr);
  void async_select(const std::vector<label::MatcherInterface*>* l,
                    std::vector<std::vector<uint64_t>>* lists,
                    void (*callback)(void* arg) = nullptr, void* arg = nullptr);

  // Async accessing functions for worker pool.
  void _add_job(uint64_t id, const label::Labels* ls, uint64_t epoch,
                mem::ARTObjectPool* art_object_pool,
                mem::MemPostingsObjectPool* m_pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                void (*callback)(void* arg), void* arg);
  void _del_job(uint64_t id, const label::Label* l, uint64_t epoch,
                mem::ARTObjectPool* art_object_pool,
                mem::MemPostingsObjectPool* m_pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                void (*callback)(void* arg), void* arg);
  void _get_job(const label::Labels* lset,
                std::vector<std::vector<uint64_t>>* lists,
                void (*callback)(void* arg), void* arg);
  void _get_single_job(const std::string* name, const std::string* value,
                       std::vector<uint64_t>* list, void (*callback)(void* arg),
                       void* arg);
  void _select_job(const std::vector<label::MatcherInterface*>* l,
                   std::vector<std::vector<uint64_t>>* lists,
                   void (*callback)(void* arg), void* arg);

  int num_running_workers();

  void start_all_stages();
  void stop_all_stages();

  base::WaitGroup wg_;

 private:
  mem::MemPostings mem_store_;
  std::string postings_dir_;
  uint64_t mem_threshold_;

  /************************* For SEDA. *************************/
  // Normal workers.
  Epoch* local_epochs_[MAX_WORKER_NUM];
  SpinLock* spinlocks_[MAX_WORKER_NUM];
  AlignedBool running_[MAX_WORKER_NUM];
  moodycamel::BlockingConcurrentQueue<WorkerRequest*> worker_requests_;

  // Migration workers.
  AlignedBool running_migration_[MAX_MIGRATION_WORKER_NUM];

  // GC workers.
  mem::ARTObjectPool art_object_pool_;
  mem::MemPostingsObjectPool mem_postings_object_pool_;
  moodycamel::ConcurrentQueue<GCRequest>*
      current_epoch_garbage_[MAX_GC_WORKER_NUM];
  moodycamel::ConcurrentQueue<GCRequest>*
      next_epoch_garbage_[MAX_GC_WORKER_NUM];
  moodycamel::ConcurrentQueue<GCRequest> gc_requests_;
  SpinLock* gc_absorb_spinlocks_[MAX_GC_WORKER_NUM];
  SpinLock* gc_handle_spinlocks_[MAX_GC_WORKER_NUM];
  AlignedBool running_absorbers_[MAX_GC_WORKER_NUM];
  AlignedBool running_handlers_[MAX_GC_WORKER_NUM];

  // Global GC worker.
  Epoch global_epoch_;
  AlignedBool global_gc_running_;

  // Normal worker controller.
  AlignedBool worker_controller_running_;

  // Migration worker.
  AlignedBool migration_worker_running_;

  int worker_num_;
  int gc_num_;

  bool seda_started_;
};

}  // namespace mem
}  // namespace tsdb