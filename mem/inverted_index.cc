#include "mem/inverted_index.h"

#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <thread>

// #include "base/Logging.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IntersectPostings.hpp"
#include "index/VectorPostings.hpp"
#include "mem/mmap.h"

namespace tsdb {
namespace mem {

std::atomic<uint64_t> _pl_gc_counter(0);
std::atomic<uint64_t> _art_gc_counter(0);

InvertedIndex::InvertedIndex(const std::string& dir,
                             const std::string& snapshot_dir,
                             uint64_t mem_threshold)
    : postings_dir_(dir), mem_threshold_(mem_threshold), seda_started_(false) {
  if (!snapshot_dir.empty())
    mem_store_.recover_from_snapshot(snapshot_dir, dir);
  else
    mem_store_.set_log(dir);
}

InvertedIndex::~InvertedIndex() {
  if (seda_started_) {
    for (int i = 0; i < MAX_WORKER_NUM; i++) {
      free(local_epochs_[i]);
      free(spinlocks_[i]);
    }
    for (int i = 0; i < MAX_GC_WORKER_NUM; i++) {
      free(gc_absorb_spinlocks_[i]);
      free(gc_handle_spinlocks_[i]);
      delete current_epoch_garbage_[i];
      delete next_epoch_garbage_[i];
    }
  }
}

void InvertedIndex::add(uint64_t id, const label::Labels& ls) {
  mem_store_.add(id, ls);
}

void InvertedIndex::del(uint64_t id, const label::Label& l) {
  bool deleted = mem_store_.del(id, l);
}

int InvertedIndex::try_migrate(
    uint64_t epoch, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  int scan_num = mem_store_.postings_list_num() / 10;
  if (scan_num == 0) return 0;

  std::vector<std::string> tn, tv;
  mem_store_.select_postings_for_gc(&tn, &tv, scan_num);
  // printf("try_migrate %d scan_num:%d\n", tn.size(), scan_num);
  mem_store_.postings_gc(&tn, &tv, epoch, gc_queue);
  _pl_gc_counter.fetch_add(tn.size());

  return tn.size();
}

int InvertedIndex::full_migrate(
    uint64_t epoch, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  int scan_num = mem_store_.postings_list_num() / 10;

  std::vector<std::string> tn, tv;
  mem_store_.select_postings_for_gc(&tn, &tv, scan_num);
  // printf("try_migrate %d scan_num:%d\n", tn.size(), scan_num);
  mem_store_.postings_gc(&tn, &tv, epoch, gc_queue);
  _pl_gc_counter.fetch_add(tn.size());

  return tn.size();
}

std::unique_ptr<index::PostingsInterface> InvertedIndex::get(
    const std::string& name, const std::string& value) {
  std::vector<uint64_t> list;
  mem_store_.get(name, value, &list);
  if (list.empty()) return nullptr;
  return std::unique_ptr<index::PostingsInterface>(
      new index::VectorPostings(std::move(list)));
}

std::unique_ptr<index::PostingsInterface> InvertedIndex::get(
    const label::Labels& lset) {
  std::vector<std::unique_ptr<index::PostingsInterface>> pvec;
  pvec.reserve(lset.size());
  for (const auto& l : lset) {
    std::vector<uint64_t> list;
    mem_store_.get(l.label, l.value, &list);
    if (list.empty()) return nullptr;
    pvec.push_back(std::unique_ptr<index::PostingsInterface>(
        new index::VectorPostings(std::move(list))));
  }
  return std::unique_ptr<index::PostingsInterface>(
      new index::IntersectPostings1(std::move(pvec)));
}

void InvertedIndex::get(const std::string& name, const std::string& value,
                        std::vector<uint64_t>* list) {
  mem_store_.get(name, value, list);
}

void InvertedIndex::get(const label::Labels& lset,
                        std::vector<std::vector<uint64_t>>* lists) {
  for (size_t i = 0; i < lset.size(); i++) {
    mem_store_.get(lset[i].label, lset[i].value, &lists->at(i));
    if (lists->at(i).empty()) return;
  }
}

std::unique_ptr<index::PostingsInterface> InvertedIndex::select(
    const std::vector<label::MatcherInterface*>& l) {
  if (l.empty())
    return std::unique_ptr<index::PostingsInterface>(
        new index::EmptyPostings());
  else if (l.size() == 1)
    return get(l.front()->name(), l.front()->value());
  else {
    // std::vector<std::unique_ptr<index::PostingsInterface>> p;
    // p.reserve(l.size());
    // for (label::MatcherInterface* m : l)
    //   p.push_back(get(m->name(), m->value()));
    // return std::unique_ptr<index::PostingsInterface>(new
    // index::IntersectPostings1(std::move(p)));
    std::vector<postings_list*> lists;
    for (label::MatcherInterface* m : l) {
      postings_list* pl = nullptr;
      mem_store_.get(m->name(), m->value(), &pl);
      if (pl == nullptr)
        return std::unique_ptr<index::PostingsInterface>(
            new index::EmptyPostings());
      lists.push_back(pl);
    }
    std::vector<uint64_t> v;
    intersect_postings_lists(std::move(lists), &v);
    return std::unique_ptr<index::PostingsInterface>(
        new index::VectorPostings(std::move(v)));
  }
}

void InvertedIndex::select(const std::vector<label::MatcherInterface*>& l,
                           std::vector<std::vector<uint64_t>>* lists) {
  for (size_t i = 0; i < l.size(); i++) {
    mem_store_.get(l[i]->name(), l[i]->value(), &lists->at(i));
    if (lists->at(i).empty()) break;  // Quick break.
  }
}

void InvertedIndex::select(const std::vector<label::MatcherInterface*>& l,
                           std::vector<uint64_t>* list) {
  std::vector<postings_list*> lists;
  for (label::MatcherInterface* m : l) {
    postings_list* pl;
    mem_store_.get(m->name(), m->value(), &pl);
    lists.push_back(pl);
  }
  intersect_postings_lists(std::move(lists), list);
}

/************************* For SEDA. *************************/
void InvertedIndex::async_add(uint64_t id, const label::Labels* ls,
                              void (*callback)(void* arg), void* arg) {
  WorkerPutRequest* req = new WorkerPutRequest();
  req->wr_.type_ = PUT;
  req->id_ = id;
  req->lset_ = ls;
  req->callback_ = callback;
  req->callback_arg_ = arg;
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::async_add(uint64_t id, label::Labels&& ls,
                              void (*callback)(void* arg), void* arg) {
  WorkerOwnPutRequest* req =
      new WorkerOwnPutRequest(id, std::move(ls), callback, arg);
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::async_del(uint64_t id, const label::Label* l,
                              void (*callback)(void* arg), void* arg) {
  WorkerDelRequest* req = new WorkerDelRequest();
  req->wr_.type_ = DEL;
  req->id_ = id;
  req->l_ = l;
  req->callback_ = callback;
  req->callback_arg_ = arg;
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::async_del(uint64_t id, label::Label&& l,
                              void (*callback)(void* arg), void* arg) {
  WorkerOwnDelRequest* req =
      new WorkerOwnDelRequest(id, std::move(l), callback, arg);
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::async_get(const label::Labels* lset,
                              std::vector<std::vector<uint64_t>>* lists,
                              void (*callback)(void* arg), void* arg) {
  WorkerGetRequest* req = new WorkerGetRequest();
  req->wr_.type_ = GET;
  req->matchers_ = lset;
  req->lists_ = lists;
  req->callback_ = callback;
  req->callback_arg_ = arg;
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::async_get_single(const std::string* name,
                                     const std::string* value,
                                     std::vector<uint64_t>* list,
                                     void (*callback)(void* arg), void* arg) {
  WorkerGetSingleRequest* req = new WorkerGetSingleRequest();
  req->wr_.type_ = GET_SINGLE;
  req->name_ = name;
  req->value_ = value;
  req->list_ = list;
  req->callback_ = callback;
  req->callback_arg_ = arg;
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::async_select(const std::vector<label::MatcherInterface*>* l,
                                 std::vector<std::vector<uint64_t>>* lists,
                                 void (*callback)(void* arg), void* arg) {
  WorkerSelectRequest* req = new WorkerSelectRequest();
  req->wr_.type_ = SELECT;
  req->matchers_ = l;
  req->lists_ = lists;
  req->callback_ = callback;
  req->callback_arg_ = arg;
  worker_requests_.enqueue(reinterpret_cast<WorkerRequest*>(req));
}

void InvertedIndex::_select_job(const std::vector<label::MatcherInterface*>* l,
                                std::vector<std::vector<uint64_t>>* lists,
                                void (*callback)(void* arg), void* arg) {
  for (size_t i = 0; i < l->size(); i++) {
    mem_store_.get(l->at(i)->name(), l->at(i)->value(), &lists->at(i));
    if (lists->at(i).empty()) break;  // Quick break.
  }

  if (callback) callback(arg);
}

void InvertedIndex::_get_job(const label::Labels* lset,
                             std::vector<std::vector<uint64_t>>* lists,
                             void (*callback)(void* arg), void* arg) {
  for (size_t i = 0; i < lset->size(); i++) {
    mem_store_.get(lset->at(i).label, lset->at(i).value, &lists->at(i));
    if (lists->at(i).empty()) break;  // Quick break.
  }

  if (callback) callback(arg);
}

void InvertedIndex::_get_single_job(const std::string* name,
                                    const std::string* value,
                                    std::vector<uint64_t>* list,
                                    void (*callback)(void* arg), void* arg) {
  mem_store_.get(*name, *value, list);

  if (callback) callback(arg);
}

void InvertedIndex::_add_job(uint64_t id, const label::Labels* ls,
                             uint64_t epoch,
                             mem::ARTObjectPool* art_object_pool,
                             mem::MemPostingsObjectPool* m_pool,
                             moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                             void (*callback)(void* arg), void* arg) {
  // LOG_DEBUG << "---- " << label::lbs_string(*ls) << " " <<id;
  mem_store_.add(id, *ls, epoch, art_object_pool, m_pool, gc_queue);
  // LOG_DEBUG << "---- " << label::lbs_string(*ls) << " " <<id;

  if (callback) callback(arg);
}

void InvertedIndex::_del_job(uint64_t id, const label::Label* l, uint64_t epoch,
                             mem::ARTObjectPool* art_object_pool,
                             mem::MemPostingsObjectPool* m_pool,
                             moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                             void (*callback)(void* arg), void* arg) {
  bool deleted =
      mem_store_.del(id, *l, epoch, art_object_pool, m_pool, gc_queue);

  if (callback) callback(arg);
}

void InvertedIndex::start_all_stages() {
  seda_started_ = true;
  for (int i = 0; i < MAX_WORKER_NUM; i++) {
    local_epochs_[i] = (Epoch*)aligned_alloc(64, sizeof(Epoch));
    local_epochs_[i]->reset();
    spinlocks_[i] = (SpinLock*)aligned_alloc(64, sizeof(SpinLock));
    spinlocks_[i]->reset();
  }
  for (int i = 0; i < MAX_GC_WORKER_NUM; i++) {
    gc_absorb_spinlocks_[i] = (SpinLock*)aligned_alloc(64, sizeof(SpinLock));
    gc_absorb_spinlocks_[i]->reset();
    gc_handle_spinlocks_[i] = (SpinLock*)aligned_alloc(64, sizeof(SpinLock));
    gc_handle_spinlocks_[i]->reset();
    current_epoch_garbage_[i] = new moodycamel::ConcurrentQueue<GCRequest>();
    next_epoch_garbage_[i] = new moodycamel::ConcurrentQueue<GCRequest>();
  }

  // Normal worker.
  running_[0].set(true);
  std::thread normal_worker(worker_thread, this, &global_epoch_,
                            local_epochs_[0], spinlocks_[0], &running_[0],
                            &worker_requests_, &gc_requests_, &art_object_pool_,
                            &mem_postings_object_pool_, true);
  normal_worker.detach();

  // Migration worker.
  migration_worker_running_.set(true);
  std::thread migration_worker_thread(migration_worker, this,
                                      &migration_worker_running_);
  migration_worker_thread.detach();

  // GC absorber.
  running_absorbers_[0].set(true);
  std::thread gc_absorb(
      gc_absorb_thread, &running_absorbers_[0], gc_absorb_spinlocks_[0],
      &gc_requests_, &current_epoch_garbage_[0], &next_epoch_garbage_[0], &wg_);
  gc_absorb.detach();

  // GC handler.
  running_handlers_[0].set(true);
  std::thread gc_handle(gc_handle_thread, &running_handlers_[0], local_epochs_,
                        gc_handle_spinlocks_[0], &current_epoch_garbage_[0],
                        &art_object_pool_, &mem_postings_object_pool_, &wg_);
  gc_handle.detach();

  // Global GC worker.
  global_gc_running_.set(true);
  std::thread global_gc(global_gc_thread, &global_gc_running_, &global_epoch_,
                        local_epochs_, &gc_requests_, current_epoch_garbage_,
                        next_epoch_garbage_, spinlocks_, gc_absorb_spinlocks_,
                        gc_handle_spinlocks_, &art_object_pool_,
                        &mem_postings_object_pool_, &wg_);
  global_gc.detach();

  // Normal worker controller.
  worker_controller_running_.set(true);
  std::thread controller(
      normal_worker_controller, this, &worker_controller_running_,
      &global_epoch_, local_epochs_, spinlocks_, running_, &worker_requests_,
      &gc_requests_, &art_object_pool_, &mem_postings_object_pool_);
  controller.detach();
}

int InvertedIndex::num_running_workers() {
  int count = 0;
  for (int i = 0; i < MAX_WORKER_NUM; i++)
    if (running_[i].get()) ++count;
  return count;
}

void InvertedIndex::stop_all_stages() {
  // Normal worker & Migration worker.
  for (int i = 0; i < MAX_WORKER_NUM; i++) running_[i].set(false);

  // GC absorber.
  for (int i = 0; i < MAX_GC_WORKER_NUM; i++) running_absorbers_[i].set(false);

  // GC handler.
  for (int i = 0; i < MAX_GC_WORKER_NUM; i++) running_handlers_[i].set(false);

  // Global GC worker.
  global_gc_running_.set(false);

  // Normal worker controller.
  worker_controller_running_.set(false);

  // Migration worker.
  migration_worker_running_.set(false);
  wg_.wait();
}

void SpinLock::lock() noexcept {
  for (;;) {
    // Optimistically assume the lock is free on the first try
    if (!lock_.exchange(true, std::memory_order_acquire)) {
      return;
    }
    // Wait for lock to be released without generating cache misses
    while (lock_.load(std::memory_order_relaxed)) {
      // Issue X86 PAUSE or ARM YIELD instruction to reduce contention between
      // hyper-threads
      __builtin_ia32_pause();
    }
  }
}

void SpinLock::unlock() noexcept {
  lock_.store(false, std::memory_order_release);
}

void worker_thread(InvertedIndex* db, Epoch* global_epoch, Epoch* local_epoch,
                   SpinLock* sl, AlignedBool* running,
                   moodycamel::BlockingConcurrentQueue<WorkerRequest*>* rq,
                   moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                   mem::ARTObjectPool* art_object_pool,
                   mem::MemPostingsObjectPool* m_pool, bool keep_running) {
  WorkerRequest* req;
  WorkerSelectRequest* s_req;
  WorkerGetRequest* g_req;
  WorkerGetSingleRequest* gs_req;
  WorkerPutRequest* p_req;
  WorkerOwnPutRequest* op_req;
  WorkerDelRequest* d_req;
  WorkerOwnDelRequest* od_req;
  int idle_count = 0;
  db->wg_.add(1);
  while (running->get() && idle_count < 3) {
    if (!rq->wait_dequeue_timed(req, GLOBAL_EPOCH_INCR_INTERVAL * 10)) {
      if (!keep_running) idle_count++;
      continue;
    } else if (!keep_running)
      idle_count = 0;

    sl->lock();
    local_epoch->epoch_.store(global_epoch->epoch_.load());

    switch (req->type_) {
      case GET:
        g_req = reinterpret_cast<WorkerGetRequest*>(req);
        db->_get_job(g_req->matchers_, g_req->lists_, g_req->callback_,
                     g_req->callback_arg_);
        break;
      case GET_SINGLE:
        gs_req = reinterpret_cast<WorkerGetSingleRequest*>(req);
        db->_get_single_job(gs_req->name_, gs_req->value_, gs_req->list_,
                            gs_req->callback_, gs_req->callback_arg_);
        break;
      case SELECT:
        s_req = reinterpret_cast<WorkerSelectRequest*>(req);
        db->_select_job(s_req->matchers_, s_req->lists_, s_req->callback_,
                        s_req->callback_arg_);
        break;
      case PUT:
        p_req = reinterpret_cast<WorkerPutRequest*>(req);
        db->_add_job(p_req->id_, p_req->lset_, local_epoch->epoch_,
                     art_object_pool, m_pool, gc_queue, p_req->callback_,
                     p_req->callback_arg_);
        break;
      case OWN_PUT:
        op_req = reinterpret_cast<WorkerOwnPutRequest*>(req);
        db->_add_job(op_req->id_, &op_req->lset_, local_epoch->epoch_,
                     art_object_pool, m_pool, gc_queue, op_req->callback_,
                     op_req->callback_arg_);
        break;
      case DEL:
        d_req = reinterpret_cast<WorkerDelRequest*>(req);
        db->_del_job(d_req->id_, d_req->l_, local_epoch->epoch_,
                     art_object_pool, m_pool, gc_queue, d_req->callback_,
                     d_req->callback_arg_);
        break;
      case OWN_DEL:
        od_req = reinterpret_cast<WorkerOwnDelRequest*>(req);
        db->_del_job(od_req->id_, &od_req->l_, local_epoch->epoch_,
                     art_object_pool, m_pool, gc_queue, od_req->callback_,
                     od_req->callback_arg_);
        break;
    }

    sl->unlock();
    delete req;
  }

  running->set(false);
  local_epoch->epoch_.store(0xFFFFFFFFFFFFFFFF);
  db->wg_.done();
  // printf("done worker_thread\n");
}

void gc_absorb_thread(AlignedBool* running, SpinLock* sl,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
                      moodycamel::ConcurrentQueue<GCRequest>** current,
                      moodycamel::ConcurrentQueue<GCRequest>** next,
                      base::WaitGroup* wg) {
  wg->add(1);
  GCRequest req;
retry:
  while (running->get()) {
    sl->lock();
    if (!gc_queue->try_dequeue(req)) {
      sl->unlock();
      usleep(GLOBAL_EPOCH_INCR_INTERVAL * 10);
      goto retry;
    }

    if (req.epoch_ & 0x8000000000000000)
      (*next)->enqueue(req);
    else
      (*current)->enqueue(req);

    sl->unlock();
  }
  wg->done();
  // printf("done gc_absorb_thread\n");
}

void gc_handling(void* garbage, GCRequestType type,
                 mem::ARTObjectPool* art_object_pool,
                 mem::MemPostingsObjectPool* m_pool) {
  switch (type) {
#ifdef MMAP_ART_NODE
    case ART_NODE4:
      mmap_node4->reclaim(garbage);
      break;
    case ART_NODE16:
      mmap_node16->reclaim(garbage);
      break;
    case ART_NODE48:
      mmap_node48->reclaim(garbage);
      break;
    case ART_NODE256:
      mmap_node256->reclaim(garbage);
      break;
#else
    case ART_NODE4:
      art_object_pool->put_node4((mem::art_node4*)(garbage));
      break;
    case ART_NODE16:
      art_object_pool->put_node16((mem::art_node16*)(garbage));
      break;
    case ART_NODE48:
      art_object_pool->put_node48((mem::art_node48*)(garbage));
      break;
    case ART_NODE256:
      art_object_pool->put_node256((mem::art_node256*)(garbage));
      break;
#endif
    case ART_LEAF:
      free((mem::art_leaf*)(garbage));
      break;
    case FLAT_NODE:
      m_pool->put_flat_node((mem::tag_values_node_flat*)(garbage));
      break;
    case POSTINGS:
      m_pool->put_postings((mem::postings_list*)(garbage));
      break;
  }
}

void gc_handle_thread(AlignedBool* running, Epoch** local_epochs, SpinLock* sl,
                      moodycamel::ConcurrentQueue<GCRequest>** current,
                      mem::ARTObjectPool* art_object_pool,
                      mem::MemPostingsObjectPool* m_pool, base::WaitGroup* wg) {
  GCRequest garbage;
  uint64_t min_local_epoch;
  bool use_current_epoch = false;
  bool garbage_put_back = false;
  wg->add(1);
retry:
  while (running->get()) {
    sl->lock();
    if (garbage_put_back) {
      (*current)->enqueue(garbage);
      garbage_put_back = false;
    }
    if (!(*current)->try_dequeue(garbage)) {
      sl->unlock();
      usleep(GLOBAL_EPOCH_INCR_INTERVAL * 10);
      goto retry;
    }
    sl->unlock();

    if (!use_current_epoch) {
      min_local_epoch = std::numeric_limits<uint64_t>::max();
      for (int i = 0; i < MAX_WORKER_NUM; i++) {
        uint64_t local_epoch = local_epochs[i]->epoch_.load();
        if (local_epoch < min_local_epoch) min_local_epoch = local_epoch;
      }
    }
    if (garbage.epoch_ < min_local_epoch) {
      gc_handling(garbage.garbage_, garbage.type_, art_object_pool, m_pool);
      use_current_epoch = true;  // Save time for repeated comparison.
      garbage_put_back = false;
    } else {
      use_current_epoch = false;
      garbage_put_back = true;
    }
  }
  if (garbage_put_back) (*current)->enqueue(garbage);
  wg->done();
  // printf("done gc_handle_thread\n");
}

void global_gc_thread(AlignedBool* running, Epoch* global_epoch,
                      Epoch** local_epochs,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_requests,
                      moodycamel::ConcurrentQueue<GCRequest>** current,
                      moodycamel::ConcurrentQueue<GCRequest>** next,
                      SpinLock** worker_locks, SpinLock** gc_absorb_locks,
                      SpinLock** gc_handle_locks,
                      mem::ARTObjectPool* art_object_pool,
                      mem::MemPostingsObjectPool* m_pool, base::WaitGroup* wg) {
  wg->add(1);
  uint64_t tmp_local_epochs[MAX_WORKER_NUM];
  while (running->get()) {
    if ((global_epoch->epoch_ & 0x8000000000000000) == 0) {
      // The next epoch bit is not set.
      ++global_epoch->epoch_;
    } else {
      // printf("global_gc_thread %lu\n", global_epoch->epoch_);
      while (true) {
        bool all_in_next_epoch = true;
        for (int i = 0; i < MAX_WORKER_NUM; i++) {
          tmp_local_epochs[i] = local_epochs[i]->epoch_.load();
          if ((tmp_local_epochs[i] & 0x8000000000000000) == 0)
            all_in_next_epoch = false;
        }

        // Next epoch bit in all workers are set.
        if (all_in_next_epoch) {
          // 1. lock all workers to make them stop producing garbage.
          for (int i = 0; i < MAX_WORKER_NUM; i++) {
            if (tmp_local_epochs[i] == 0xFFFFFFFFFFFFFFFF)
              continue;  // Not running worker.
            worker_locks[i]->lock();
          }

          // 2. wait for the GC request queue to be empty.
          GCRequest garbage;
          while (gc_requests->try_dequeue(garbage)) {
            // Insert into the first garbage queues.
            if ((garbage.epoch_ & 0x8000000000000000) == 0)
              current[0]->enqueue(garbage);
            else
              next[0]->enqueue(garbage);
          }

          // 3. lock all gc_absorb.
          for (int i = 0; i < MAX_GC_WORKER_NUM; i++)
            gc_absorb_locks[i]->lock();

          // 4. wait for all current epoch garbage queues to be empty.
          uint64_t min_local_epoch = std::numeric_limits<uint64_t>::max();
          for (int i = 0; i < MAX_WORKER_NUM; i++) {
            uint64_t local_epoch = local_epochs[i]->epoch_.load();
            if (local_epoch < min_local_epoch) min_local_epoch = local_epoch;
          }
          for (int i = 0; i < MAX_GC_WORKER_NUM; i++) {
            while (current[i]->try_dequeue(garbage)) {
              if (garbage.epoch_ < min_local_epoch) {
                gc_handling(garbage.garbage_, garbage.type_, art_object_pool,
                            m_pool);
              }
            }
          }

          // 5. lock all gc_handle.
          for (int i = 0; i < MAX_GC_WORKER_NUM; i++)
            gc_handle_locks[i]->lock();

          // 6. Swap current and next epoch garbage queues.
          for (int i = 0; i < MAX_GC_WORKER_NUM; i++) {
            moodycamel::ConcurrentQueue<GCRequest>* tmp = current[i];
            current[i] = next[i];
            next[i] = tmp;
          }

          // 7. Reset the bit of global epoch.
          // NOTE(Alec): it's fine even if global epoch is the max.
          // Because the next queue is swapped to the current queue, when
          // the worker goes to the next next round, it will be handled by the
          // next queue.
          global_epoch->epoch_.store(global_epoch->epoch_.load() &
                                     0x7FFFFFFFFFFFFFFF);

          break;
        }

        usleep(GLOBAL_EPOCH_INCR_INTERVAL);
      }
    }
    usleep(GLOBAL_EPOCH_INCR_INTERVAL);
  }
  wg->done();
  // printf("done global_gc_thread\n");
}

void normal_worker_controller(
    InvertedIndex* db, AlignedBool* running, Epoch* global_epoch,
    Epoch** local_epoch, SpinLock** sl, AlignedBool* worker_running,
    moodycamel::BlockingConcurrentQueue<WorkerRequest*>* rq,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue,
    mem::ARTObjectPool* art_object_pool, mem::MemPostingsObjectPool* m_pool) {
  db->wg_.add(1);
  while (running->get()) {
    if (rq->size_approx() > WORKER_REQUEST_QUEUE_THRES) {
      // Try to add a new normal worker.
      for (int i = 1; i < MAX_WORKER_NUM; i++) {
        if (!worker_running[i].get()) {
          worker_running[i].set(true);
          std::thread worker(worker_thread, db, global_epoch, local_epoch[i],
                             sl[i], &worker_running[i], rq, gc_queue,
                             art_object_pool, m_pool, false);
          worker.detach();
          break;
        }
      }
    }

    usleep(GLOBAL_EPOCH_INCR_INTERVAL);
  }
  db->wg_.done();
  // printf("done normal_worker_controller\n");
}

void migration_worker(InvertedIndex* db, AlignedBool* running) {
  db->wg_.add(1);
  while (running->get()) {
    db->try_migrate();

    usleep(GLOBAL_EPOCH_INCR_INTERVAL);
  }
  db->wg_.done();
  // printf("done migration_worker\n");
}

}  // namespace mem
}  // namespace tsdb