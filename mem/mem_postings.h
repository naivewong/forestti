#pragma once

#include <atomic>
#include <vector>

#include "base/Mutex.hpp"
#include "disk/log_manager.h"
#include "index/prefix_postings.h"
#include "label/Label.hpp"
#include "label/MatcherInterface.hpp"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "mem/go_art.h"
#include "mem/mergeset_manager.h"
#include "third_party/art.h"
#include "third_party/art_optlock.h"
#include "third_party/art_optlock_epoch.h"
#include "third_party/moodycamel/concurrentqueue.h"

#define ART_OPT_EPOCH_NS

#ifdef ART_ORIGINAL_NS
#define ART_NS art
#define ART_LEAF_TYPE ART_NS::art_leaf
#define ART_LEAF_KEY(leaf) reinterpret_cast<const unsigned char*>(leaf->key)
#define ART_LEAF_KEY_LEN(leaf) (leaf->key_len)
#endif

#ifdef ART_OPT_NS
#define ART_NS artoptlock
#define ART_LEAF_TYPE ART_NS::tree_node
#define ART_LEAF_KEY(leaf) reinterpret_cast<const unsigned char*>(leaf->key())
#define ART_LEAF_KEY_LEN(leaf) (leaf->key_len())
#endif

#ifdef ART_OPT_EPOCH_NS
#define ART_NS ::tsdb::mem
#define ART_LEAF_TYPE ART_NS::art_leaf
#define ART_LEAF_KEY(leaf) reinterpret_cast<const unsigned char*>(leaf->key)
#define ART_LEAF_KEY_LEN(leaf) (leaf->key_len)
#endif

#define TAG_VALUES_NODE_FLAT 1
#define TAG_VALUES_NODE_ART 2
#define TAG_VALUES_NODE_MERGESET 3

#define MAX_FLAT_CAPACITY 10

namespace tsdb {
namespace mem {

class GCRequest;
class MergeSet;

/*********************** tag_values_node *********************/
struct tag_values_node {
  std::atomic<uint64_t> access_epoch;
  std::atomic<uint32_t> version;  // For optimistic locking (add).
  uint8_t type;

  tag_values_node() : access_epoch(0), version(0) {}
};

/*********************** tag_values_node_flat *********************/
struct tag_values_node_flat {
  tag_values_node node_;
  std::vector<int> pos_;
  std::vector<int> size_;
  base::RWMutexLock mutex_;
  std::string data_;

  tag_values_node_flat() {
    node_.type = TAG_VALUES_NODE_FLAT;
    node_.access_epoch = 0;
    node_.version = 0;
  }

  void clear() {
    node_.version = 0;
    pos_.clear();
    size_.clear();
    data_.clear();
  }

  size_t size() {
    return 8 * pos_.size() + 8 * size_.size() + data_.size();
  }
};

int _flat_compare(tag_values_node_flat* node, int idx, const std::string& s);
int _flat_upper_bound(tag_values_node_flat* node, const std::string& s);
void flat_add(tag_values_node_flat* node, uint64_t addr, const std::string& s);
bool flat_add_update(tag_values_node_flat* node, uint64_t addr,
                     const std::string& s);
bool flat_add_no_update(tag_values_node_flat* node, uint64_t addr,
                        const std::string& s);
uint64_t flat_get(tag_values_node_flat* node, const std::string& s);
void flat_del(tag_values_node_flat* node, const std::string& s);
void flat_remove_with_addr(tag_values_node_flat* node, uint64_t addr);
void flat_get_key_with_addr(tag_values_node_flat* node, uint64_t addr,
                            std::string* s);

/*********************** tag_values_node_art *********************/
struct tag_values_node_art {
  tag_values_node node_;
  ART_NS::art_tree tree_;
  std::atomic<uint32_t> version;

  tag_values_node_art() {
    node_.type = TAG_VALUES_NODE_ART;
    node_.access_epoch = 0;
    node_.version = 0;
    tree_.size = 0;
    tree_.mem_size = 0;
  }

  size_t size() {
    return tree_.mem_size.load();
  }
};

// Add/get the address of postings list.
uint64_t art_get(tag_values_node_art* node, const std::string& s);

/*********************** tag_values_node_msergeset *********************/
struct tag_values_node_mergeset {
  tag_values_node node_;
  MergeSet* ms_;

  tag_values_node_mergeset() {
    node_.type = TAG_VALUES_NODE_MERGESET;
    node_.access_epoch = 0;
    node_.version = 0;
    ms_ = nullptr;
  }
  uint64_t get(const std::string& s);
  void add(const std::string& s, uint64_t addr);
  void del(const std::string& s);
};

struct postings_list {
  std::atomic<uint64_t> read_epoch_;
  index::PrefixPostingsV2 list_;

  std::atomic<uint32_t> version;

  // NOTE(Alec): May be version is enough.
  // There may be a read thread during the expansion (another write thread).
  // Not sure if this causes SEG fault. Maybe we need a special vector.
  std::atomic<bool> lock_;

  postings_list() : read_epoch_(0), version(0), lock_(false) {}

  void clear() {
    read_epoch_ = 0;
    list_.reset();
    lock_ = false;
    version = 0;
  }

  void lock() {
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

  void unlock() { lock_.store(false, std::memory_order_release); }

  void insert(uint64_t id) { list_.insert(id); }

  // Use with lock.
  bool remove(uint64_t id) { return list_.remove(id); }

  size_t size() { return list_.size(); }
  bool empty() { return list_.size() == 0; }
};

void intersect_postings_lists(std::vector<postings_list*>&& pls,
                              std::vector<uint64_t>* pids);

class MemPostingsObjectPool {
 private:
  moodycamel::ConcurrentQueue<tag_values_node_flat*> flat_objs_;
  moodycamel::ConcurrentQueue<tag_values_node_art*> art_objs_;
  moodycamel::ConcurrentQueue<postings_list*> list_objs_;

 public:
  ~MemPostingsObjectPool();
  void put_flat_node(tag_values_node_flat* n);
  tag_values_node_flat* get_flat_node();
  void put_art_node(tag_values_node_art* n);
  tag_values_node_art* get_art_node();
  void put_postings(postings_list* l);
  postings_list* get_postings();
};

struct LastAccessTimePair {
  std::string value;
  uint64_t epoch;

  LastAccessTimePair(const std::string& v, uint64_t e) : value(v), epoch(e) {}
};
bool gc_epoch_cmp(const LastAccessTimePair& l, const LastAccessTimePair& r);

void encode_postings(std::string* dst, const std::string& tn,
                     const std::string& tv, const std::vector<uint64_t>& list);
void encode_postings(std::string* dst, const std::string& tn,
                     const std::string& tv, const index::PrefixPostingsV2& p);
void decode_postings(leveldb::Slice src, std::string* tn, std::string* tv,
                     std::vector<uint64_t>* list);
void decode_postings(leveldb::Slice src, std::string* tn, std::string* tv,
                     index::PrefixPostingsV2* list);

class MemPostings {
 private:
  ART_NS::art_tree tag_keys_;
  std::atomic<int> postings_list_size_;
  base::RWMutexLock mutex_;

  std::unique_ptr<disk::LogManager> postings_log_;
  MergeSetManager* mergeset_manager_;

  /***************** add *****************/
  bool _flat_add(tag_values_node_flat* node, uint64_t id,
                 const label::Label& l);
  bool _art_add(tag_values_node_art* node, uint64_t id, const label::Label& l);
  void _mergeset_add(tag_values_node_mergeset* node, uint64_t id, const label::Label& l);

  /***************** get *****************/
  bool _flat_get(tag_values_node_flat* node, const std::string& l,
                 std::vector<uint64_t>* pids);
  bool _flat_get(tag_values_node_flat* node, const std::string& l,
                 postings_list** pl);
  bool _art_get(tag_values_node_art* node, const std::string& l,
                std::vector<uint64_t>* pids);
  bool _art_get(tag_values_node_art* node, const std::string& l,
                postings_list** pl);
  bool _mergeset_get(tag_values_node_mergeset* node, const std::string& l,
                     std::vector<uint64_t>* pids);
  bool _mergeset_get(tag_values_node_mergeset* node, const std::string& l,
                     postings_list** pl);

  /***************** del *****************/
  bool _flat_del(tag_values_node_flat* node, uint64_t id, const label::Label& l,
                 bool* deleted);
  bool _art_del(tag_values_node_art* node, uint64_t id, const label::Label& l);
  bool _mergeset_del(tag_values_node_mergeset* node, uint64_t id, const label::Label& l);

  void gc_get_tag_values(
      const std::string& key,
      std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)>* values);
  postings_list* read_postings_from_log(uint64_t pos);
  void read_postings_from_log(uint64_t pos, std::vector<uint64_t>* list);

 public:
  MemPostings();
  ~MemPostings();

  ART_NS::art_tree* get_tag_keys_trie() { return &tag_keys_; }
  void set_log(const std::string& dir);
  void set_mergeset_manager(const std::string& dir, leveldb::Options opts);

  void add(uint64_t id, const label::Label& l);
  void add(uint64_t id, const label::Labels& ls);

  // NOTE(Alec): current get mechanism loops on a single type of node.
  void get(const label::Label& l, std::vector<uint64_t>* pids);
  void get(const std::string& l, const std::string& v,
           std::vector<uint64_t>* pids);
  void get(const std::string& l, const std::string& v, postings_list** pl);
  bool del(uint64_t id, const label::Label& l);  // return true if deleted.

  void select_postings_for_gc(std::vector<std::string>* tag_names,
                              std::vector<std::string>* tag_values, int num);
  void postings_gc(std::vector<std::string>* tag_names,
                   std::vector<std::string>* tag_values, uint64_t epoch = 0,
                   moodycamel::ConcurrentQueue<GCRequest>* gc_queue = nullptr);

  leveldb::Status art_convert(tag_values_node_art* node, const std::string& name, uint64_t epoch, ARTObjectPool* art_object_pool, moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
  int art_gc(double percentage, uint64_t epoch = 0, ARTObjectPool* art_object_pool = nullptr, moodycamel::ConcurrentQueue<GCRequest>* gc_queue = nullptr);

  uint64_t mem_size();
  uint64_t tag_key_size() { return art_mem_size(&tag_keys_); }
  uint64_t postings_size();
  int postings_num();
  int num_tag_values_nodes();

  void get_values(const std::string& key, std::vector<std::string>* values);

  // Fast path.
  int postings_list_num() { return postings_list_size_.load(); }

  // void ensure_order();

  // TODO(Alec): add mergeset in snapshot.
  leveldb::Status snapshot_index(const std::string& dir);
  leveldb::Status recover_from_snapshot(const std::string& dir,
                                        const std::string& main_dir);


  /************************* For SEDA and GC *************************/
  postings_list* _new_postings_list(MemPostingsObjectPool* pool);
  void release_postings_list(postings_list* list, uint64_t epoch,
                             ARTObjectPool* art_object_pool,
                             moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
    /***************** add *****************/
  bool _flat_add(tag_values_node_flat* node, uint64_t id, const label::Label& l,
                 uint64_t epoch, ARTObjectPool* art_object_pool,
                 MemPostingsObjectPool* m_pool,
                 moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
  bool _art_add(tag_values_node_art* node, uint64_t id, const label::Label& l,
                uint64_t epoch, ARTObjectPool* art_object_pool,
                MemPostingsObjectPool* m_pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue);

    /***************** del *****************/
  bool _flat_del(tag_values_node_flat* node, uint64_t id, const label::Label& l,
                 bool* deleted, uint64_t epoch,
                 moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
  bool _art_del(tag_values_node_art* node, uint64_t id, const label::Label& l,
                uint64_t epoch, ARTObjectPool* art_object_pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
  bool _mergeset_del(tag_values_node_mergeset* node, uint64_t id, const label::Label& l,
                     uint64_t epoch, moodycamel::ConcurrentQueue<GCRequest>* gc_queue);

  void add(uint64_t id, const label::Label& l, uint64_t epoch,
           ARTObjectPool* art_object_pool, MemPostingsObjectPool* m_pool,
           moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
  void add(uint64_t id, const label::Labels& ls, uint64_t epoch,
           ARTObjectPool* art_object_pool, MemPostingsObjectPool* m_pool,
           moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
  bool del(uint64_t id, const label::Label& l, uint64_t epoch,
           ARTObjectPool* art_object_pool, MemPostingsObjectPool* m_pool,
           moodycamel::ConcurrentQueue<GCRequest>*
               gc_queue);  // return true if deleted.
  /************************* For SEDA and GC *************************/
};

}  // namespace mem
}  // namespace tsdb