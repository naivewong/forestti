#pragma once

#include <sched.h>
#include <stdint.h>

#include <atomic>

#include "third_party/moodycamel/concurrentqueue.h"

namespace leveldb {
class RandomAccessFile;
class RandomRWFile;
class WritableFile;
}  // namespace leveldb

namespace tsdb {
namespace mem {

class GCRequest;

#define NODE4 1
#define NODE16 2
#define NODE48 3
#define NODE256 4
#define NODELEAF 5

#define MAX_PREFIX_LEN 8

#define NODE48_EMPTY_SLOTS 0xffff000000000000
#define NODE48_GROW_SLOTS 0xffffffff00000000

#define NODE16_MIN_SIZE 4
#define NODE48_MIN_SIZE 13
#define NODE256_MIN_SIZE 38

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) ((bool)((uintptr_t)x & 0x8000000000000000))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 0x8000000000000000))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & 0x7fffffffffffffff)))

/**
 * This struct is included as part
 * of all the various node sizes
 */
struct art_node {
  std::atomic<uint32_t> version;
  uint32_t prefix_len;
  // prefixLeaf store value of key which is prefix of other keys.
  // eg. [1]'s value will store here when [1, 0] exist.
  std::atomic<void*> prefix_leaf;
  uint8_t type;
  uint8_t num_children;
  unsigned char prefix[MAX_PREFIX_LEN];
};

/**
 * Small node with only 4 children
 */
struct art_node4 {
  art_node n;
  unsigned char keys[4];
  art_node* children[4];
};

/**
 * Node with 16 children
 */
struct art_node16 {
  art_node n;
  unsigned char keys[16];
  art_node* children[16];
};

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
struct art_node48 {
  art_node n;
  unsigned char keys[256];
  uint64_t slots;
  art_node* children[48];

  int alloc_slot() {
    int idx = 48 - (64 - __builtin_clzll(~slots));
    slots |= (((uint64_t)(1)) << (48 - idx - 1));
    return idx;
  }

  void free_slot(int idx) { slots &= (~(((uint64_t)(1)) << (48 - idx - 1))); }
};

/**
 * Full node with 256 children
 */
struct art_node256 {
  art_node n;
  art_node* children[256];
};

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
struct art_leaf {
  void* value;
  uint32_t key_len;
  unsigned char key[];
};

/**
 * Main struct, points to root.
 */
struct art_tree {
  art_node* root;
  uint64_t size;
  std::atomic<uint64_t> mem_size;
};

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree* t);

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree* t);

uint64_t art_mem_size(art_tree* t);

bool art_empty(art_tree* t);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(art_tree* t, const unsigned char* key, int key_len);

/**
 * inserts a new value into the art tree
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 */
void art_insert(art_tree* t, const unsigned char* key, int key_len,
                void* value);
void art_insert_no_update(art_tree* t, const unsigned char* key, int key_len,
                          void* value, void** return_value);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree* t, const unsigned char* key, int key_len);

// If art_callback returns true the current query will terminate immediately.
typedef bool (*art_callback)(void* data, const unsigned char* key,
                             uint32_t key_len, void* value);
void art_range(art_tree* t, const unsigned char* begin, int begin_len,
               const unsigned char* end, int end_len, bool include_begin,
               bool include_end, art_callback cb, void* data);
void art_prefix(art_tree* t, const unsigned char* key, int key_len,
                art_callback cb, void* data);

/********************************* With GC ********************************/
class ARTObjectPool {
 public:
  ~ARTObjectPool();
  void put_node4(art_node4* n);
  art_node4* get_node4();
  art_node4* get_node4(std::atomic<uint64_t>* size);
  void put_node16(art_node16* n);
  art_node16* get_node16();
  art_node16* get_node16(std::atomic<uint64_t>* size);
  void put_node48(art_node48* n);
  art_node48* get_node48();
  art_node48* get_node48(std::atomic<uint64_t>* size);
  void put_node256(art_node256* n);
  art_node256* get_node256();
  art_node256* get_node256(std::atomic<uint64_t>* size);

  void put_node(art_node* n);
  art_node* get_node(uint8_t type);

 private:
  moodycamel::ConcurrentQueue<art_node4*> node4_queue_;
  moodycamel::ConcurrentQueue<art_node16*> node16_queue_;
  moodycamel::ConcurrentQueue<art_node48*> node48_queue_;
  moodycamel::ConcurrentQueue<art_node256*> node256_queue_;
};

void art_insert(art_tree* t, const unsigned char* key, int key_len, void* value,
                uint64_t epoch, ARTObjectPool* pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue);
void art_insert_no_update(art_tree* t, const unsigned char* key, int key_len,
                          void* value, void** return_value, uint64_t epoch,
                          ARTObjectPool* pool,
                          moodycamel::ConcurrentQueue<GCRequest>* gc_queue);

void* art_delete(art_tree* t, const unsigned char* key, int key_len,
                 uint64_t epoch, ARTObjectPool* pool,
                 moodycamel::ConcurrentQueue<GCRequest>* gc_queue);

int art_tree_destroy(art_tree* t, uint64_t epoch,
                     moodycamel::ConcurrentQueue<GCRequest>* gc_queue);

/************************ Serialization ****************************/
void art_serialization(art_tree* t, leveldb::WritableFile* file,
                       leveldb::RandomRWFile* rwfile);
void art_deserialization(art_tree* t, leveldb::RandomAccessFile* file);

/************************ Helper Functions *************************/
inline uint32_t set_locked_bit(uint32_t version) { return version + 2; }

inline bool is_obsolete(uint32_t version) { return (version & 1) == 1; }

template <typename T>
uint32_t await_node_unlocked(T* node) {
  uint32_t version = node->version.load();
  // uint32_t version = __sync_val_compare_and_swap(&node->version, 0, 0);
  while ((version & 2) == 2) {
    __builtin_ia32_pause();
    // sched_yield();
    version = node->version.load();
    // version = __sync_val_compare_and_swap(&node->version, 0, 0);
  }
  return version;
}

// false for restart.
// false only when obsolete.
template <typename T>
bool read_lock_or_restart(T* node, uint32_t* version) {
  *version = await_node_unlocked(node);
  // printf("%u is_obsolete:%d\n", *version, is_obsolete(*version));
  if (is_obsolete(*version)) {
    return false;
  }
  return true;
}

template <typename T>
bool check_or_restart(T* node, uint32_t version) {
  return read_unlock_or_restart(node, version);
}

template <typename T>
bool read_unlock_or_restart(T* node, uint32_t version) {
  if (!node) return true;
  // return version == __sync_val_compare_and_swap(&node->version, 0, 0);
  return version == node->version.load();
}

template <typename T, typename R>
bool read_unlock_or_restart(T* node, uint32_t version, R* locked_node) {
  if (!node) return true;
  if (version != node->version.load()) {
    // if (version != __sync_val_compare_and_swap(&node->version, 0, 0)) {
    write_unlock(locked_node);
    return false;
  }
  return true;
}

template <typename T>
bool upgrade_to_write_lock_or_restart(T* node, uint32_t version) {
  if (!node) return true;
  // return __sync_bool_compare_and_swap(&node->version, version,
  // set_locked_bit(version));
  return node->version.compare_exchange_weak(version, set_locked_bit(version));
}

template <typename T>
bool upgrade_to_write_lock_or_restart(T* node, uint32_t version,
                                      T* locked_node) {
  if (!node) return true;
  // if (!__sync_bool_compare_and_swap(&node->version, version,
  // set_locked_bit(version))) {
  if (!node->version.compare_exchange_weak(version, set_locked_bit(version))) {
    write_unlock(locked_node);
    return false;
  }
  return true;
}

template <typename T>
bool write_lock_or_restart(T* node) {
  uint32_t version;
  do {
    bool ok = read_lock_or_restart(node, &version);
    if (!ok) return false;
  } while (!upgrade_to_write_lock_or_restart(node, version));
  return true;
}

template <typename T>
void write_unlock(T* node) {
  if (!node) return;
  node->version.fetch_add(2);
  // __sync_fetch_and_add(&node->version, 2);
}

template <typename T>
void write_unlock_obsolete(T* node) {
  if (!node) return;
  node->version.fetch_add(3);
  // __sync_fetch_and_add(&node->version, 3);
}

}  // namespace mem
}  // namespace tsdb