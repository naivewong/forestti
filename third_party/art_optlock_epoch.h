#pragma once
#include <stdint.h>

#include <atomic>

#include "third_party/moodycamel/concurrentqueue.h"

namespace tsdb {
namespace mem {
class GCRequest;
}
}  // namespace tsdb

namespace artoptlockepoch {
#define NODE4 1
#define NODE16 2
#define NODE48 3
#define NODE256 4

#define MAX_PREFIX_LEN 10

#if defined(__GNUC__) && !defined(__clang__)
#if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#define BROKEN_GCC_C99_INLINE
#endif
#endif

typedef int (*art_callback)(void* data, const unsigned char* key,
                            uint32_t key_len, void* value);
typedef int (*art_clean_callback)(void* data);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
  std::atomic<uint32_t> version;
  uint32_t partial_len;
  uint8_t type;
  uint8_t num_children;
  unsigned char partial[MAX_PREFIX_LEN];
} art_node;

/**
 * Small node with only 4 children
 */
typedef struct {
  art_node n;
  unsigned char keys[4];
  art_node* children[4];
} art_node4;

/**
 * Node with 16 children
 */
typedef struct {
  art_node n;
  unsigned char keys[16];
  art_node* children[16];
} art_node16;

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct {
  art_node n;
  unsigned char keys[256];
  art_node* children[48];
} art_node48;

/**
 * Full node with 256 children
 */
typedef struct {
  art_node n;
  art_node* children[256];
} art_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
  void* value;
  uint32_t version;
  uint32_t key_len;
  unsigned char key[];
} art_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
  art_node* root;
  uint64_t size;
  uint64_t mem_size;
} art_tree;

class ARTObjectPool {
 public:
  ~ARTObjectPool();
  void put_node4(art_node4* n);
  art_node4* get_node4();
  art_node4* get_node4(uint64_t* size);
  void put_node16(art_node16* n);
  art_node16* get_node16();
  art_node16* get_node16(uint64_t* size);
  void put_node48(art_node48* n);
  art_node48* get_node48();
  art_node48* get_node48(uint64_t* size);
  void put_node256(art_node256* n);
  art_node256* get_node256();
  art_node256* get_node256(uint64_t* size);

  void put_node(art_node* n);
  art_node* get_node(uint8_t type);

 private:
  moodycamel::ConcurrentQueue<art_node4*> node4_queue_;
  moodycamel::ConcurrentQueue<art_node16*> node16_queue_;
  moodycamel::ConcurrentQueue<art_node48*> node48_queue_;
  moodycamel::ConcurrentQueue<art_node256*> node256_queue_;
};

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree* t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_art_tree(...) art_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree* t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_art_tree(...) art_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.
 */
#ifdef BROKEN_GCC_C99_INLINE
#define art_size(t) ((t)->size)
#else
inline uint64_t art_size(art_tree* t) { return t->size; }
#endif

inline uint64_t art_mem_size(art_tree* t) {
  return __sync_val_compare_and_swap(&(t->mem_size), 0, 0);
}

/**
 * inserts a new value into the art tree
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree* t, const unsigned char* key, int key_len,
                 void* value);
void* art_insert_get_leaf(art_tree* t, const unsigned char* key, int key_len,
                          void* value, art_leaf** new_leaf);

/************************* GC Version *************************/
void* art_insert(art_tree* t, const unsigned char* key, int key_len,
                 void* value, uint64_t epoch, ARTObjectPool* pool,
                 moodycamel::ConcurrentQueue<tsdb::mem::GCRequest>* gc_queue);
void* art_insert_get_leaf(
    art_tree* t, const unsigned char* key, int key_len, void* value,
    art_leaf** new_leaf, uint64_t epoch, ARTObjectPool* pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest>* gc_queue);

/**
 * inserts a new value into the art tree (not replacing)
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert_no_replace(art_tree* t, const unsigned char* key, int key_len,
                            void* value);
void* art_insert_no_replace_get_leaf(art_tree* t, const unsigned char* key,
                                     int key_len, void* value,
                                     art_leaf** new_leaf);

/************************* GC Version *************************/
void* art_insert_no_replace(
    art_tree* t, const unsigned char* key, int key_len, void* value,
    uint64_t epoch, ARTObjectPool* pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest>* gc_queue);
void* art_insert_no_replace_get_leaf(
    art_tree* t, const unsigned char* key, int key_len, void* value,
    art_leaf** new_leaf, uint64_t epoch, ARTObjectPool* pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest>* gc_queue);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree* t, const unsigned char* key, int key_len);

/************************* GC Version *************************/
void* art_delete(art_tree* t, const unsigned char* key, int key_len,
                 uint64_t epoch, ARTObjectPool* pool,
                 moodycamel::ConcurrentQueue<tsdb::mem::GCRequest>* gc_queue);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree* t, const unsigned char* key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
art_leaf* art_minimum(art_tree* t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
art_leaf* art_maximum(art_tree* t);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art_tree* t, art_callback cb, art_clean_callback clean_cb,
             void* data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
// int art_iter_prefix(art_tree *t, const unsigned char *prefix, int prefix_len,
// art_callback cb, void *data);

/********************************* Helper Functions
 * ********************************/
inline uint32_t set_locked_bit(uint32_t version) { return version + 2; }

inline bool is_obsolete(uint32_t version) { return (version & 1) == 1; }

// false for restart.
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

}  // namespace artoptlockepoch