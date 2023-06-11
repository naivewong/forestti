#pragma once
#include <stdint.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <memory>

namespace artoptlock {
#define NODE4 1
#define NODE16 2
#define NODE48 3
#define NODE256 4
#define NODELEAF 5

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
struct art_node {
  uint32_t partial_len;
  uint8_t type;
  uint8_t num_children;
  unsigned char partial[MAX_PREFIX_LEN];

  art_node() : partial_len(0), type(0), num_children(0) {
    memset(partial, 0, MAX_PREFIX_LEN);
  }
};

class tree_node {
 public:
  uint32_t version;  // For optimistic lock coupling.
  tree_node() : version(0) {}

  virtual uint8_t type() = 0;
  virtual uint32_t partial_len() { return 0; }
  virtual void set_partial_len(uint32_t l) {}
  virtual unsigned char* partial() { return nullptr; }
  virtual uint8_t num_children() { return 0; }
  virtual void set_num_children(uint8_t n) {}

  // Internal node.
  virtual unsigned char* keys() { return nullptr; }
  virtual std::shared_ptr<tree_node>* children() { return nullptr; }

  // Leaf.
  virtual const char* key() { return nullptr; }
  virtual size_t key_len() { return 0; }
  virtual void* value() { return nullptr; }
  virtual void set_value(void* v) {}
};

/**
 * Small node with only 4 children
 */
class art_node4 : public tree_node {
 public:
  art_node n;
  unsigned char keys_[4];
  std::shared_ptr<tree_node> children_[4];

  art_node4() {
    n.type = NODE4;
    memset(keys_, 0, 4);
    for (int i = 0; i < 4; i++) children_[i] = NULL;
  }

  virtual uint8_t type() override { return n.type; }
  virtual uint32_t partial_len() override { return n.partial_len; }
  virtual void set_partial_len(uint32_t l) override { n.partial_len = l; }
  virtual unsigned char* partial() override { return n.partial; }
  virtual uint8_t num_children() override { return n.num_children; }
  virtual void set_num_children(uint8_t num) override { n.num_children = num; }

  virtual unsigned char* keys() override { return keys_; }
  virtual std::shared_ptr<tree_node>* children() override { return children_; }
};

/**
 * Node with 16 children
 */
class art_node16 : public tree_node {
 public:
  art_node n;
  unsigned char keys_[16];
  std::shared_ptr<tree_node> children_[16];

  art_node16() {
    n.type = NODE16;
    memset(keys_, 0, 16);
    for (int i = 0; i < 16; i++) children_[i] = NULL;
  }

  virtual uint8_t type() override { return n.type; }
  virtual uint32_t partial_len() override { return n.partial_len; }
  virtual void set_partial_len(uint32_t l) override { n.partial_len = l; }
  virtual unsigned char* partial() override { return n.partial; }
  virtual uint8_t num_children() override { return n.num_children; }
  virtual void set_num_children(uint8_t num) override { n.num_children = num; }

  virtual unsigned char* keys() override { return keys_; }
  virtual std::shared_ptr<tree_node>* children() override { return children_; }
};

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
class art_node48 : public tree_node {
 public:
  art_node n;
  unsigned char keys_[256];
  std::shared_ptr<tree_node> children_[48];

  art_node48() {
    n.type = NODE48;
    memset(keys_, 0, 256);
    for (int i = 0; i < 48; i++) children_[i] = NULL;
  }

  virtual uint8_t type() override { return n.type; }
  virtual uint32_t partial_len() override { return n.partial_len; }
  virtual void set_partial_len(uint32_t l) override { n.partial_len = l; }
  virtual unsigned char* partial() override { return n.partial; }
  virtual uint8_t num_children() override { return n.num_children; }
  virtual void set_num_children(uint8_t num) override { n.num_children = num; }

  virtual unsigned char* keys() override { return keys_; }
  virtual std::shared_ptr<tree_node>* children() override { return children_; }
};

/**
 * Full node with 256 children
 */
class art_node256 : public tree_node {
 public:
  art_node n;
  std::shared_ptr<tree_node> children_[256];

  art_node256() {
    n.type = NODE256;
    for (int i = 0; i < 256; i++) children_[i] = NULL;
  }

  virtual uint8_t type() override { return n.type; }
  virtual uint32_t partial_len() override { return n.partial_len; }
  virtual void set_partial_len(uint32_t l) override { n.partial_len = l; }
  virtual unsigned char* partial() override { return n.partial; }
  virtual uint8_t num_children() override { return n.num_children; }
  virtual void set_num_children(uint8_t num) override { n.num_children = num; }

  virtual unsigned char* keys() override { return nullptr; }
  virtual std::shared_ptr<tree_node>* children() override { return children_; }
};

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
class art_leaf : public tree_node {
 public:
  void* value_;
  std::string key_;

  art_leaf(void* val, const unsigned char* k, int key_len)
      : value_(val), key_(reinterpret_cast<const char*>(k), key_len) {}

  virtual uint8_t type() override { return NODELEAF; }
  virtual const char* key() override { return key_.data(); }
  virtual size_t key_len() override { return key_.size(); }
  virtual void* value() override { return value_; }
  virtual void set_value(void* v) override { value_ = v; }
};

/**
 * Main struct, points to root.
 */
struct art_tree {
  std::shared_ptr<tree_node> root;
  uint64_t size;
  int64_t mem_size;
  std::atomic<bool> lock;
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
                          void* value, tree_node** new_leaf);

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

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree* t, const unsigned char* key, int key_len);

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
  // std::cout << *version << " " << is_obsolete(*version) << std::endl;
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
  return version == __sync_val_compare_and_swap(&node->version, 0, 0);
}

template <typename T, typename R>
bool read_unlock_or_restart(T* node, uint32_t version, R* locked_node) {
  if (!node) return true;
  if (version != __sync_val_compare_and_swap(&node->version, 0, 0)) {
    write_unlock(locked_node);
    return false;
  }
  return true;
}

template <typename T>
bool upgrade_to_write_lock_or_restart(T* node, uint32_t version) {
  if (!node) return true;
  return __sync_bool_compare_and_swap(&node->version, version,
                                      set_locked_bit(version));
}

template <typename T>
bool upgrade_to_write_lock_or_restart(T* node, uint32_t version,
                                      T* locked_node) {
  if (!node) return true;
  if (!__sync_bool_compare_and_swap(&node->version, version,
                                    set_locked_bit(version))) {
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
  __sync_fetch_and_add(&node->version, 2);
}

template <typename T>
void write_unlock_obsolete(T* node) {
  if (!node) return;
  __sync_fetch_and_add(&node->version, 3);
}

template <typename T>
uint32_t await_node_unlocked(T* node) {
  uint32_t version = __sync_val_compare_and_swap(&node->version, 0, 0);
  while ((version & 2) == 2) {
    __builtin_ia32_pause();
    version = __sync_val_compare_and_swap(&node->version, 0, 0);
  }
  return version;
}

}  // namespace artoptlock