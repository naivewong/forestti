#include "art_optlock.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <atomic>
#include <iostream>

namespace artoptlock {
#ifdef __i386__
#include <emmintrin.h>
#else
#ifdef __amd64__
#include <emmintrin.h>
#endif
#endif

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (x->type() == NODELEAF)
#define SET_LEAF(x) x
#define LEAF_RAW(x) x

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static std::shared_ptr<tree_node> alloc_node(art_tree *tree, uint8_t type) {
  switch (type) {
    case NODE4:
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node4));
      return std::make_shared<art_node4>();
    case NODE16:
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node16));
      return std::make_shared<art_node16>();
    case NODE48:
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node48));
      return std::make_shared<art_node48>();
    case NODE256:
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node256));
      return std::make_shared<art_node256>();
    default:
      abort();
  }
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
  t->size = 0;
  t->mem_size = 0;
  t->lock = 0;
  t->root = alloc_node(t, NODE4);
  __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node4));
  return 0;
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) { return 0; }

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);
#endif

static std::shared_ptr<tree_node> *find_child(tree_node *n, unsigned char c) {
  int i, mask, bitfield;
  switch (n->type()) {
    case NODE4:
      for (i = 0; i < n->num_children(); i++) {
        /* this cast works around a bug in gcc 5.1 when unrolling loops
         * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
         */
        if ((n->keys())[i] == c) return &(n->children()[i]);
      }
      break;

      {
        case NODE16:

// support non-86 architectures
#ifdef __i386__
          // Compare the key to all 16 stored keys
          __m128i cmp;
          cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                               _mm_loadu_si128((__m128i *)n->keys()));

          // Use a mask to ignore children that don't exist
          mask = (1 << n->num_children()) - 1;
          bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
          // Compare the key to all 16 stored keys
          __m128i cmp;
          cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                               _mm_loadu_si128((__m128i *)n->keys()));

          // Use a mask to ignore children that don't exist
          mask = (1 << n->num_children()) - 1;
          bitfield = _mm_movemask_epi8(cmp) & mask;
#else
          // Compare the key to all 16 stored keys
          bitfield = 0;
          for (i = 0; i < 16; ++i) {
            if (n->keys()[i] == c) bitfield |= (1 << i);
          }

          // Use a mask to ignore children that don't exist
          mask = (1 << n->num_children()) - 1;
          bitfield &= mask;
#endif
#endif

          /*
           * If we have a match (any bit set) then we can
           * return the pointer match using ctz to get
           * the index.
           */
          if (bitfield) return &(n->children()[__builtin_ctz(bitfield)]);
          break;
      }

    case NODE48:
      i = n->keys()[c];
      if (i) return &(n->children()[i - 1]);
      break;

    case NODE256:
      if (n->children()[c]) return &(n->children()[c]);
      break;

    default:
      abort();
  }
  return nullptr;
}

// Simple inlined if
static inline int min(int a, int b) { return (a < b) ? a : b; }

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(tree_node *n, const unsigned char *key, int key_len,
                        int depth) {
  int max_cmp = min(min(n->partial_len(), MAX_PREFIX_LEN), key_len - depth);
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (n->partial()[idx] != key[depth + idx]) return idx;
  }
  return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(tree_node *n, const unsigned char *key, int key_len,
                        int depth) {
  (void)depth;
  // Fail if the key lengths are different
  if (n->key_len() != (uint32_t)key_len) return 1;

  // Compare the keys starting at the depth
  return memcmp(n->key(), key, key_len);
}

bool art_search_helper(const unsigned char *key, int key_len,
                       std::shared_ptr<tree_node> node, int depth,
                       std::shared_ptr<tree_node> parent,
                       uint32_t parent_version, void **value_addr) {
  std::shared_ptr<tree_node> *child;
  std::shared_ptr<tree_node> nextnode;
  uint32_t version;

RECUR:

  if (!read_lock_or_restart(node.get(), &version)) {
    *value_addr = nullptr;
    return false;
  }

  if (parent) {
    if (!read_unlock_or_restart(parent.get(), parent_version)) {
      *value_addr = nullptr;
      return false;
    }
  }

  // Check if prefix matches, may increment level.
  if (node->partial_len()) {
    int prefix_len = check_prefix(node.get(), key, key_len, depth);
    if (prefix_len != min(MAX_PREFIX_LEN, node->partial_len())) {
      *value_addr = nullptr;
      if (!read_unlock_or_restart(node.get(), version)) return false;
      return true;  // Key not found.
    }
    depth = depth + node->partial_len();
  }

  // Recursively search
  child = find_child(node.get(), key[depth]);
  if (!check_or_restart(node.get(), version)) {
    *value_addr = nullptr;
    return false;
  }

  nextnode = (child ? std::atomic_load(child) : nullptr);

  if (!nextnode) {
    *value_addr = nullptr;
    if (!read_unlock_or_restart(node.get(), version)) return false;
    return true;
  }

  // Might be a leaf
  if (IS_LEAF(nextnode)) {
    // Check if the expanded path matches
    if (!leaf_matches(nextnode.get(), key, key_len, depth))
      *value_addr = nextnode->value();
    else
      *value_addr = nullptr;
    if (!read_unlock_or_restart(node.get(), version)) return false;
    return true;
  }

  depth++;
  parent = node;
  parent_version = version;
  node = nextnode;
  goto RECUR;
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return nullptr if the item was not found, otherwise
 * the value pointer is returned.
 */
void *art_search(const art_tree *t, const unsigned char *key, int key_len) {
  void *value = nullptr;
  while (true) {
    if (art_search_helper(key, key_len, std::atomic_load(&t->root), 0, nullptr,
                          0, &value))
      return value;
  }
}

// Find the minimum leaf under a node
static bool minimum(std::shared_ptr<tree_node> n, uint32_t version,
                    std::shared_ptr<tree_node> *leaf_addr) {
  std::shared_ptr<tree_node> next;
  int idx;
  switch (n->type()) {
    case NODE4:
      next = std::atomic_load(&(n->children()[0]));
      break;
    case NODE16:
      next = std::atomic_load(&(n->children()[0]));
      break;
    case NODE48:
      idx = 0;
      while (!n->keys()[idx]) idx++;
      idx = n->keys()[idx] - 1;
      next = std::atomic_load(&(n->children()[idx]));
      break;
    case NODE256:
      idx = 0;
      while (!n->children()[idx]) idx++;
      next = std::atomic_load(&(n->children()[idx]));
      break;
    default:
      abort();
  }

  if (!check_or_restart(n.get(), version)) return false;

  if (IS_LEAF(next)) {
    std::atomic_store(leaf_addr, next);
    if (!read_unlock_or_restart(n.get(), version)) return false;
    return true;
  }

  if (!read_lock_or_restart(next.get(), &version)) return false;
  return minimum(next, version, leaf_addr);
}

static std::shared_ptr<tree_node> minimum_unsafe(std::shared_ptr<tree_node> n) {
  std::shared_ptr<tree_node> next;
  int idx;
  switch (n->type()) {
    case NODE4:
      next = std::atomic_load(&(n->children()[0]));
      break;
    case NODE16:
      next = std::atomic_load(&(n->children()[0]));
      break;
    case NODE48:
      idx = 0;
      while (!n->keys()[idx]) idx++;
      idx = n->keys()[idx] - 1;
      next = std::atomic_load(&(n->children()[idx]));
      break;
    case NODE256:
      idx = 0;
      while (!n->children()[idx]) idx++;
      next = std::atomic_load(&(n->children()[idx]));
      break;
    default:
      abort();
  }
  if (IS_LEAF(next)) return next;
  return minimum_unsafe(next);
}

// Find the maximum leaf under a node
static std::shared_ptr<tree_node> maximum(std::shared_ptr<tree_node> n) {
  // Handle base cases
  if (!n) return nullptr;
  if (IS_LEAF(n)) return LEAF_RAW(n);

  int idx;
  switch (n->type()) {
    case NODE4:
      return maximum(n->children()[n->num_children() - 1]);
    case NODE16:
      return maximum(n->children()[n->num_children() - 1]);
    case NODE48:
      idx = 255;
      while (!n->keys()[idx]) idx--;
      idx = n->keys()[idx] - 1;
      return maximum(n->children()[idx]);
    case NODE256:
      idx = 255;
      while (!n->children()[idx]) idx--;
      return maximum(n->children()[idx]);
    default:
      abort();
  }
}

static art_leaf *make_leaf(art_tree *t, const unsigned char *key, int key_len,
                           void *value) {
  // art_leaf *l = (art_leaf*)calloc(1, sizeof(art_leaf)+key_len);
  art_leaf *l = new art_leaf(value, key, key_len);
  __sync_fetch_and_add(&(t->mem_size), sizeof(art_leaf) + key_len);
  return l;
}

static std::shared_ptr<art_leaf> make_leaf_shared_ptr(art_tree *t,
                                                      const unsigned char *key,
                                                      int key_len,
                                                      void *value) {
  __sync_fetch_and_add(&(t->mem_size), sizeof(art_leaf) + key_len);
  return std::make_shared<art_leaf>(value, key, key_len);
}

static int longest_common_prefix(tree_node *l1, tree_node *l2, int depth) {
  int max_cmp = min(l1->key_len(), l2->key_len()) - depth;
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (l1->key()[depth + idx] != l2->key()[depth + idx]) return idx;
  }
  return idx;
}

static void copy_header(tree_node *dest, tree_node *src) {
  dest->set_num_children(src->num_children());
  dest->set_partial_len(src->partial_len());
  memcpy(dest->partial(), src->partial(),
         min(MAX_PREFIX_LEN, src->partial_len()));
}

static void add_child256(art_tree *tree, tree_node *n,
                         std::shared_ptr<tree_node> *ref, unsigned char c,
                         std::shared_ptr<tree_node> child) {
  (void)ref;
  n->set_num_children(n->num_children() + 1);
  n->children()[c] = child;
}

static void add_child48(art_tree *tree, tree_node *n,
                        std::shared_ptr<tree_node> *ref, unsigned char c,
                        std::shared_ptr<tree_node> child) {
  if (n->num_children() < 48) {
    int pos = 0;
    while (n->children()[pos]) pos++;
    n->children()[pos] = child;
    n->keys()[c] = pos + 1;
    n->set_num_children(n->num_children() + 1);
  } else {
    std::shared_ptr<tree_node> new_node = alloc_node(tree, NODE256);
    for (int i = 0; i < 256; i++) {
      if (n->keys()[i]) {
        new_node->children()[i] = n->children()[n->keys()[i] - 1];
      }
    }
    copy_header(new_node.get(), n);
    *ref = new_node;
    // free(n);
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node48));
    add_child256(tree, new_node.get(), ref, c, child);
  }
}

static void add_child16(art_tree *tree, tree_node *n,
                        std::shared_ptr<tree_node> *ref, unsigned char c,
                        std::shared_ptr<tree_node> child) {
  if (n->num_children() < 16) {
    unsigned mask = (1 << n->num_children()) - 1;

// support non-x86 architectures
#ifdef __i386__
    __m128i cmp;

    // Compare the key to all 16 stored keys
    cmp =
        _mm_cmplt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *)n->keys()));

    // Use a mask to ignore children that don't exist
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
    __m128i cmp;

    // Compare the key to all 16 stored keys
    cmp =
        _mm_cmplt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *)n->keys()));

    // Use a mask to ignore children that don't exist
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
    // Compare the key to all 16 stored keys
    unsigned bitfield = 0;
    for (short i = 0; i < 16; ++i) {
      if (c < n->keys()[i]) bitfield |= (1 << i);
    }

    // Use a mask to ignore children that don't exist
    bitfield &= mask;
#endif
#endif

    // Check if less than any
    unsigned idx;
    if (bitfield) {
      idx = __builtin_ctz(bitfield);
      memmove(n->keys() + idx + 1, n->keys() + idx, n->num_children() - idx);
      for (int i = n->num_children() - idx - 1; i >= 0; i--)
        n->children()[idx + 1 + i] = n->children()[idx + i];
    } else
      idx = n->num_children();

    // Set the child
    n->keys()[idx] = c;
    n->children()[idx] = child;
    n->set_num_children(n->num_children() + 1);

  } else {
    std::shared_ptr<tree_node> new_node = alloc_node(tree, NODE48);

    // Copy the child pointers and populate the key map
    for (int i = 0; i < n->num_children(); i++)
      new_node->children()[i] = n->children()[i];
    for (int i = 0; i < n->num_children(); i++) {
      new_node->keys()[n->keys()[i]] = i + 1;
    }
    copy_header(new_node.get(), n);
    *ref = new_node;
    // free(n);
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node16));
    add_child48(tree, new_node.get(), ref, c, child);
  }
}

static void add_child4(art_tree *tree, tree_node *n,
                       std::shared_ptr<tree_node> *ref, unsigned char c,
                       std::shared_ptr<tree_node> child) {
  if (n->num_children() < 4) {
    int idx;
    for (idx = 0; idx < n->num_children(); idx++) {
      if (c < n->keys()[idx]) break;
    }

    // Shift to make room
    memmove(n->keys() + idx + 1, n->keys() + idx, n->num_children() - idx);
    for (int i = n->num_children() - idx - 1; i >= 0; i--)
      n->children()[idx + 1 + i] = n->children()[idx + i];

    // Insert element
    n->keys()[idx] = c;
    n->children()[idx] = child;
    n->set_num_children(n->num_children() + 1);

  } else {
    std::shared_ptr<tree_node> new_node = alloc_node(tree, NODE16);

    // Copy the child pointers and the key map
    for (int i = 0; i < n->num_children(); i++)
      new_node->children()[i] = n->children()[i];
    memcpy(new_node->keys(), n->keys(),
           sizeof(unsigned char) * n->num_children());
    copy_header(new_node.get(), n);
    *ref = new_node;
    // free(n);
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node4));
    add_child16(tree, new_node.get(), ref, c, child);
  }
}

static void add_child(art_tree *tree, tree_node *n,
                      std::shared_ptr<tree_node> *ref, unsigned char c,
                      std::shared_ptr<tree_node> child) {
  switch (n->type()) {
    case NODE4:
      return add_child4(tree, n, ref, c, child);
    case NODE16:
      return add_child16(tree, n, ref, c, child);
    case NODE48:
      return add_child48(tree, n, ref, c, child);
    case NODE256:
      return add_child256(tree, n, ref, c, child);
    default:
      abort();
  }
}

bool node_full(tree_node *n) {
  switch (n->type()) {
    case NODE4:
      return n->num_children() >= 4;
    case NODE16:
      return n->num_children() >= 16;
    case NODE48:
      return n->num_children() >= 48;
    default:
      return false;
  }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static bool prefix_mismatch(std::shared_ptr<tree_node> n, uint32_t version,
                            std::shared_ptr<tree_node> parent,
                            uint64_t parent_version, const unsigned char *key,
                            int key_len, int depth, int *prefix_diff) {
  int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len()), key_len - depth);
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (n->partial()[idx] != key[depth + idx]) {
      *prefix_diff = idx;
      return true;
    }
  }

  // If the prefix is short we can avoid finding a leaf
  if (n->partial_len() > MAX_PREFIX_LEN) {
    // Prefix is longer than what we've checked, find a leaf
    std::shared_ptr<tree_node> l;
    while (true) {
      if (!check_or_restart(n.get(), version) ||
          !check_or_restart(parent ? parent.get() : nullptr, parent_version))
        return false;
      if (minimum(n, version, &l)) {
        if (!check_or_restart(n.get(), version) ||
            !check_or_restart(parent ? parent.get() : nullptr, parent_version))
          return false;
        break;
      }
    }
    max_cmp = min(l->key_len(), key_len) - depth;
    for (; idx < max_cmp; idx++) {
      if (l->key()[idx + depth] != key[depth + idx]) {
        *prefix_diff = idx;
        return true;
      }
    }
  }
  *prefix_diff = idx;
  return true;
}

static bool recursive_insert(art_tree *tree, std::shared_ptr<tree_node> n,
                             std::shared_ptr<tree_node> *ref,
                             std::shared_ptr<tree_node> parent,
                             uint64_t parent_version, const unsigned char *key,
                             int key_len, void *value, int depth, int *old,
                             int replace, tree_node **new_leaf,
                             void **old_val_addr) {
  uint32_t version;
  if (!read_lock_or_restart(n.get(), &version)) {
    return false;
  }

  // Check if given node has a prefix
  if (n->partial_len()) {
    // Determine if the prefixes differ, since we need to split
    int prefix_diff;
    if (!prefix_mismatch(n, version, parent, parent_version, key, key_len,
                         depth, &prefix_diff))
      return false;
    // printf("depth:%d key:%s prefix_diff:%d partial_len:%d partial:%s
    // null:%d\n", depth, key, prefix_diff, n->partial_len(), n->partial(), leaf
    // == nullptr);
    if ((uint32_t)prefix_diff >= n->partial_len()) {
      depth += n->partial_len();
      goto RECURSE_SEARCH;
    }

    if (!upgrade_to_write_lock_or_restart(parent ? parent.get() : nullptr,
                                          parent_version))
      return false;
    if (!upgrade_to_write_lock_or_restart(n.get(), version,
                                          parent ? parent.get() : nullptr))
      return false;

    // Create a new node
    std::shared_ptr<tree_node> new_node = alloc_node(tree, NODE4);
    std::atomic_store(ref, new_node);
    new_node->set_partial_len(prefix_diff);
    memcpy(new_node->partial(), n->partial(), min(MAX_PREFIX_LEN, prefix_diff));

    // Adjust the prefix of the old node
    if (n->partial_len() <= MAX_PREFIX_LEN) {
      add_child4(tree, new_node.get(), ref, n->partial()[prefix_diff], n);
      n->set_partial_len(n->partial_len() - prefix_diff - 1);
      memmove(n->partial(), n->partial() + prefix_diff + 1,
              min(MAX_PREFIX_LEN, n->partial_len()));
    } else {
      n->set_partial_len(n->partial_len() - prefix_diff - 1);
      std::shared_ptr<tree_node> leaf = minimum_unsafe(n);
      add_child4(tree, new_node.get(), ref, leaf->key()[depth + prefix_diff],
                 n);
      memcpy(n->partial(), leaf->key() + depth + prefix_diff + 1,
             min(MAX_PREFIX_LEN, n->partial_len()));
    }

    // Insert the new leaf
    std::shared_ptr<tree_node> l =
        make_leaf_shared_ptr(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l.get();
    add_child4(tree, new_node.get(), ref, key[depth + prefix_diff],
               SET_LEAF(l));

    write_unlock(n.get());  // n is not obsolete.
    if (parent) write_unlock(parent.get());
    *old_val_addr = nullptr;
    return true;
  }

RECURSE_SEARCH:;

  // Find a child to recurse to
  std::shared_ptr<tree_node> *child = find_child(n.get(), key[depth]);

  if (!check_or_restart(n.get(), version)) return false;

  if (!child) {
    bool full = node_full(n.get());
    if (full) {
      if (!upgrade_to_write_lock_or_restart(parent ? parent.get() : nullptr,
                                            parent_version))
        return false;
      if (!upgrade_to_write_lock_or_restart(n.get(), version,
                                            parent ? parent.get() : nullptr))
        return false;
    } else {
      if (!upgrade_to_write_lock_or_restart(n.get(), version)) return false;
    }

    // No child, node goes within us
    std::shared_ptr<tree_node> l =
        make_leaf_shared_ptr(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l.get();
    add_child(tree, n.get(), ref, key[depth], SET_LEAF(l));

    if (full) {
      write_unlock_obsolete(n.get());  // n is obsolete.
      if (parent) write_unlock(parent.get());
    } else {
      write_unlock(n.get());
    }
    *old_val_addr = nullptr;
    return true;
  }

  if (!read_unlock_or_restart(parent ? parent.get() : nullptr, parent_version))
    return false;

  std::shared_ptr<tree_node> childptr = std::atomic_load(child);
  // If we are at a leaf, we need to replace it with a node
  if (IS_LEAF(childptr)) {
    // NOTE(Alec): we only update the current node,
    // because we only have two leaves in the new NODE4 without updating ref.
    if (!upgrade_to_write_lock_or_restart(n.get(), version)) return false;

    ++depth;
    // Check if we are updating an existing value
    if (!leaf_matches(childptr.get(), key, key_len, depth)) {
      *old = 1;
      void *old_val = childptr->value();
      if (replace) childptr->set_value(value);
      if (new_leaf) *new_leaf = childptr.get();

      write_unlock(n.get());
      *old_val_addr = old_val;
      return true;
    }

    // New value, we must split the leaf into a node4
    std::shared_ptr<tree_node> new_node = alloc_node(tree, NODE4);

    // Create a new leaf
    std::shared_ptr<tree_node> l2 =
        make_leaf_shared_ptr(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l2.get();

    /* NOTE(Alec): if the new leaf shares the prefix with the
       old leaf but has shorter length, a new child with '\0'
       will be created.
    */

    // Determine longest prefix
    int longest_prefix = longest_common_prefix(childptr.get(), l2.get(), depth);
    new_node->set_partial_len(longest_prefix);
    memcpy(new_node->partial(), key + depth,
           min(MAX_PREFIX_LEN, longest_prefix));

    // Add the leafs to the new node4
    std::atomic_store(child, new_node);
    add_child4(tree, new_node.get(), child,
               childptr->key()[depth + longest_prefix], SET_LEAF(childptr));
    add_child4(tree, new_node.get(), child, l2->key()[depth + longest_prefix],
               SET_LEAF(l2));

    write_unlock(n.get());
    *old_val_addr = nullptr;
    return true;
  }

  return recursive_insert(tree, childptr, child, n, version, key, key_len,
                          value, depth + 1, old, replace, new_leaf,
                          old_val_addr);
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
void *art_insert(art_tree *t, const unsigned char *key, int key_len,
                 void *value) {
  // printf("\n----- key: %s\n", key);
  while (true) {
    int old_val = 0;
    void *old;
    std::shared_ptr<tree_node> root = std::atomic_load(&t->root);
    if (recursive_insert(t, root, &t->root, nullptr, 0, key, key_len, value, 0,
                         &old_val, 1, nullptr, &old)) {
      if (!old_val) t->size++;
      return old;
    }
  }
}

void *art_insert_get_leaf(art_tree *t, const unsigned char *key, int key_len,
                          void *value, tree_node **new_leaf) {
  // printf("\n----- key: %s\n", key);
  while (true) {
    int old_val = 0;
    void *old;
    std::shared_ptr<tree_node> root = std::atomic_load(&t->root);
    if (recursive_insert(t, root, &t->root, nullptr, 0, key, key_len, value, 0,
                         &old_val, 1, new_leaf, &old)) {
      if (!old_val) t->size++;
      return old;
    }
  }
}

/**
 * inserts a new value into the art tree (no replace)
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void *art_insert_no_replace(art_tree *t, const unsigned char *key, int key_len,
                            void *value) {
  while (true) {
    int old_val = 0;
    void *old;
    std::shared_ptr<tree_node> root = std::atomic_load(&t->root);
    if (recursive_insert(t, root, &t->root, nullptr, 0, key, key_len, value, 0,
                         &old_val, 0, nullptr, &old)) {
      if (!old_val) t->size++;
      return old;
    }
  }
}

static void remove_child256(art_tree *t, std::shared_ptr<tree_node> n,
                            std::shared_ptr<tree_node> *ref, unsigned char c) {
  std::atomic_store(&(n->children()[c]), std::shared_ptr<tree_node>());
  n->set_num_children(n->num_children() - 1);

  // Resize to a node48 on underflow, not immediately to prevent
  // trashing if we sit on the 48/49 boundary
  if (n->num_children() == 37) {
    std::shared_ptr<tree_node> new_node = alloc_node(t, NODE48);
    copy_header(new_node.get(), n.get());

    int pos = 0;
    for (int i = 0; i < 256; i++) {
      std::shared_ptr<tree_node> child = std::atomic_load(&(n->children()[i]));
      if (child) {
        new_node->children()[pos] = child;
        new_node->keys()[i] = pos + 1;
        pos++;
      }
    }
    std::atomic_store(ref, new_node);
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node256));
  }
}

static void remove_child48(art_tree *t, std::shared_ptr<tree_node> n,
                           std::shared_ptr<tree_node> *ref, unsigned char c) {
  int pos = n->keys()[c];
  n->keys()[c] = 0;
  std::atomic_store(&(n->children()[pos - 1]), std::shared_ptr<tree_node>());
  n->set_num_children(n->num_children() - 1);

  if (n->num_children() == 12) {
    std::shared_ptr<tree_node> new_node = alloc_node(t, NODE16);
    copy_header(new_node.get(), n.get());

    int child = 0;
    for (int i = 0; i < 256; i++) {
      pos = n->keys()[i];
      if (pos) {
        new_node->keys()[child] = i;
        new_node->children()[child] =
            std::atomic_load(&(n->children()[pos - 1]));
        child++;
      }
    }
    std::atomic_store(ref, new_node);
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node48));
  }
}

static void remove_child16(art_tree *t, std::shared_ptr<tree_node> n,
                           std::shared_ptr<tree_node> *ref, int pos) {
  memmove(n->keys() + pos, n->keys() + pos + 1, n->num_children() - 1 - pos);
  for (int i = 0; i < n->num_children() - 1 - pos; i++)
    std::atomic_store(&(n->children()[pos + i]),
                      std::atomic_load(&(n->children()[pos + i + 1])));
  n->set_num_children(n->num_children() - 1);

  if (n->num_children() == 3) {
    std::shared_ptr<tree_node> new_node = alloc_node(t, NODE4);
    copy_header(new_node.get(), n.get());
    memcpy(new_node->keys(), n->keys(), 4);
    for (int i = 0; i < 4; i++)
      std::atomic_store(&(new_node->children()[i]),
                        std::atomic_load(&(n->children()[i])));
    std::atomic_store(ref, new_node);
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node16));
  }
}

static void remove_child4(art_tree *t, std::shared_ptr<tree_node> n,
                          std::shared_ptr<tree_node> *ref, int pos) {
  memmove(n->keys() + pos, n->keys() + pos + 1, n->num_children() - 1 - pos);
  for (int i = 0; i < n->num_children() - 1 - pos; i++)
    std::atomic_store(&(n->children()[pos + i]),
                      std::atomic_load(&(n->children()[pos + i + 1])));
  n->set_num_children(n->num_children() - 1);

  // Remove nodes with only a single child
  if (n->num_children() == 1 && (&t->root) != ref) {
    std::shared_ptr<tree_node> child = std::atomic_load(&(n->children()[0]));
    if (!IS_LEAF(child)) {
      // Concatenate the prefixes
      int prefix = n->partial_len();
      if (prefix < MAX_PREFIX_LEN) {
        n->partial()[prefix] = n->keys()[0];
        prefix++;
      }
      if (prefix < MAX_PREFIX_LEN) {
        int sub_prefix = min(child->partial_len(), MAX_PREFIX_LEN - prefix);
        memcpy(n->partial() + prefix, child->partial(), sub_prefix);
        prefix += sub_prefix;
      }

      // Store the prefix in the child
      memcpy(child->partial(), n->partial(), min(prefix, MAX_PREFIX_LEN));
      child->set_partial_len(child->partial_len() + n->partial_len() + 1);
    }
    std::atomic_store(ref, child);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node4));
  }
}

static void remove_child(art_tree *t, std::shared_ptr<tree_node> n,
                         std::shared_ptr<tree_node> *ref, unsigned char c,
                         int pos) {
  switch (n->type()) {
    case NODE4:
      return remove_child4(t, n, ref, pos);
    case NODE16:
      return remove_child16(t, n, ref, pos);
    case NODE48:
      return remove_child48(t, n, ref, c);
    case NODE256:
      return remove_child256(t, n, ref, c);
    default:
      abort();
  }
}

bool need_compression(art_tree *t, tree_node *n,
                      std::shared_ptr<tree_node> *ref) {
  switch (n->type()) {
    case NODE4:
      return n->num_children() == 2 && (&t->root) != ref;
    case NODE16:
      return n->num_children() == 4;
    case NODE48:
      return n->num_children() == 13;
    case NODE256:
      return n->num_children() == 38;
    default:
      abort();
  }
}

static bool recursive_delete(art_tree *t, std::shared_ptr<tree_node> n,
                             std::shared_ptr<tree_node> *ref,
                             std::shared_ptr<tree_node> parent,
                             uint64_t parent_version, const unsigned char *key,
                             int key_len, int depth,
                             std::shared_ptr<tree_node> *leaf_addr) {
  uint32_t version;

AGAIN:
  if (!read_lock_or_restart(n.get(), &version)) return false;
  if (!read_unlock_or_restart(parent ? parent.get() : nullptr, parent_version))
    return false;

  // Bail if the prefix does not match
  if (n->partial_len()) {
    int prefix_len = check_prefix(n.get(), key, key_len, depth);
    if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len())) {
      if (!read_unlock_or_restart(n.get(), version)) return false;
      std::atomic_store(leaf_addr, std::shared_ptr<tree_node>());
      return true;
    }
    depth = depth + n->partial_len();
  }

  // Find child node
  std::shared_ptr<tree_node> *child = find_child(n.get(), key[depth]);
  if (!check_or_restart(n.get(), version)) return false;
  if (!child) {
    if (!read_unlock_or_restart(n.get(), version)) return false;
    std::atomic_store(leaf_addr, std::shared_ptr<tree_node>());
    return true;
  }

  std::shared_ptr<tree_node> childptr = std::atomic_load(child);
  // If the child is leaf, delete from this node
  if (IS_LEAF(childptr)) {
    if (!leaf_matches(childptr.get(), key, key_len, depth)) {
      bool compress = need_compression(t, n.get(), ref);
      if (compress) {
        if (!upgrade_to_write_lock_or_restart(parent ? parent.get() : nullptr,
                                              parent_version))
          return false;
        if (!upgrade_to_write_lock_or_restart(n.get(), version,
                                              parent ? parent.get() : nullptr))
          return false;
      } else if (!upgrade_to_write_lock_or_restart(n.get(), version))
        return false;

      remove_child(t, n, ref, key[depth], child - n->children());

      if (compress) {
        write_unlock_obsolete(n.get());  // n is obsolete.
        write_unlock(parent ? parent.get() : nullptr);
      } else
        write_unlock(n.get());
      std::atomic_store(leaf_addr, childptr);
      return true;
    }
    if (!read_unlock_or_restart(n.get(), version)) return false;
    std::atomic_store(leaf_addr, std::shared_ptr<tree_node>());
    return true;
  }

  ++depth;
  parent = n;
  parent_version = version;
  ref = child;
  n = childptr;
  goto AGAIN;
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return nullptr if the item was not found, otherwise
 * the value pointer is returned.
 */
void *art_delete(art_tree *t, const unsigned char *key, int key_len) {
  std::shared_ptr<tree_node> l;
  while (true) {
    if (recursive_delete(t, std::atomic_load(&t->root), &t->root, nullptr, 0,
                         key, key_len, 0, &l)) {
      if (l) {
        t->size--;
        __sync_fetch_and_add(&(t->mem_size), -sizeof(art_leaf) - key_len);
        return l->value();
      }
      return nullptr;
    }
  }
}

// Recursively iterates over the tree
static bool recursive_iter(std::shared_ptr<tree_node> n, art_callback cb,
                           void *data, int *res) {
  // Handle base cases
  if (!n) return true;

  uint32_t version;
  if (!read_lock_or_restart(n.get(), &version)) {
    *res = 0;
    return false;
  }

  if (IS_LEAF(n)) {
    return cb(data, (const unsigned char *)n->key(), n->key_len(), n->value());
  }

  int idx;
  std::shared_ptr<tree_node> child;
  switch (n->type()) {
    case NODE4:
      for (int i = 0; i < n->num_children(); i++) {
        child = std::atomic_load(&n->children()[i]);
        if (!read_unlock_or_restart(n.get(), version)) return false;
        return recursive_iter(child, cb, data, res);
      }
      break;

    case NODE16:
      for (int i = 0; i < n->num_children(); i++) {
        child = std::atomic_load(&n->children()[i]);
        if (!read_unlock_or_restart(n.get(), version)) return false;
        return recursive_iter(child, cb, data, res);
      }
      break;

    case NODE48:
      for (int i = 0; i < 256; i++) {
        idx = n->keys()[i];
        if (!idx) continue;

        child = std::atomic_load(&n->children()[idx - 1]);
        if (!read_unlock_or_restart(n.get(), version)) return false;
        return recursive_iter(child, cb, data, res);
      }
      break;

    case NODE256:
      for (int i = 0; i < 256; i++) {
        child = std::atomic_load(&n->children()[i]);
        if (!read_unlock_or_restart(n.get(), version)) return false;
        if (!child) continue;
        return recursive_iter(child, cb, data, res);
      }
      break;

    default:
      abort();
  }
  if (!read_unlock_or_restart(n.get(), version)) {
    *res = 0;
    return false;
  }
  return true;
}

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
int art_iter(art_tree *t, art_callback cb, art_clean_callback clean_cb,
             void *data) {
  int res = 0;
  while (true) {
    if (recursive_iter(std::atomic_load(&t->root), cb, data, &res)) break;
    clean_cb(data);
  }
  return res;
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(tree_node *n, const unsigned char *prefix,
                               int prefix_len) {
  // Fail if the key length is too short
  if (n->key_len() < (uint32_t)prefix_len) return 1;

  // Compare the keys
  return memcmp(n->key(), prefix, prefix_len);
}

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
// int art_iter_prefix(art_tree *t, const unsigned char *key, int key_len,
// art_callback cb, void *data) {
//     std::shared_ptr<tree_node>* child;
//     std::shared_ptr<tree_node> n;
//     int prefix_len, depth, res;

// AGAIN:
//     n = std::atomic_load(&t->root);
//     depth = 0;
//     res = 0;
//     uint32_t version;
//     while (n) {
//         if (!read_lock_or_restart(n.get(), &version)) {
//             res = 0;
//             goto AGAIN;
//         }

//         // Might be a leaf
//         if (IS_LEAF(n)) {
//             // Check if the expanded path matches
//             if (!leaf_prefix_matches(n.get(), key, key_len)) {
//                 return cb(data, (const unsigned char*)n->key(), n->key_len(),
//                 n->value());
//             }
//             return 0;
//         }

//         // If the depth matches the prefix, we need to handle this node
//         if (depth == key_len) {
//             std::shared_ptr<tree_node> l = minimum(n);
//             if (!leaf_prefix_matches(l.get(), key, key_len)) {
//                 if (!read_unlock_or_restart(n.get(), version)) goto AGAIN;
//                 while (res == 0 && !recursive_iter(n, cb, data, &res)) {}
//                 return res;
//             }
//             return 0;
//         }

//         // Bail if the prefix does not match
//         if (n->partial_len()) {
//             prefix_len = prefix_mismatch(n, key, key_len, depth);

//             // Guard if the mis-match is longer than the MAX_PREFIX_LEN
//             if ((uint32_t)prefix_len > n->partial_len()) {
//                 prefix_len = n->partial_len();
//             }

//             // If there is no match, search is terminated
//             if (!prefix_len) {
//                 return 0;

//             // If we've matched the prefix, iterate on this node
//             } else if (depth + prefix_len == key_len) {
//                 if (!read_unlock_or_restart(n.get(), version)) goto AGAIN;
//                 while (res == 0 && !recursive_iter(n, cb, data, &res)) {}
//                 return res;
//             }

//             // if there is a full match, go deeper
//             depth = depth + n->partial_len();
//         }

//         // Recursively search
//         child = find_child(n.get(), key[depth]);
//         if (!check_or_restart(n.get(), version)) goto AGAIN;
//         n = (child) ? *child : nullptr;
//         if (!read_unlock_or_restart(n.get(), version)) goto AGAIN;
//         depth++;
//     }
//     return 0;
// }

}  // namespace artoptlock