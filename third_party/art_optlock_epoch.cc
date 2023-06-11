#include "art_optlock_epoch.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "mem/inverted_index.h"
#include "mem/mmap.h"

namespace artoptlockepoch {
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
#define IS_LEAF(x) (((uintptr_t)x & 0x8000000000000000))
#define SET_LEAF(x) ((void *)((uintptr_t)x | 0x8000000000000000))
#define LEAF_RAW(x) ((art_leaf *)((void *)((uintptr_t)x & 0x7fffffffffffffff)))

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
#ifdef MMAP_ART_NODE
static art_node *alloc_node(art_tree *tree, uint8_t type) {
  art_node *n;
  switch (type) {
    case NODE4:
      n = (art_node *)(tsdb::mem::mmap_node4->alloc());
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node4));
      break;
    case NODE16:
      n = (art_node *)(tsdb::mem::mmap_node16->alloc());
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node16));
      break;
    case NODE48:
      n = (art_node *)(tsdb::mem::mmap_node48->alloc());
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node48));
      break;
    case NODE256:
      n = (art_node *)(tsdb::mem::mmap_node256->alloc());
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node256));
      break;
    default:
      printf("abort in alloc_node()\n");
      abort();
  }
  n->type = type;
  return n;
}

static art_node *alloc_node(uint8_t type) {
  art_node *n;
  switch (type) {
    case NODE4:
      n = (art_node *)(tsdb::mem::mmap_node4->alloc());
      break;
    case NODE16:
      n = (art_node *)(tsdb::mem::mmap_node16->alloc());
      break;
    case NODE48:
      n = (art_node *)(tsdb::mem::mmap_node48->alloc());
      break;
    case NODE256:
      n = (art_node *)(tsdb::mem::mmap_node256->alloc());
      break;
    default:
      printf("abort in alloc_node()\n");
      abort();
  }
  n->type = type;
  return n;
}
#else
static art_node *alloc_node(art_tree *tree, uint8_t type) {
  art_node *n;
  switch (type) {
    case NODE4:
      n = (art_node *)calloc(1, sizeof(art_node4));
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node4));
      break;
    case NODE16:
      n = (art_node *)calloc(1, sizeof(art_node16));
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node16));
      break;
    case NODE48:
      n = (art_node *)calloc(1, sizeof(art_node48));
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node48));
      break;
    case NODE256:
      n = (art_node *)calloc(1, sizeof(art_node256));
      __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node256));
      break;
    default:
      printf("abort in alloc_node()\n");
      abort();
  }
  n->type = type;
  return n;
}

static art_node *alloc_node(uint8_t type) {
  art_node *n;
  switch (type) {
    case NODE4:
      n = (art_node *)calloc(1, sizeof(art_node4));
      break;
    case NODE16:
      n = (art_node *)calloc(1, sizeof(art_node16));
      break;
    case NODE48:
      n = (art_node *)calloc(1, sizeof(art_node48));
      break;
    case NODE256:
      n = (art_node *)calloc(1, sizeof(art_node256));
      break;
    default:
      printf("abort in alloc_node()\n");
      abort();
  }
  n->type = type;
  return n;
}
#endif

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
  t->root = NULL;
  t->size = 0;
  t->mem_size = 0;
  t->root = alloc_node(t, NODE4);
  __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node4));
  return 0;
}

// Recursively destroys the tree
static void destroy_node(art_node *n) {
  // Break if null
  if (!n) return;

  // Special case leafs
  if (IS_LEAF(n)) {
    free(LEAF_RAW(n));
    return;
  }

  // Handle each node type
  int i, idx;
  union {
    art_node4 *p1;
    art_node16 *p2;
    art_node48 *p3;
    art_node256 *p4;
  } p;
  switch (n->type) {
    case NODE4:
      p.p1 = (art_node4 *)n;
      for (i = 0; i < n->num_children; i++) {
        destroy_node(p.p1->children[i]);
      }
      break;

    case NODE16:
      p.p2 = (art_node16 *)n;
      for (i = 0; i < n->num_children; i++) {
        destroy_node(p.p2->children[i]);
      }
      break;

    case NODE48:
      p.p3 = (art_node48 *)n;
      for (i = 0; i < 256; i++) {
        idx = ((art_node48 *)n)->keys[i];
        if (!idx) continue;
        destroy_node(p.p3->children[idx - 1]);
      }
      break;

    case NODE256:
      p.p4 = (art_node256 *)n;
      for (i = 0; i < 256; i++) {
        if (p.p4->children[i]) destroy_node(p.p4->children[i]);
      }
      break;

    default:
      printf("abort in destroy_node()\n");
      abort();
  }

#ifdef MMAP_ART_NODE
  switch (n->type) {
    case NODE4:
      tsdb::mem::mmap_node4->reclaim(n);
      break;

    case NODE16:
      tsdb::mem::mmap_node16->reclaim(n);
      break;

    case NODE48:
      tsdb::mem::mmap_node48->reclaim(n);
      break;

    case NODE256:
      tsdb::mem::mmap_node256->reclaim(n);
      break;
  }
#else
  // Free ourself on the way up
  free(n);
#endif
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
  destroy_node(t->root);
  return 0;
}

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);
#endif

static art_node **find_child(art_node *n, unsigned char c) {
  int i, mask, bitfield;
  union {
    art_node4 *p1;
    art_node16 *p2;
    art_node48 *p3;
    art_node256 *p4;
  } p;
  switch (n->type) {
    case NODE4:
      p.p1 = (art_node4 *)n;
      for (i = 0; i < n->num_children; i++) {
        /* this cast works around a bug in gcc 5.1 when unrolling loops
         * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
         */
        if (((unsigned char *)p.p1->keys)[i] == c) return &p.p1->children[i];
      }
      break;

      {
        case NODE16:
          p.p2 = (art_node16 *)n;

// support non-86 architectures
#ifdef __i386__
          // Compare the key to all 16 stored keys
          __m128i cmp;
          cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                               _mm_loadu_si128((__m128i *)p.p2->keys));

          // Use a mask to ignore children that don't exist
          mask = (1 << n->num_children) - 1;
          bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
          // Compare the key to all 16 stored keys
          __m128i cmp;
          cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                               _mm_loadu_si128((__m128i *)p.p2->keys));

          // Use a mask to ignore children that don't exist
          mask = (1 << n->num_children) - 1;
          bitfield = _mm_movemask_epi8(cmp) & mask;
#else
          // Compare the key to all 16 stored keys
          bitfield = 0;
          for (i = 0; i < 16; ++i) {
            if (p.p2->keys[i] == c) bitfield |= (1 << i);
          }

          // Use a mask to ignore children that don't exist
          mask = (1 << n->num_children) - 1;
          bitfield &= mask;
#endif
#endif

          /*
           * If we have a match (any bit set) then we can
           * return the pointer match using ctz to get
           * the index.
           */
          if (bitfield) return &p.p2->children[__builtin_ctz(bitfield)];
          break;
      }

    case NODE48:
      p.p3 = (art_node48 *)n;
      i = p.p3->keys[c];
      if (i) return &p.p3->children[i - 1];
      break;

    case NODE256:
      p.p4 = (art_node256 *)n;
      if (p.p4->children[c]) return &p.p4->children[c];
      break;

    default:
      printf("abort in find_child()\n");
      abort();
  }
  return NULL;
}

// Simple inlined if
static inline int min(int a, int b) { return (a < b) ? a : b; }

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const art_node *n, const unsigned char *key,
                        int key_len, int depth) {
  int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), key_len - depth);
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (n->partial[idx] != key[depth + idx]) return idx;
  }
  return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, const unsigned char *key,
                        int key_len, int depth) {
  (void)depth;
  // Fail if the key lengths are different
  if (n->key_len != (uint32_t)key_len) return 1;

  // Compare the keys starting at the depth
  return memcmp(n->key, key, key_len);
}

bool art_search_helper(const unsigned char *key, int key_len, art_node *node,
                       int depth, art_node *parent, uint32_t parent_version,
                       void **value_addr) {
  art_node **child;
  art_node *nextnode;
  uint32_t version;

RECUR:
  if (!read_lock_or_restart(node, &version)) {
    *value_addr = nullptr;
    return false;
  }

  if (!read_unlock_or_restart(parent, parent_version)) {
    *value_addr = nullptr;
    return false;
  }

  // Check if prefix matches, may increment level.
  if (node->partial_len) {
    int prefix_len = check_prefix(node, key, key_len, depth);
    if (prefix_len != min(MAX_PREFIX_LEN, node->partial_len)) {
      *value_addr = nullptr;
      if (!read_unlock_or_restart(node, version)) return false;
      return true;  // Key not found.
    }
    depth = depth + node->partial_len;
  }

  // Recursively search
  child = find_child(node, key[depth]);
  if (!check_or_restart(node, version)) {
    *value_addr = nullptr;
    return false;
  }

  nextnode = (child ? __sync_val_compare_and_swap(child, 0, 0) : nullptr);

  if (!nextnode) {
    *value_addr = nullptr;
    if (!read_unlock_or_restart(node, version)) return false;
    return true;
  }

  // Might be a leaf
  if (IS_LEAF(nextnode)) {
    art_leaf *leaf = LEAF_RAW(nextnode);
    // Check if the expanded path matches
    if (!leaf_matches(leaf, key, key_len, depth))
      *value_addr = leaf->value;
    else
      *value_addr = nullptr;
    if (!read_unlock_or_restart(node, version)) return false;
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
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void *art_search(const art_tree *t, const unsigned char *key, int key_len) {
  void *value = nullptr;
  while (!art_search_helper(
      key, key_len,
      __sync_val_compare_and_swap(&const_cast<art_tree *>(t)->root, 0, 0), 0,
      nullptr, 0, &value)) {
  }
  return value;
}

// Find the minimum leaf under a node
static bool minimum(art_node *n, uint32_t version, art_leaf **leaf_addr) {
  int idx;
  art_node *next;
  switch (n->type) {
    case NODE4:
      next =
          __sync_val_compare_and_swap(&(((art_node4 *)n)->children[0]), 0, 0);
      break;
    case NODE16:
      next =
          __sync_val_compare_and_swap(&(((art_node16 *)n)->children[0]), 0, 0);
      break;
    case NODE48:
      idx = 0;
      while (!((const art_node48 *)n)->keys[idx]) idx++;
      idx = ((const art_node48 *)n)->keys[idx] - 1;
      next = __sync_val_compare_and_swap(&(((art_node48 *)n)->children[idx]), 0,
                                         0);
      break;
    case NODE256:
      idx = 0;
      while (!((const art_node256 *)n)->children[idx]) idx++;
      next = __sync_val_compare_and_swap(&(((art_node256 *)n)->children[idx]),
                                         0, 0);
      break;
    default:
      printf("abort in minimum()\n");
      abort();
  }

  if (!check_or_restart(n, version)) return false;

  if (IS_LEAF(next)) {
    __sync_lock_test_and_set(leaf_addr, LEAF_RAW(next));
    if (!read_unlock_or_restart(n, version)) return false;
    return true;
  }

  if (!read_lock_or_restart(next, &version)) return false;
  return minimum(next, version, leaf_addr);
}

static art_leaf *minimum_unsafe(art_node *n) {
  art_node *next;
  int idx;
  switch (n->type) {
    case NODE4:
      next =
          __sync_val_compare_and_swap(&(((art_node4 *)n)->children[0]), 0, 0);
      break;
    case NODE16:
      next =
          __sync_val_compare_and_swap(&(((art_node16 *)n)->children[0]), 0, 0);
      break;
    case NODE48:
      idx = 0;
      while (!((const art_node48 *)n)->keys[idx]) idx++;
      idx = ((const art_node48 *)n)->keys[idx] - 1;
      next = __sync_val_compare_and_swap(&(((art_node48 *)n)->children[idx]), 0,
                                         0);
      break;
    case NODE256:
      idx = 0;
      while (!((const art_node256 *)n)->children[idx]) idx++;
      next = __sync_val_compare_and_swap(&(((art_node256 *)n)->children[idx]),
                                         0, 0);
      break;
    default:
      printf("abort in minimum_unsafe()\n");
      abort();
  }
  if (IS_LEAF(next)) return LEAF_RAW(next);
  return minimum_unsafe(next);
}

// Find the maximum leaf under a node
static art_leaf *maximum(const art_node *n) {
  // Handle base cases
  if (!n) return NULL;
  if (IS_LEAF(n)) return LEAF_RAW(n);

  int idx;
  switch (n->type) {
    case NODE4:
      return maximum(((const art_node4 *)n)->children[n->num_children - 1]);
    case NODE16:
      return maximum(((const art_node16 *)n)->children[n->num_children - 1]);
    case NODE48:
      idx = 255;
      while (!((const art_node48 *)n)->keys[idx]) idx--;
      idx = ((const art_node48 *)n)->keys[idx] - 1;
      return maximum(((const art_node48 *)n)->children[idx]);
    case NODE256:
      idx = 255;
      while (!((const art_node256 *)n)->children[idx]) idx--;
      return maximum(((const art_node256 *)n)->children[idx]);
    default:
      printf("abort in maximum()\n");
      abort();
  }
}

static art_leaf *make_leaf(art_tree *t, const unsigned char *key, int key_len,
                           void *value) {
  art_leaf *l = (art_leaf *)calloc(1, sizeof(art_leaf) + key_len);
  __sync_fetch_and_add(&(t->mem_size), sizeof(art_leaf) + key_len);
  l->value = value;
  l->key_len = key_len;
  memcpy(l->key, key, key_len);
  return l;
}

static int longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
  int max_cmp = min(l1->key_len, l2->key_len) - depth;
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (l1->key[depth + idx] != l2->key[depth + idx]) return idx;
  }
  return idx;
}

static void copy_header(art_node *dest, art_node *src) {
  dest->num_children = src->num_children;
  dest->partial_len = src->partial_len;
  memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}

static bool add_child256(art_tree *tree, art_node256 *n, art_node **ref,
                         unsigned char c, void *child) {
  (void)ref;
  n->n.num_children++;
  n->children[c] = (art_node *)child;
  return false;
}

static bool add_child48(art_tree *tree, art_node48 *n, art_node **ref,
                        unsigned char c, void *child) {
  if (n->n.num_children < 48) {
    int pos = 0;
    while (n->children[pos]) pos++;
    n->children[pos] = (art_node *)child;
    n->keys[c] = pos + 1;
    n->n.num_children++;
    return false;
  } else {
    art_node256 *new_node = (art_node256 *)alloc_node(tree, NODE256);
    for (int i = 0; i < 256; i++) {
      if (n->keys[i]) {
        new_node->children[i] = n->children[n->keys[i] - 1];
      }
    }
    copy_header((art_node *)new_node, (art_node *)n);
    *ref = (art_node *)new_node;
    // free(n);
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node48));
    add_child256(tree, new_node, ref, c, child);
    return true;
  }
}

/************************* GC Version *************************/
static bool add_child48(
    art_tree *tree, art_node48 *n, art_node **ref, unsigned char c, void *child,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  if (n->n.num_children < 48) {
    int pos = 0;
    while (n->children[pos]) pos++;
    n->children[pos] = (art_node *)child;
    n->keys[c] = pos + 1;
    n->n.num_children++;
    return false;
  } else {
    art_node256 *new_node = pool->get_node256(&(tree->mem_size));
    for (int i = 0; i < 256; i++) {
      if (n->keys[i]) {
        new_node->children[i] = n->children[n->keys[i] - 1];
      }
    }
    copy_header((art_node *)new_node, (art_node *)n);
    *ref = (art_node *)new_node;
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE48));
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node48));
    add_child256(tree, new_node, ref, c, child);
    return true;
  }
}

static bool add_child16(art_tree *tree, art_node16 *n, art_node **ref,
                        unsigned char c, void *child) {
  if (n->n.num_children < 16) {
    unsigned mask = (1 << n->n.num_children) - 1;

// support non-x86 architectures
#ifdef __i386__
    __m128i cmp;

    // Compare the key to all 16 stored keys
    cmp = _mm_cmplt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *)n->keys));

    // Use a mask to ignore children that don't exist
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
    __m128i cmp;

    // Compare the key to all 16 stored keys
    cmp = _mm_cmplt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *)n->keys));

    // Use a mask to ignore children that don't exist
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
    // Compare the key to all 16 stored keys
    unsigned bitfield = 0;
    for (short i = 0; i < 16; ++i) {
      if (c < n->keys[i]) bitfield |= (1 << i);
    }

    // Use a mask to ignore children that don't exist
    bitfield &= mask;
#endif
#endif

    // Check if less than any
    unsigned idx;
    if (bitfield) {
      idx = __builtin_ctz(bitfield);
      memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
      memmove(n->children + idx + 1, n->children + idx,
              (n->n.num_children - idx) * sizeof(void *));
    } else
      idx = n->n.num_children;

    // Set the child
    n->keys[idx] = c;
    n->children[idx] = (art_node *)child;
    n->n.num_children++;
    return false;
  } else {
    art_node48 *new_node = (art_node48 *)alloc_node(tree, NODE48);

    // Copy the child pointers and populate the key map
    memcpy(new_node->children, n->children, sizeof(void *) * n->n.num_children);
    for (int i = 0; i < n->n.num_children; i++) {
      new_node->keys[n->keys[i]] = i + 1;
    }
    copy_header((art_node *)new_node, (art_node *)n);
    *ref = (art_node *)new_node;
    // free(n);
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node16));
    add_child48(tree, new_node, ref, c, child);
    return true;
  }
}

/************************* GC Version *************************/
static bool add_child16(
    art_tree *tree, art_node16 *n, art_node **ref, unsigned char c, void *child,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  if (n->n.num_children < 16) {
    unsigned mask = (1 << n->n.num_children) - 1;

// support non-x86 architectures
#ifdef __i386__
    __m128i cmp;

    // Compare the key to all 16 stored keys
    cmp = _mm_cmplt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *)n->keys));

    // Use a mask to ignore children that don't exist
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
    __m128i cmp;

    // Compare the key to all 16 stored keys
    cmp = _mm_cmplt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *)n->keys));

    // Use a mask to ignore children that don't exist
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
    // Compare the key to all 16 stored keys
    unsigned bitfield = 0;
    for (short i = 0; i < 16; ++i) {
      if (c < n->keys[i]) bitfield |= (1 << i);
    }

    // Use a mask to ignore children that don't exist
    bitfield &= mask;
#endif
#endif

    // Check if less than any
    unsigned idx;
    if (bitfield) {
      idx = __builtin_ctz(bitfield);
      memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
      memmove(n->children + idx + 1, n->children + idx,
              (n->n.num_children - idx) * sizeof(void *));
    } else
      idx = n->n.num_children;

    // Set the child
    n->keys[idx] = c;
    n->children[idx] = (art_node *)child;
    n->n.num_children++;
    return false;
  } else {
    art_node48 *new_node = pool->get_node48(&(tree->mem_size));

    // Copy the child pointers and populate the key map
    memcpy(new_node->children, n->children, sizeof(void *) * n->n.num_children);
    for (int i = 0; i < n->n.num_children; i++) {
      new_node->keys[n->keys[i]] = i + 1;
    }
    copy_header((art_node *)new_node, (art_node *)n);
    *ref = (art_node *)new_node;
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE16));
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node16));
    add_child48(tree, new_node, ref, c, child);
    return true;
  }
}

static bool add_child4(art_tree *tree, art_node4 *n, art_node **ref,
                       unsigned char c, void *child) {
  if (n->n.num_children < 4) {
    int idx;
    for (idx = 0; idx < n->n.num_children; idx++) {
      if (c < n->keys[idx]) break;
    }

    // Shift to make room
    memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
    memmove(n->children + idx + 1, n->children + idx,
            (n->n.num_children - idx) * sizeof(void *));

    // Insert element
    n->keys[idx] = c;
    n->children[idx] = (art_node *)child;
    n->n.num_children++;
    return false;
  } else {
    art_node16 *new_node = (art_node16 *)alloc_node(tree, NODE16);

    // Copy the child pointers and the key map
    memcpy(new_node->children, n->children, sizeof(void *) * n->n.num_children);
    memcpy(new_node->keys, n->keys, sizeof(unsigned char) * n->n.num_children);
    copy_header((art_node *)new_node, (art_node *)n);
    *ref = (art_node *)new_node;
    // free(n);
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node4));
    add_child16(tree, new_node, ref, c, child);
    return true;
  }
}

/************************* GC Version *************************/
static bool add_child4(
    art_tree *tree, art_node4 *n, art_node **ref, unsigned char c, void *child,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  if (n->n.num_children < 4) {
    int idx;
    for (idx = 0; idx < n->n.num_children; idx++) {
      if (c < n->keys[idx]) break;
    }

    // Shift to make room
    memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
    memmove(n->children + idx + 1, n->children + idx,
            (n->n.num_children - idx) * sizeof(void *));

    // Insert element
    n->keys[idx] = c;
    n->children[idx] = (art_node *)child;
    n->n.num_children++;
    return false;
  } else {
    art_node16 *new_node = pool->get_node16(&(tree->mem_size));

    // Copy the child pointers and the key map
    memcpy(new_node->children, n->children, sizeof(void *) * n->n.num_children);
    memcpy(new_node->keys, n->keys, sizeof(unsigned char) * n->n.num_children);
    copy_header((art_node *)new_node, (art_node *)n);
    *ref = (art_node *)new_node;
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE4));
    __sync_fetch_and_add(&(tree->mem_size), -sizeof(art_node4));
    add_child16(tree, new_node, ref, c, child);
    return true;
  }
}

// return true if the node need reclamation.
static bool add_child(art_tree *tree, art_node *n, art_node **ref,
                      unsigned char c, void *child) {
  switch (n->type) {
    case NODE4:
      return add_child4(tree, (art_node4 *)n, ref, c, child);
    case NODE16:
      return add_child16(tree, (art_node16 *)n, ref, c, child);
    case NODE48:
      return add_child48(tree, (art_node48 *)n, ref, c, child);
    case NODE256:
      return add_child256(tree, (art_node256 *)n, ref, c, child);
    default:
      printf("abort in add_child()\n");
      abort();
  }
}

/************************* GC Version *************************/
// return true if the node need reclamation.
static bool add_child(
    art_tree *tree, art_node *n, art_node **ref, unsigned char c, void *child,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  switch (n->type) {
    case NODE4:
      return add_child4(tree, (art_node4 *)n, ref, c, child, epoch, pool,
                        gc_queue);
    case NODE16:
      return add_child16(tree, (art_node16 *)n, ref, c, child, epoch, pool,
                         gc_queue);
    case NODE48:
      return add_child48(tree, (art_node48 *)n, ref, c, child, epoch, pool,
                         gc_queue);
    case NODE256:
      return add_child256(tree, (art_node256 *)n, ref, c, child);
    default:
      printf("abort in add_child()\n");
      abort();
  }
}

bool node_full(art_node *n) {
  switch (n->type) {
    case NODE4:
      return n->num_children >= 4;
    case NODE16:
      return n->num_children >= 16;
    case NODE48:
      return n->num_children >= 48;
    default:
      return false;
  }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static bool prefix_mismatch(art_node *n, uint32_t version, art_node *parent,
                            uint32_t parent_version, const unsigned char *key,
                            int key_len, int depth, int *prefix_diff) {
  int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (n->partial[idx] != key[depth + idx]) {
      *prefix_diff = idx;
      return true;
    }
  }

  // If the prefix is short we can avoid finding a leaf
  if (n->partial_len > MAX_PREFIX_LEN) {
    // Prefix is longer than what we've checked, find a leaf
    art_leaf *l;
    while (true) {
      if (!check_or_restart(n, version) ||
          !check_or_restart(parent, parent_version))
        return false;
      if (minimum(n, version, &l)) {
        if (!check_or_restart(n, version) ||
            !check_or_restart(parent, parent_version))
          return false;
        break;
      }
    }
    max_cmp = min(l->key_len, key_len) - depth;
    for (; idx < max_cmp; idx++) {
      if (l->key[idx + depth] != key[depth + idx]) {
        *prefix_diff = idx;
        return true;
      }
    }
  }
  *prefix_diff = idx;
  return true;
}

static bool recursive_insert(art_tree *tree, art_node *n, art_node **ref,
                             art_node *parent, uint32_t parent_version,
                             const unsigned char *key, int key_len, void *value,
                             int depth, int *old, int replace,
                             art_leaf **new_leaf, void **old_val_addr) {
  uint32_t version = 0;

AGAIN:
  if (!read_lock_or_restart(n, &version)) {
    return false;
  }

  // Check if given node has a prefix
  if (n->partial_len) {
    // Determine if the prefixes differ, since we need to split
    int prefix_diff;
    if (!prefix_mismatch(n, version, parent, parent_version, key, key_len,
                         depth, &prefix_diff))
      return false;
    if ((uint32_t)prefix_diff >= n->partial_len) {
      depth += n->partial_len;
      goto RECURSE_SEARCH;
    }

    if (!upgrade_to_write_lock_or_restart(parent, parent_version)) return false;
    if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;

    // Create a new node
    art_node4 *new_node = (art_node4 *)alloc_node(tree, NODE4);
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    new_node->n.partial_len = prefix_diff;
    memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

    // Adjust the prefix of the old node
    if (n->partial_len <= MAX_PREFIX_LEN) {
      add_child4(tree, new_node, ref, n->partial[prefix_diff], n);
      n->partial_len -= (prefix_diff + 1);
      memmove(n->partial, n->partial + prefix_diff + 1,
              min(MAX_PREFIX_LEN, n->partial_len));
    } else {
      n->partial_len -= (prefix_diff + 1);
      art_leaf *leaf = minimum_unsafe(n);
      add_child4(tree, new_node, ref, leaf->key[depth + prefix_diff], n);
      memcpy(n->partial, leaf->key + depth + prefix_diff + 1,
             min(MAX_PREFIX_LEN, n->partial_len));
    }

    // Insert the new leaf
    art_leaf *l = make_leaf(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l;
    add_child4(tree, new_node, ref, key[depth + prefix_diff], SET_LEAF(l));

    write_unlock(n);  // n is not obsolete.
    write_unlock(parent);
    *old_val_addr = nullptr;
    return true;
  }

RECURSE_SEARCH:;

  // Find a child to recurse to
  art_node **child = find_child(n, key[depth]);

  if (!check_or_restart(n, version)) return false;

  if (!child) {
    bool full = node_full(n);
    if (full) {
      if (!upgrade_to_write_lock_or_restart(parent, parent_version))
        return false;
      if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;
    } else {
      if (!upgrade_to_write_lock_or_restart(n, version)) return false;
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l;
    add_child(tree, n, ref, key[depth], SET_LEAF(l));

    if (full) {
      write_unlock_obsolete(n);  // n is obsolete.
      write_unlock(parent);
    } else {
      write_unlock(n);
    }
    *old_val_addr = nullptr;
    return true;
  }

  if (!read_unlock_or_restart(parent, parent_version)) return false;

  art_node *childptr = __sync_val_compare_and_swap(child, 0, 0);
  // If we are at a leaf, we need to replace it with a node
  if (IS_LEAF(childptr)) {
    // NOTE(Alec): we only update the current node,
    // because we only have two leaves in the new NODE4 without updating ref.
    if (!upgrade_to_write_lock_or_restart(n, version)) return false;
    art_leaf *leaf = LEAF_RAW(childptr);

    ++depth;
    // Check if we are updating an existing value
    if (!leaf_matches(leaf, key, key_len, depth)) {
      *old = 1;
      void *old_val = leaf->value;
      if (replace) leaf->value = value;
      if (new_leaf) *new_leaf = leaf;

      write_unlock(n);
      *old_val_addr = old_val;
      return true;
    }

    // New value, we must split the leaf into a node4
    art_node4 *new_node = (art_node4 *)alloc_node(tree, NODE4);

    // Create a new leaf
    art_leaf *l2 = make_leaf(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l2;

    /* NOTE(Alec): if the new leaf shares the prefix with the
       old leaf but has shorter length, a new child with '\0'
       will be created.
    */

    // Determine longest prefix
    int longest_prefix = longest_common_prefix(leaf, l2, depth);
    new_node->n.partial_len = longest_prefix;
    memcpy(new_node->n.partial, key + depth,
           min(MAX_PREFIX_LEN, longest_prefix));

    // Add the leafs to the new node4
    __sync_lock_test_and_set(child, (art_node *)new_node);
    add_child4(tree, new_node, child, leaf->key[depth + longest_prefix],
               SET_LEAF(leaf));
    add_child4(tree, new_node, child, l2->key[depth + longest_prefix],
               SET_LEAF(l2));

    write_unlock(n);
    *old_val_addr = nullptr;
    return true;
  }

  parent = n;
  parent_version = version;
  n = childptr;
  ref = child;
  ++depth;
  goto AGAIN;
}

/************************* GC Version *************************/
static bool recursive_insert(
    art_tree *tree, art_node *n, art_node **ref, art_node *parent,
    uint32_t parent_version, const unsigned char *key, int key_len, void *value,
    int depth, int *old, int replace, art_leaf **new_leaf, void **old_val_addr,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  uint32_t version = 0;

AGAIN:
  if (!read_lock_or_restart(n, &version)) {
    return false;
  }

  // Check if given node has a prefix
  if (n->partial_len) {
    // Determine if the prefixes differ, since we need to split
    int prefix_diff;
    if (!prefix_mismatch(n, version, parent, parent_version, key, key_len,
                         depth, &prefix_diff))
      return false;
    if ((uint32_t)prefix_diff >= n->partial_len) {
      depth += n->partial_len;
      goto RECURSE_SEARCH;
    }

    if (!upgrade_to_write_lock_or_restart(parent, parent_version)) return false;
    if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;

    // Create a new node
    art_node4 *new_node = pool->get_node4(&(tree->mem_size));
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    new_node->n.partial_len = prefix_diff;
    memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

    // Adjust the prefix of the old node
    if (n->partial_len <= MAX_PREFIX_LEN) {
      add_child4(tree, new_node, ref, n->partial[prefix_diff], n, epoch, pool,
                 gc_queue);
      n->partial_len -= (prefix_diff + 1);
      memmove(n->partial, n->partial + prefix_diff + 1,
              min(MAX_PREFIX_LEN, n->partial_len));
    } else {
      n->partial_len -= (prefix_diff + 1);
      art_leaf *leaf = minimum_unsafe(n);
      add_child4(tree, new_node, ref, leaf->key[depth + prefix_diff], n, epoch,
                 pool, gc_queue);
      memcpy(n->partial, leaf->key + depth + prefix_diff + 1,
             min(MAX_PREFIX_LEN, n->partial_len));
    }

    // Insert the new leaf
    art_leaf *l = make_leaf(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l;
    add_child4(tree, new_node, ref, key[depth + prefix_diff], SET_LEAF(l),
               epoch, pool, gc_queue);

    write_unlock(n);  // n is not obsolete.
    write_unlock(parent);
    *old_val_addr = nullptr;
    return true;
  }

RECURSE_SEARCH:;

  // Find a child to recurse to
  art_node **child = find_child(n, key[depth]);

  if (!check_or_restart(n, version)) return false;

  if (!child) {
    bool full = node_full(n);
    if (full) {
      if (!upgrade_to_write_lock_or_restart(parent, parent_version))
        return false;
      if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;
    } else {
      if (!upgrade_to_write_lock_or_restart(n, version)) return false;
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l;
    add_child(tree, n, ref, key[depth], SET_LEAF(l), epoch, pool, gc_queue);

    if (full) {
      // NOTE(Alec) n is obsolete and n is already freed in add_child.
      write_unlock_obsolete(n);
      write_unlock(parent);
    } else {
      write_unlock(n);
    }
    *old_val_addr = nullptr;
    return true;
  }

  if (!read_unlock_or_restart(parent, parent_version)) return false;

  art_node *childptr = __sync_val_compare_and_swap(child, 0, 0);
  // If we are at a leaf, we need to replace it with a node
  if (IS_LEAF(childptr)) {
    // NOTE(Alec): we only update the current node,
    // because we only have two leaves in the new NODE4 without updating ref.
    if (!upgrade_to_write_lock_or_restart(n, version)) return false;
    art_leaf *leaf = LEAF_RAW(childptr);

    ++depth;
    // Check if we are updating an existing value
    if (!leaf_matches(leaf, key, key_len, depth)) {
      *old = 1;
      void *old_val = leaf->value;
      if (replace) leaf->value = value;
      if (new_leaf) *new_leaf = leaf;

      write_unlock(n);
      *old_val_addr = old_val;
      return true;
    }

    // New value, we must split the leaf into a node4
    art_node4 *new_node = pool->get_node4(&(tree->mem_size));

    // Create a new leaf
    art_leaf *l2 = make_leaf(tree, key, key_len, value);
    if (new_leaf) *new_leaf = l2;

    /* NOTE(Alec): if the new leaf shares the prefix with the
       old leaf but has shorter length, a new child with '\0'
       will be created.
    */

    // Determine longest prefix
    int longest_prefix = longest_common_prefix(leaf, l2, depth);
    new_node->n.partial_len = longest_prefix;
    memcpy(new_node->n.partial, key + depth,
           min(MAX_PREFIX_LEN, longest_prefix));

    // Add the leafs to the new node4
    __sync_lock_test_and_set(child, (art_node *)new_node);
    add_child4(tree, new_node, child, leaf->key[depth + longest_prefix],
               SET_LEAF(leaf), epoch, pool, gc_queue);
    add_child4(tree, new_node, child, l2->key[depth + longest_prefix],
               SET_LEAF(l2), epoch, pool, gc_queue);

    write_unlock(n);
    *old_val_addr = nullptr;
    return true;
  }

  parent = n;
  parent_version = version;
  n = childptr;
  ref = child;
  ++depth;
  goto AGAIN;
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
  int old_val = 0;
  void *old;
  while (!recursive_insert(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, value, 0,
                           &old_val, 1, nullptr, &old)) {
  }
  if (!old_val) t->size++;
  return old;
}

void *art_insert_get_leaf(art_tree *t, const unsigned char *key, int key_len,
                          void *value, art_leaf **new_leaf) {
  int old_val = 0;
  void *old;
  while (!recursive_insert(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, value, 0,
                           &old_val, 1, new_leaf, &old)) {
  }
  if (!old_val) t->size++;
  return old;
}

/************************* GC Version *************************/
void *art_insert(art_tree *t, const unsigned char *key, int key_len,
                 void *value,
                 /* NOTE(Alec): for garbage collection and reutilization */
                 uint64_t epoch, ARTObjectPool *pool,
                 moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int old_val = 0;
  void *old;
  while (!recursive_insert(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, value, 0,
                           &old_val, 1, nullptr, &old, epoch, pool, gc_queue)) {
  }
  if (!old_val) t->size++;
  return old;
}

/************************* GC Version *************************/
void *art_insert_get_leaf(
    art_tree *t, const unsigned char *key, int key_len, void *value,
    art_leaf **new_leaf,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int old_val = 0;
  void *old;
  while (!recursive_insert(
      t, __sync_val_compare_and_swap(&t->root, 0, 0), &t->root, nullptr, 0, key,
      key_len, value, 0, &old_val, 1, new_leaf, &old, epoch, pool, gc_queue)) {
  }
  if (!old_val) t->size++;
  return old;
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
  int old_val = 0;
  void *old;
  while (!recursive_insert(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, value, 0,
                           &old_val, 0, nullptr, &old)) {
  }
  if (!old_val) t->size++;
  return old;
}

void *art_insert_no_replace_get_leaf(art_tree *t, const unsigned char *key,
                                     int key_len, void *value,
                                     art_leaf **new_leaf) {
  int old_val = 0;
  void *old;
  while (!recursive_insert(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, value, 0,
                           &old_val, 0, new_leaf, &old)) {
  }
  if (!old_val) t->size++;
  return old;
}

/************************* GC Version *************************/
void *art_insert_no_replace(
    art_tree *t, const unsigned char *key, int key_len, void *value,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int old_val = 0;
  void *old;
  while (!recursive_insert(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, value, 0,
                           &old_val, 0, nullptr, &old, epoch, pool, gc_queue)) {
  }
  if (!old_val) t->size++;
  return old;
}

/************************* GC Version *************************/
void *art_insert_no_replace_get_leaf(
    art_tree *t, const unsigned char *key, int key_len, void *value,
    art_leaf **new_leaf,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int old_val = 0;
  void *old;
  while (!recursive_insert(
      t, __sync_val_compare_and_swap(&t->root, 0, 0), &t->root, nullptr, 0, key,
      key_len, value, 0, &old_val, 0, new_leaf, &old, epoch, pool, gc_queue)) {
  }
  if (!old_val) t->size++;
  return old;
}

static void remove_child256(art_tree *t, art_node256 *n, art_node **ref,
                            unsigned char c) {
  n->children[c] = NULL;
  n->n.num_children--;

  // Resize to a node48 on underflow, not immediately to prevent
  // trashing if we sit on the 48/49 boundary
  if (n->n.num_children == 37) {
    art_node48 *new_node = (art_node48 *)alloc_node(t, NODE48);
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    copy_header((art_node *)new_node, (art_node *)n);

    int pos = 0;
    for (int i = 0; i < 256; i++) {
      if (n->children[i]) {
        new_node->children[pos] = n->children[i];
        new_node->keys[i] = pos + 1;
        pos++;
      }
    }
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node256));
  }
}

static void remove_child256(
    art_tree *t, art_node256 *n, art_node **ref, unsigned char c,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  n->children[c] = NULL;
  n->n.num_children--;

  // Resize to a node48 on underflow, not immediately to prevent
  // trashing if we sit on the 48/49 boundary
  if (n->n.num_children == 37) {
    art_node48 *new_node = pool->get_node48(&(t->mem_size));
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    copy_header((art_node *)new_node, (art_node *)n);

    int pos = 0;
    for (int i = 0; i < 256; i++) {
      if (n->children[i]) {
        new_node->children[pos] = n->children[i];
        new_node->keys[i] = pos + 1;
        pos++;
      }
    }
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE256));
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node256));
  }
}

static void remove_child48(art_tree *t, art_node48 *n, art_node **ref,
                           unsigned char c) {
  int pos = n->keys[c];
  n->keys[c] = 0;
  n->children[pos - 1] = NULL;
  n->n.num_children--;

  if (n->n.num_children == 12) {
    art_node16 *new_node = (art_node16 *)alloc_node(t, NODE16);
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    copy_header((art_node *)new_node, (art_node *)n);

    int child = 0;
    for (int i = 0; i < 256; i++) {
      pos = n->keys[i];
      if (pos) {
        new_node->keys[child] = i;
        new_node->children[child] = n->children[pos - 1];
        child++;
      }
    }
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node48));
  }
}

/************************* GC Version *************************/
static void remove_child48(
    art_tree *t, art_node48 *n, art_node **ref, unsigned char c,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int pos = n->keys[c];
  n->keys[c] = 0;
  n->children[pos - 1] = NULL;
  n->n.num_children--;

  if (n->n.num_children == 12) {
    art_node16 *new_node = pool->get_node16(&(t->mem_size));
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    copy_header((art_node *)new_node, (art_node *)n);

    int child = 0;
    for (int i = 0; i < 256; i++) {
      pos = n->keys[i];
      if (pos) {
        new_node->keys[child] = i;
        new_node->children[child] = n->children[pos - 1];
        child++;
      }
    }
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE48));
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node48));
  }
}

static void remove_child16(art_tree *t, art_node16 *n, art_node **ref,
                           art_node **l) {
  int pos = l - n->children;
  memmove(n->keys + pos, n->keys + pos + 1, n->n.num_children - 1 - pos);
  memmove(n->children + pos, n->children + pos + 1,
          (n->n.num_children - 1 - pos) * sizeof(void *));
  n->n.num_children--;

  if (n->n.num_children == 3) {
    art_node4 *new_node = (art_node4 *)alloc_node(t, NODE4);
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    copy_header((art_node *)new_node, (art_node *)n);
    memcpy(new_node->keys, n->keys, 4);
    memcpy(new_node->children, n->children, 4 * sizeof(void *));
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node16));
  }
}

/************************* GC Version *************************/
static void remove_child16(
    art_tree *t, art_node16 *n, art_node **ref, art_node **l,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int pos = l - n->children;
  memmove(n->keys + pos, n->keys + pos + 1, n->n.num_children - 1 - pos);
  memmove(n->children + pos, n->children + pos + 1,
          (n->n.num_children - 1 - pos) * sizeof(void *));
  n->n.num_children--;

  if (n->n.num_children == 3) {
    art_node4 *new_node = pool->get_node4(&(t->mem_size));
    __sync_lock_test_and_set(ref, (art_node *)new_node);
    copy_header((art_node *)new_node, (art_node *)n);
    memcpy(new_node->keys, n->keys, 4);
    memcpy(new_node->children, n->children, 4 * sizeof(void *));
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE16));
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node16));
  }
}

static void remove_child4(art_tree *t, art_node4 *n, art_node **ref,
                          art_node **l) {
  int pos = l - n->children;
  memmove(n->keys + pos, n->keys + pos + 1, n->n.num_children - 1 - pos);
  memmove(n->children + pos, n->children + pos + 1,
          (n->n.num_children - 1 - pos) * sizeof(void *));
  n->n.num_children--;

  // Remove nodes with only a single child
  if (n->n.num_children == 1 && (&t->root) != ref) {
    art_node *child = n->children[0];
    if (!IS_LEAF(child)) {
      // Concatenate the prefixes
      int prefix = n->n.partial_len;
      if (prefix < MAX_PREFIX_LEN) {
        n->n.partial[prefix] = n->keys[0];
        prefix++;
      }
      if (prefix < MAX_PREFIX_LEN) {
        int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
        memcpy(n->n.partial + prefix, child->partial, sub_prefix);
        prefix += sub_prefix;
      }

      // Store the prefix in the child
      memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
      child->partial_len += n->n.partial_len + 1;
    }
    __sync_lock_test_and_set(ref, child);
    // free(n);
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node4));
  }
}

/************************* GC Version *************************/
static void remove_child4(
    art_tree *t, art_node4 *n, art_node **ref, art_node **l,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  int pos = l - n->children;
  memmove(n->keys + pos, n->keys + pos + 1, n->n.num_children - 1 - pos);
  memmove(n->children + pos, n->children + pos + 1,
          (n->n.num_children - 1 - pos) * sizeof(void *));
  n->n.num_children--;

  // Remove nodes with only a single child
  if (n->n.num_children == 1 && (&t->root) != ref) {
    art_node *child = n->children[0];
    if (!IS_LEAF(child)) {
      // Concatenate the prefixes
      int prefix = n->n.partial_len;
      if (prefix < MAX_PREFIX_LEN) {
        n->n.partial[prefix] = n->keys[0];
        prefix++;
      }
      if (prefix < MAX_PREFIX_LEN) {
        int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
        memcpy(n->n.partial + prefix, child->partial, sub_prefix);
        prefix += sub_prefix;
      }

      // Store the prefix in the child
      memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
      child->partial_len += n->n.partial_len + 1;
    }
    __sync_lock_test_and_set(ref, child);
    // free(n);
    gc_queue->enqueue(
        tsdb::mem::GCRequest(epoch, (void *)(n), tsdb::mem::ART_NODE4));
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_node4));
  }
}

static void remove_child(art_tree *t, art_node *n, art_node **ref,
                         unsigned char c, art_node **l) {
  switch (n->type) {
    case NODE4:
      return remove_child4(t, (art_node4 *)n, ref, l);
    case NODE16:
      return remove_child16(t, (art_node16 *)n, ref, l);
    case NODE48:
      return remove_child48(t, (art_node48 *)n, ref, c);
    case NODE256:
      return remove_child256(t, (art_node256 *)n, ref, c);
    default:
      printf("abort in remove_child()\n");
      abort();
  }
}

/************************* GC Version *************************/
static void remove_child(
    art_tree *t, art_node *n, art_node **ref, unsigned char c, art_node **l,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  switch (n->type) {
    case NODE4:
      return remove_child4(t, (art_node4 *)n, ref, l, epoch, gc_queue);
    case NODE16:
      return remove_child16(t, (art_node16 *)n, ref, l, epoch, pool, gc_queue);
    case NODE48:
      return remove_child48(t, (art_node48 *)n, ref, c, epoch, pool, gc_queue);
    case NODE256:
      return remove_child256(t, (art_node256 *)n, ref, c, epoch, pool,
                             gc_queue);
    default:
      printf("abort in remove_child()\n");
      abort();
  }
}

bool need_compression(art_tree *t, art_node *n, art_node **ref) {
  switch (n->type) {
    case NODE4:
      return n->num_children == 2 && (&t->root) != ref;
    case NODE16:
      return n->num_children == 4;
    case NODE48:
      return n->num_children == 13;
    case NODE256:
      return n->num_children == 38;
    default:
      printf("abort in need_compression()\n");
      abort();
  }
}

static bool recursive_delete(art_tree *t, art_node *n, art_node **ref,
                             art_node *parent, uint32_t parent_version,
                             const unsigned char *key, int key_len, int depth,
                             art_leaf **leaf_addr) {
  uint32_t version;

AGAIN:
  if (!read_lock_or_restart(n, &version)) return false;
  if (!read_unlock_or_restart(parent, parent_version)) return false;

  // Bail if the prefix does not match
  if (n->partial_len) {
    int prefix_len = check_prefix(n, key, key_len, depth);
    if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
      if (!read_unlock_or_restart(n, version)) return false;
      *leaf_addr = nullptr;
      return true;
    }
    depth = depth + n->partial_len;
  }

  // Find child node
  art_node **child = find_child(n, key[depth]);
  if (!check_or_restart(n, version)) return false;
  if (!child) {
    if (!read_unlock_or_restart(n, version)) return false;
    *leaf_addr = nullptr;
    return true;
  }

  art_node *childptr = __sync_val_compare_and_swap(child, 0, 0);
  // If the child is leaf, delete from this node
  if (IS_LEAF(childptr)) {
    art_leaf *l = LEAF_RAW(childptr);
    if (!leaf_matches(l, key, key_len, depth)) {
      bool compress = need_compression(t, n, ref);
      if (compress) {
        if (!upgrade_to_write_lock_or_restart(parent, parent_version))
          return false;
        if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;
      } else if (!upgrade_to_write_lock_or_restart(n, version))
        return false;

      remove_child(t, n, ref, key[depth], child);

      if (compress) {
        write_unlock_obsolete(n);  // n is obsolete.
        write_unlock(parent);
      } else
        write_unlock(n);
      __sync_lock_test_and_set(leaf_addr, l);
      return true;
    }
    if (!read_unlock_or_restart(n, version)) return false;
    *leaf_addr = nullptr;
    return true;
  }

  ++depth;
  parent = n;
  parent_version = version;
  ref = child;
  n = childptr;
  goto AGAIN;
}

/************************* GC Version *************************/
static bool recursive_delete(
    art_tree *t, art_node *n, art_node **ref, art_node *parent,
    uint32_t parent_version, const unsigned char *key, int key_len, int depth,
    art_leaf **leaf_addr,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool *pool,
    moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  uint32_t version;

AGAIN:
  if (!read_lock_or_restart(n, &version)) return false;
  if (!read_unlock_or_restart(parent, parent_version)) return false;

  // Bail if the prefix does not match
  if (n->partial_len) {
    int prefix_len = check_prefix(n, key, key_len, depth);
    if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
      if (!read_unlock_or_restart(n, version)) return false;
      *leaf_addr = nullptr;
      return true;
    }
    depth = depth + n->partial_len;
  }

  // Find child node
  art_node **child = find_child(n, key[depth]);
  if (!check_or_restart(n, version)) return false;
  if (!child) {
    if (!read_unlock_or_restart(n, version)) return false;
    *leaf_addr = nullptr;
    return true;
  }

  art_node *childptr = __sync_val_compare_and_swap(child, 0, 0);
  // If the child is leaf, delete from this node
  if (IS_LEAF(childptr)) {
    art_leaf *l = LEAF_RAW(childptr);
    if (!leaf_matches(l, key, key_len, depth)) {
      bool compress = need_compression(t, n, ref);
      if (compress) {
        if (!upgrade_to_write_lock_or_restart(parent, parent_version))
          return false;
        if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;
      } else if (!upgrade_to_write_lock_or_restart(n, version))
        return false;

      remove_child(t, n, ref, key[depth], child, epoch, pool, gc_queue);

      if (compress) {
        write_unlock_obsolete(n);  // n is obsolete.
        write_unlock(parent);
      } else
        write_unlock(n);
      __sync_lock_test_and_set(leaf_addr, l);
      return true;
    }
    if (!read_unlock_or_restart(n, version)) return false;
    *leaf_addr = nullptr;
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
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void *art_delete(art_tree *t, const unsigned char *key, int key_len) {
  art_leaf *l;
  while (!recursive_delete(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, 0, &l)) {
  }
  if (l) {
    t->size--;
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_leaf) - key_len);
    return l->value;
  }
  return nullptr;
}

/************************* GC Version *************************/
void *art_delete(art_tree *t, const unsigned char *key, int key_len,
                 /* NOTE(Alec): for garbage collection and reutilization */
                 uint64_t epoch, ARTObjectPool *pool,
                 moodycamel::ConcurrentQueue<tsdb::mem::GCRequest> *gc_queue) {
  art_leaf *l;
  while (!recursive_delete(t, __sync_val_compare_and_swap(&t->root, 0, 0),
                           &t->root, nullptr, 0, key, key_len, 0, &l, epoch,
                           pool, gc_queue)) {
  }
  if (l) {
    t->size--;
    __sync_fetch_and_add(&(t->mem_size), -sizeof(art_leaf) - key_len);
    return l->value;
  }
  return nullptr;
}

// Recursively iterates over the tree
static bool recursive_iter(art_node *n, art_callback cb, void *data, int *res) {
  // Handle base cases
  if (!n) return 0;

  uint32_t version;
  if (!read_lock_or_restart(n, &version)) {
    *res = 0;
    return false;
  }

  if (IS_LEAF(n)) {
    art_leaf *l = LEAF_RAW(n);
    return cb(data, (const unsigned char *)l->key, l->key_len, l->value);
  }

  int idx;
  art_node *child;
  switch (n->type) {
    case NODE4:
      for (int i = 0; i < n->num_children; i++) {
        child =
            __sync_val_compare_and_swap(&(((art_node4 *)n)->children[i]), 0, 0);
        if (!read_unlock_or_restart(n, version)) return false;
        return recursive_iter(child, cb, data, res);
      }
      break;

    case NODE16:
      for (int i = 0; i < n->num_children; i++) {
        child = __sync_val_compare_and_swap(&(((art_node16 *)n)->children[i]),
                                            0, 0);
        if (!read_unlock_or_restart(n, version)) return false;
        return recursive_iter(child, cb, data, res);
      }
      break;

    case NODE48:
      for (int i = 0; i < 256; i++) {
        idx = ((art_node48 *)n)->keys[i];
        if (!idx) continue;

        child = __sync_val_compare_and_swap(
            &(((art_node48 *)n)->children[idx - 1]), 0, 0);
        if (!read_unlock_or_restart(n, version)) return false;
        return recursive_iter(child, cb, data, res);
      }
      break;

    case NODE256:
      for (int i = 0; i < 256; i++) {
        child = __sync_val_compare_and_swap(&(((art_node256 *)n)->children[i]),
                                            0, 0);
        if (!read_unlock_or_restart(n, version)) return false;
        if (!child) continue;
        return recursive_iter(child, cb, data, res);
      }
      break;

    default:
      printf("abort in recursive_iter()\n");
      abort();
  }
  if (!read_unlock_or_restart(n, version)) {
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
    if (recursive_iter(__sync_val_compare_and_swap(&t->root, 0, 0), cb, data,
                       &res))
      break;
    clean_cb(data);
  }
  return res;
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(const art_leaf *n, const unsigned char *prefix,
                               int prefix_len) {
  // Fail if the key length is too short
  if (n->key_len < (uint32_t)prefix_len) return 1;

  // Compare the keys
  return memcmp(n->key, prefix, prefix_len);
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
//     art_node **child;
//     art_node *n = t->root;
//     int prefix_len, depth = 0;
//     while (n) {
//         // Might be a leaf
//         if (IS_LEAF(n)) {
//             n = (art_node*)LEAF_RAW(n);
//             // Check if the expanded path matches
//             if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
//                 art_leaf *l = (art_leaf*)n;
//                 return cb(data, (const unsigned char*)l->key, l->key_len,
//                 l->value);
//             }
//             return 0;
//         }

//         // If the depth matches the prefix, we need to handle this node
//         if (depth == key_len) {
//             art_leaf *l = minimum(n);
//             if (!leaf_prefix_matches(l, key, key_len))
//                return recursive_iter(n, cb, data);
//             return 0;
//         }

//         // Bail if the prefix does not match
//         if (n->partial_len) {
//             prefix_len = prefix_mismatch(n, key, key_len, depth);

//             // Guard if the mis-match is longer than the MAX_PREFIX_LEN
//             if ((uint32_t)prefix_len > n->partial_len) {
//                 prefix_len = n->partial_len;
//             }

//             // If there is no match, search is terminated
//             if (!prefix_len) {
//                 return 0;

//             // If we've matched the prefix, iterate on this node
//             } else if (depth + prefix_len == key_len) {
//                 return recursive_iter(n, cb, data);
//             }

//             // if there is a full match, go deeper
//             depth = depth + n->partial_len;
//         }

//         // Recursively search
//         child = find_child(n, key[depth]);
//         n = (child) ? *child : NULL;
//         depth++;
//     }
//     return 0;
// }

/**************************** Node Pool *******************************/
ARTObjectPool::~ARTObjectPool() {
  art_node4 *n4;
  while (node4_queue_.try_dequeue(n4)) free(n4);
  art_node16 *n16;
  while (node16_queue_.try_dequeue(n16)) free(n16);
  art_node48 *n48;
  while (node48_queue_.try_dequeue(n48)) free(n48);
  art_node256 *n256;
  while (node256_queue_.try_dequeue(n256)) free(n256);
}

void ARTObjectPool::put_node4(art_node4 *n) {
  memset(n, 0, sizeof(art_node4));
  node4_queue_.enqueue(n);
}
art_node4 *ARTObjectPool::get_node4() {
  art_node4 *n;
  if (!node4_queue_.try_dequeue(n)) n = (art_node4 *)alloc_node(NODE4);
  n->n.type = NODE4;
  return n;
}
art_node4 *ARTObjectPool::get_node4(uint64_t *size) {
  art_node4 *n;
  if (!node4_queue_.try_dequeue(n)) n = (art_node4 *)alloc_node(NODE4);
  n->n.type = NODE4;
  __sync_fetch_and_add(size, sizeof(art_node4));
  return n;
}

void ARTObjectPool::put_node16(art_node16 *n) {
  memset(n, 0, sizeof(art_node16));
  node16_queue_.enqueue(n);
}
art_node16 *ARTObjectPool::get_node16() {
  art_node16 *n;
  if (!node16_queue_.try_dequeue(n)) n = (art_node16 *)alloc_node(NODE16);
  n->n.type = NODE16;
  return n;
}
art_node16 *ARTObjectPool::get_node16(uint64_t *size) {
  art_node16 *n;
  if (!node16_queue_.try_dequeue(n)) n = (art_node16 *)alloc_node(NODE16);
  n->n.type = NODE16;
  __sync_fetch_and_add(size, sizeof(art_node16));
  return n;
}

void ARTObjectPool::put_node48(art_node48 *n) {
  memset(n, 0, sizeof(art_node48));
  node48_queue_.enqueue(n);
}
art_node48 *ARTObjectPool::get_node48() {
  art_node48 *n;
  if (!node48_queue_.try_dequeue(n)) n = (art_node48 *)alloc_node(NODE48);
  n->n.type = NODE48;
  return n;
}
art_node48 *ARTObjectPool::get_node48(uint64_t *size) {
  art_node48 *n;
  if (!node48_queue_.try_dequeue(n)) n = (art_node48 *)alloc_node(NODE48);
  n->n.type = NODE48;
  __sync_fetch_and_add(size, sizeof(art_node48));
  return n;
}

void ARTObjectPool::put_node256(art_node256 *n) {
  memset(n, 0, sizeof(art_node256));
  node256_queue_.enqueue(n);
}
art_node256 *ARTObjectPool::get_node256() {
  art_node256 *n;
  if (!node256_queue_.try_dequeue(n)) n = (art_node256 *)alloc_node(NODE256);
  n->n.type = NODE256;
  return n;
}
art_node256 *ARTObjectPool::get_node256(uint64_t *size) {
  art_node256 *n;
  if (!node256_queue_.try_dequeue(n)) n = (art_node256 *)alloc_node(NODE256);
  n->n.type = NODE256;
  __sync_fetch_and_add(size, sizeof(art_node256));
  return n;
}

art_node *ARTObjectPool::get_node(uint8_t type) {
  art_node *n;
  switch (type) {
    case NODE4:
      art_node4 *tmp1;
      if (!node4_queue_.try_dequeue(tmp1))
        n = (art_node *)calloc(1, sizeof(art_node4));
      else
        n = (art_node *)(tmp1);
      break;
    case NODE16:
      art_node16 *tmp2;
      if (!node16_queue_.try_dequeue(tmp2))
        n = (art_node *)calloc(1, sizeof(art_node16));
      else
        n = (art_node *)(tmp2);
      break;
    case NODE48:
      art_node48 *tmp3;
      if (!node48_queue_.try_dequeue(tmp3))
        n = (art_node *)calloc(1, sizeof(art_node48));
      else
        n = (art_node *)(tmp3);
      break;
    case NODE256:
      art_node256 *tmp4;
      if (!node256_queue_.try_dequeue(tmp4))
        n = (art_node *)calloc(1, sizeof(art_node256));
      else
        n = (art_node *)(tmp4);
      break;
    default:
      printf("abort in get_node()\n");
      abort();
  }
  n->type = type;
  return n;
}

void ARTObjectPool::put_node(art_node *n) {
  switch (n->type) {
    case NODE4:
      node4_queue_.enqueue((art_node4 *)(n));
      break;
    case NODE16:
      node16_queue_.enqueue((art_node16 *)(n));
      break;
    case NODE48:
      node48_queue_.enqueue((art_node48 *)(n));
      break;
    case NODE256:
      node256_queue_.enqueue((art_node256 *)(n));
      break;
    default:
      printf("abort in put_node()\n");
      abort();
  }
}

}  // namespace artoptlockepoch