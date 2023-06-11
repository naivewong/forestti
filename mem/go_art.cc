#include "mem/go_art.h"
#include "mem/mmap.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <functional>
#include <utility>

namespace tsdb {
namespace mem {

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
art_node* alloc_node(art_tree* tree, uint8_t type) {
  art_node* n;
  switch (type) {
#ifdef MMAP_ART_NODE
    case NODE4:
      n = (art_node*)(mmap_node4->alloc());
      tree->mem_size.fetch_add(sizeof(art_node4));
      break;
    case NODE16:
      n = (art_node*)(mmap_node16->alloc());
      tree->mem_size.fetch_add(sizeof(art_node16));
      break;
    case NODE48:
      n = (art_node*)(mmap_node48->alloc());
      ((art_node48*)(n))->slots = NODE48_EMPTY_SLOTS;
      tree->mem_size.fetch_add(sizeof(art_node48));
      break;
    case NODE256:
      n = (art_node*)(mmap_node256->alloc());
      tree->mem_size.fetch_add(sizeof(art_node256));
      break;
#else
    case NODE4:
      n = (art_node*)calloc(1, sizeof(art_node4));
      tree->mem_size.fetch_add(sizeof(art_node4));
      // __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node4));
      break;
    case NODE16:
      n = (art_node*)calloc(1, sizeof(art_node16));
      tree->mem_size.fetch_add(sizeof(art_node16));
      // __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node16));
      break;
    case NODE48:
      n = (art_node*)calloc(1, sizeof(art_node48));
      ((art_node48*)(n))->slots = NODE48_EMPTY_SLOTS;
      tree->mem_size.fetch_add(sizeof(art_node48));
      // __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node48));
      break;
    case NODE256:
      n = (art_node*)calloc(1, sizeof(art_node256));
      tree->mem_size.fetch_add(sizeof(art_node256));
      // __sync_fetch_and_add(&(tree->mem_size), sizeof(art_node256));
      break;
#endif
    default:
      printf("abort in alloc_node()\n");
      abort();
  }
  n->version = 0;
  n->type = type;
  n->prefix_leaf = NULL;
  return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree* t) {
  t->root = NULL;
  t->size = 0;
  t->mem_size = 0;
  t->root = alloc_node(t, NODE4);
  return 0;
}

// Recursively destroys the tree
void destroy_node(art_node* n) {
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
    art_node4* p1;
    art_node16* p2;
    art_node48* p3;
    art_node256* p4;
  } p;
  switch (n->type) {
    case NODE4:
      p.p1 = (art_node4*)n;
      for (i = 0; i < n->num_children; i++) {
        destroy_node(p.p1->children[i]);
      }
      break;

    case NODE16:
      p.p2 = (art_node16*)n;
      for (i = 0; i < n->num_children; i++) {
        destroy_node(p.p2->children[i]);
      }
      break;

    case NODE48:
      p.p3 = (art_node48*)n;
      for (i = 0; i < 256; i++) {
        idx = ((art_node48*)n)->keys[i];
        if (!idx) continue;
        destroy_node(p.p3->children[idx - 1]);
      }
      break;

    case NODE256:
      p.p4 = (art_node256*)n;
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
      mmap_node4->reclaim(n);
      break;

    case NODE16:
      mmap_node16->reclaim(n);
      break;

    case NODE48:
      mmap_node48->reclaim(n);
      break;

    case NODE256:
      mmap_node256->reclaim(n);
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
int art_tree_destroy(art_tree* t) {
  destroy_node(t->root);
  return 0;
}

uint64_t art_mem_size(art_tree* t) { return t->mem_size.load(); }

bool art_empty(art_tree* t) {
  return __sync_val_compare_and_swap(&t->root, 0, 0)->num_children == 0;
}

int min(int a, int b) { return (a < b) ? a : b; }

art_leaf* make_leaf(art_tree* t, const unsigned char* key, int key_len,
                    void* value) {
  art_leaf* l = (art_leaf*)calloc(1, sizeof(art_leaf) + key_len);
  // __sync_fetch_and_add(&(t->mem_size), sizeof(art_leaf)+key_len);
  t->mem_size.fetch_add(sizeof(art_leaf) + key_len);
  l->value = value;
  l->key_len = key_len;
  memcpy(l->key, key, key_len);
  return l;
}

void copy_header(art_node* dest, art_node* src) {
  dest->num_children = src->num_children;
  dest->prefix_len = src->prefix_len;
  memcpy(dest->prefix, src->prefix, min(MAX_PREFIX_LEN, src->prefix_len));
  dest->prefix_leaf.store(src->prefix_leaf);
}

void add_child256(art_tree* tree, art_node256* n, unsigned char c,
                  void* child) {
  n->n.num_children++;
  n->children[c] = (art_node*)child;
  // __sync_lock_test_and_set(&n->children[c], (art_node*)child);
}

void add_child48(art_tree* tree, art_node48* n, unsigned char c, void* child) {
  int pos = n->alloc_slot();
  n->children[pos] = (art_node*)child;
  // __sync_lock_test_and_set(&n->children[pos], (art_node*)child);
  n->keys[c] = pos + 1;
  n->n.num_children++;
}

void add_child16(art_tree* tree, art_node16* n, unsigned char c, void* child) {
  unsigned mask = (1 << n->n.num_children) - 1;

  // Compare the key to all 16 stored keys
  unsigned bitfield = 0;
  for (short i = 0; i < 16; ++i) {
    if (c < n->keys[i]) bitfield |= (1 << i);
  }

  // Use a mask to ignore children that don't exist
  bitfield &= mask;

  // Check if less than any
  unsigned idx;
  if (bitfield) {
    idx = __builtin_ctz(bitfield);
    memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
    memmove(n->children + idx + 1, n->children + idx,
            (n->n.num_children - idx) * sizeof(void*));
  } else
    idx = n->n.num_children;

  // Set the child
  n->keys[idx] = c;
  n->children[idx] = (art_node*)child;
  // __sync_lock_test_and_set(&n->children[idx], (art_node*)child);
  n->n.num_children++;
}

void add_child4(art_tree* tree, art_node4* n, unsigned char c, void* child) {
  int idx;
  for (idx = 0; idx < n->n.num_children; idx++) {
    if (c < n->keys[idx]) break;
  }

  // Shift to make room
  memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
  memmove(n->children + idx + 1, n->children + idx,
          (n->n.num_children - idx) * sizeof(void*));

  // Insert element
  n->keys[idx] = c;
  n->children[idx] = (art_node*)child;
  // __sync_lock_test_and_set(&n->children[idx], (art_node*)child);
  n->n.num_children++;
}

// return true if the node need reclamation.
void add_child(art_tree* tree, art_node* n, unsigned char c, void* child) {
  switch (n->type) {
    case NODE4:
      add_child4(tree, (art_node4*)n, c, child);
      break;
    case NODE16:
      add_child16(tree, (art_node16*)n, c, child);
      break;
    case NODE48:
      add_child48(tree, (art_node48*)n, c, child);
      break;
    case NODE256:
      add_child256(tree, (art_node256*)n, c, child);
      break;
    default:
      printf("abort in add_child()\n");
      abort();
  }
}

void grow_and_insert256(art_tree* tree, art_node256* n, unsigned char c,
                        void* child, art_node** node_loc) {
  (void)node_loc;
  n->n.num_children++;
  n->children[c] = (art_node*)child;
  // __sync_lock_test_and_set(&n->children[c], (art_node*)child);
}

void grow_and_insert48(art_tree* tree, art_node48* n, unsigned char c,
                       void* child, art_node** node_loc) {
  art_node256* new_node = (art_node256*)alloc_node(tree, NODE256);
  for (int i = 0; i < 256; i++) {
    if (n->keys[i]) {
      new_node->children[i] = n->children[n->keys[i] - 1];
    }
  }
  copy_header((art_node*)new_node, (art_node*)n);
  add_child256(tree, new_node, c, child);
  // TODO: GC.
  // __sync_lock_test_and_set(node_loc, (art_node*)new_node);
  *node_loc = (art_node*)new_node;
  tree->mem_size.fetch_sub(sizeof(art_node48));
}

void grow_and_insert16(art_tree* tree, art_node16* n, unsigned char c,
                       void* child, art_node** node_loc) {
  art_node48* new_node = (art_node48*)alloc_node(tree, NODE48);
  new_node->slots = NODE48_GROW_SLOTS;

  // Copy the child pointers and populate the key map
  memcpy(new_node->children, n->children, sizeof(void*) * n->n.num_children);
  for (int i = 0; i < n->n.num_children; i++) {
    new_node->keys[n->keys[i]] = i + 1;
  }
  copy_header((art_node*)new_node, (art_node*)n);
  add_child48(tree, new_node, c, child);
  // TODO: GC.
  // __sync_lock_test_and_set(node_loc, (art_node*)new_node);
  *node_loc = (art_node*)new_node;
  tree->mem_size.fetch_sub(sizeof(art_node16));
}

void grow_and_insert4(art_tree* tree, art_node4* n, unsigned char c,
                      void* child, art_node** node_loc) {
  art_node16* new_node = (art_node16*)alloc_node(tree, NODE16);

  // Copy the child pointers and the key map
  memcpy(new_node->children, n->children, sizeof(void*) * n->n.num_children);
  memcpy(new_node->keys, n->keys, sizeof(unsigned char) * n->n.num_children);
  copy_header((art_node*)new_node, (art_node*)n);
  add_child16(tree, new_node, c, child);
  // TODO: GC.
  // __sync_lock_test_and_set(node_loc, (art_node*)new_node);
  *node_loc = (art_node*)new_node;
  tree->mem_size.fetch_sub(sizeof(art_node4));
}

void grow_and_insert(art_tree* tree, art_node* n, unsigned char c, void* child,
                     art_node** node_loc) {
  switch (n->type) {
    case NODE4:
      grow_and_insert4(tree, (art_node4*)n, c, child, node_loc);
      break;
    case NODE16:
      grow_and_insert16(tree, (art_node16*)n, c, child, node_loc);
      break;
    case NODE48:
      grow_and_insert48(tree, (art_node48*)n, c, child, node_loc);
      break;
    case NODE256:
      grow_and_insert256(tree, (art_node256*)n, c, child, node_loc);
      break;
    default:
      printf("abort in add_child()\n");
      abort();
  }
}

void remove_child(art_node* n, int i) {
  union {
    art_node4* p1;
    art_node16* p2;
    art_node48* p3;
    art_node256* p4;
  } p;
  switch (n->type) {
    case NODE4:
      p.p1 = (art_node4*)n;
      memmove(p.p1->keys + i, p.p1->keys + i + 1, p.p1->n.num_children - 1 - i);
      memmove(p.p1->children + i, p.p1->children + i + 1,
              (p.p1->n.num_children - 1 - i) * sizeof(void*));
      p.p1->n.num_children--;
      break;
    case NODE16:
      p.p2 = (art_node16*)n;
      memmove(p.p2->keys + i, p.p2->keys + i + 1, p.p2->n.num_children - 1 - i);
      memmove(p.p2->children + i, p.p2->children + i + 1,
              (p.p2->n.num_children - 1 - i) * sizeof(void*));
      p.p2->n.num_children--;
      break;
    case NODE48:
      p.p3 = (art_node48*)n;
      p.p3->children[p.p3->keys[i] - 1] = NULL;
      p.p3->free_slot(p.p3->keys[i] - 1);
      p.p3->keys[i] = 0;
      p.p3->n.num_children--;
      break;
    case NODE256:
      p.p4 = (art_node256*)n;
      p.p4->children[i] = NULL;
      p.p4->n.num_children--;
      break;
    default:
      printf("abort in remove_child()\n");
      abort();
  }
}

bool should_shrink(art_node* n, art_node* parent) {
  switch (n->type) {
    case NODE4:
      if (parent == NULL) return false;
      if (n->prefix_leaf.load() == NULL)
        return n->num_children <= 2;
      else
        return n->num_children <= 1;
    case NODE16:
      return n->num_children <= NODE16_MIN_SIZE;
    case NODE48:
      return n->num_children <= NODE48_MIN_SIZE;
    case NODE256:
      return n->num_children <= NODE256_MIN_SIZE;
    default:
      printf("abort in should_shrink()\n");
      abort();
  }
}

bool compress_child(art_tree* tree, art_node* n, int idx, art_node** node_loc) {
  art_node* child = ((art_node4*)(n))->children[idx];
  if (!IS_LEAF(child)) {
    if (!write_lock_or_restart(child)) return false;
    int prefix_len = n->prefix_len;
    if (prefix_len < MAX_PREFIX_LEN) {
      n->prefix[prefix_len] = ((art_node4*)(n))->keys[idx];
      prefix_len++;
    }
    if (prefix_len < MAX_PREFIX_LEN) {
      int sub_prefix_len = min(child->prefix_len, MAX_PREFIX_LEN - prefix_len);
      memmove(n->prefix + prefix_len, child->prefix, sub_prefix_len);
      prefix_len += sub_prefix_len;
    }

    memmove(child->prefix, n->prefix, min(prefix_len, MAX_PREFIX_LEN));
    child->prefix_len += n->prefix_len + 1;
    write_unlock(child);
  }
  // TODO: GC.
  tree->mem_size.fetch_sub(sizeof(art_node4));
  // __sync_lock_test_and_set(node_loc, child);
  *node_loc = child;
  return true;
}

// Actually removes node4 itself.
bool remove_child_and_shrink4(art_tree* tree, art_node4* n, unsigned char c,
                              art_node** node_loc) {
  if (n->n.prefix_leaf.load()) {
    // __sync_lock_test_and_set(node_loc, n->n.prefix_leaf.load());
    *node_loc = (art_node*)(n->n.prefix_leaf.load());
    return true;
  }

  for (int i = 0; i < n->n.num_children; i++) {
    if (n->keys[i] != c)
      return compress_child(tree, (art_node*)(n), i, node_loc);
  }

  printf("abort in remove_child_and_shrink4()\n");
  abort();
}

bool remove_child_and_shrink16(art_tree* tree, art_node16* n, unsigned char c,
                               art_node** node_loc) {
  art_node4* newnode = (art_node4*)alloc_node(tree, NODE4);
  copy_header((art_node*)newnode, (art_node*)n);
  newnode->n.num_children = NODE16_MIN_SIZE - 1;

  int idx = 0;
  for (int i = 0; i < n->n.num_children; i++) {
    if (n->keys[i] != c) {
      newnode->keys[idx] = n->keys[i];
      newnode->children[idx] = n->children[i];
      idx++;
    }
  }
  // TODO: GC.
  tree->mem_size.fetch_sub(sizeof(art_node16));
  // __sync_lock_test_and_set(node_loc, (art_node*)newnode);
  *node_loc = (art_node*)newnode;
  return true;
}

bool remove_child_and_shrink48(art_tree* tree, art_node48* n, unsigned char c,
                               art_node** node_loc) {
  art_node16* newnode = (art_node16*)alloc_node(tree, NODE16);
  copy_header((art_node*)newnode, (art_node*)n);
  newnode->n.num_children = NODE48_MIN_SIZE - 1;

  int child = 0, pos;
  for (int i = 0; i < 256; i++) {
    pos = n->keys[i];
    if (pos && i != c) {
      newnode->keys[child] = i;
      newnode->children[child] = n->children[pos - 1];
      child++;
    }
  }
  // TODO: GC.
  tree->mem_size.fetch_sub(sizeof(art_node48));
  // __sync_lock_test_and_set(node_loc, (art_node*)newnode);
  *node_loc = (art_node*)newnode;
  return true;
}

bool remove_child_and_shrink256(art_tree* tree, art_node256* n, unsigned char c,
                                art_node** node_loc) {
  art_node48* newnode = (art_node48*)alloc_node(tree, NODE48);
  copy_header((art_node*)newnode, (art_node*)n);
  newnode->n.num_children = NODE256_MIN_SIZE - 1;

  int pos = 0;
  for (int i = 0; i < 256; i++) {
    if (i != c && n->children[i]) {
      pos = newnode->alloc_slot();
      newnode->keys[i] = pos + 1;
      newnode->children[pos] = n->children[i];
    }
  }
  // TODO: GC.
  tree->mem_size.fetch_sub(sizeof(art_node256));
  // __sync_lock_test_and_set(node_loc, (art_node*)newnode);
  *node_loc = (art_node*)newnode;
  return true;
}

bool remove_child_and_shrink(art_tree* tree, art_node* n, unsigned char c,
                             art_node** node_loc) {
  switch (n->type) {
    case NODE4:
      return remove_child_and_shrink4(tree, (art_node4*)n, c, node_loc);
    case NODE16:
      return remove_child_and_shrink16(tree, (art_node16*)n, c, node_loc);
    case NODE48:
      return remove_child_and_shrink48(tree, (art_node48*)n, c, node_loc);
    case NODE256:
      return remove_child_and_shrink256(tree, (art_node256*)n, c, node_loc);
    default:
      printf("abort in remove_child_and_shrink()\n");
      abort();
  }
}

bool should_compress(art_node* n, art_node* parent) {
  if (n->type == NODE4) return n->num_children == 1 && parent != NULL;
  return false;
}

art_node* first_child(art_node* n) {
  if (n->type == NODE4) {
    return __sync_val_compare_and_swap(&(((art_node4*)(n))->children[0]), 0, 0);
  } else if (n->type == NODE16) {
    return __sync_val_compare_and_swap(&(((art_node16*)(n))->children[0]), 0,
                                       0);
  } else if (n->type == NODE48) {
    art_node48* n48 = (art_node48*)(n);
    for (int i = 0; i < 256; i++) {
      int pos = n48->keys[i];
      if (pos == 0) continue;
      return __sync_val_compare_and_swap(&n48->children[pos - 1], 0, 0);
    }
  } else if (n->type == NODE256) {
    art_node256* n256 = (art_node256*)(n);
    for (int i = 0; i < 256; i++) {
      art_node* c = __sync_val_compare_and_swap(&n256->children[i], 0, 0);
      if (c) return c;
    }
  } else {
    printf("abort in first_child()\n");
    abort();
  }
}

void update_prefix_leaf(art_tree* t, art_node* n, const unsigned char* key,
                        int key_len, void* value, void** value_addr) {
  art_leaf* l = LEAF_RAW(n->prefix_leaf.load());
  if (l == NULL) {
    if (value_addr) *value_addr = value;
    n->prefix_leaf.store(SET_LEAF(make_leaf(t, key, key_len, value)));
  } else {
    if (value_addr)
      *value_addr = l->value;
    else
      l->value = value;
  }
}

bool node_full(art_node* n) {
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
 * Returns the number of prefix characters shared between
 * the key and node.
 */
int check_prefix(const art_node* n, const unsigned char* key, int key_len,
                 int depth) {
  int max_cmp = min(min(n->prefix_len, MAX_PREFIX_LEN), key_len - depth);
  int idx;
  for (idx = 0; idx < max_cmp; idx++) {
    if (n->prefix[idx] != key[depth + idx]) return idx;
  }
  return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
int leaf_matches(const art_leaf* n, const unsigned char* key, int key_len) {
  // Fail if the key lengths are different
  if (n->key_len != (uint32_t)key_len) return 1;

  // Compare the keys starting at the depth
  return memcmp(n->key, key, key_len);
}

// Replace the leaf with node4.
void update_or_expand(art_tree* t, art_leaf* l, const unsigned char* key,
                      int key_len, void* value, int depth, art_node** node_loc,
                      void** value_addr) {
  if (leaf_matches(l, key, key_len) == 0) {
    if (value_addr)
      *value_addr = l->value;
    else
      l->value = value;
    return;
  }

  int i;
  int prefix_len = min(key_len, l->key_len);
  art_node* newnode = alloc_node(t, NODE4);

  for (i = depth; i < prefix_len; i++) {
    if (l->key[i] != key[i]) break;
  }
  newnode->prefix_len = i - depth;
  memmove(newnode->prefix, key + depth,
          min(newnode->prefix_len, MAX_PREFIX_LEN));

  if (i == l->key_len)
    newnode->prefix_leaf.store(SET_LEAF(l));
  else
    add_child(t, newnode, l->key[i], SET_LEAF(l));
  if (i == key_len)
    newnode->prefix_leaf.store(SET_LEAF(make_leaf(t, key, key_len, value)));
  else
    add_child(t, newnode, key[i], SET_LEAF(make_leaf(t, key, key_len, value)));
  // __sync_lock_test_and_set(node_loc, newnode);
  *node_loc = (art_node*)newnode;

  if (value_addr) *value_addr = value;
}

art_node** find_child(art_node* n, unsigned char c, art_node** nextnode,
                      int* idx) {
  int i;
  union {
    art_node4* p1;
    art_node16* p2;
    art_node48* p3;
    art_node256* p4;
  } p;
  switch (n->type) {
    case NODE4:
      p.p1 = (art_node4*)n;
      for (i = 0; i < n->num_children; i++) {
        /* this cast works around a bug in gcc 5.1 when unrolling loops
         * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
         */
        if (((unsigned char*)p.p1->keys)[i] == c) {
          *idx = i;
          *nextnode = p.p1->children[i];
          return &p.p1->children[i];
        }
      }
      break;

    case NODE16:
      p.p2 = (art_node16*)n;

      for (i = 0; i < n->num_children; i++) {
        if (((unsigned char*)p.p2->keys)[i] == c) {
          *idx = i;
          *nextnode = p.p2->children[i];
          return &p.p2->children[i];
        }
      }
      break;

    case NODE48:
      p.p3 = (art_node48*)n;
      i = p.p3->keys[c];
      if (i) {
        *idx = c;
        *nextnode = p.p3->children[i - 1];
        return &p.p3->children[i - 1];
      }
      break;

    case NODE256:
      p.p4 = (art_node256*)n;
      if (p.p4->children[c]) {
        *idx = c;
        *nextnode = p.p4->children[c];
        return &p.p4->children[c];
      }
      break;

    default:
      printf("abort in find_child() %c %d\n", c, n->type);
      abort();
  }
  *nextnode = NULL;
  return NULL;
}

void search_opt(const unsigned char* key, int key_len, int depth,
                art_node* node, art_node* parent, uint32_t parent_version,
                void** value_addr, bool* ok) {
  uint32_t version;
  art_node* nextnode;

RECUR:
  if (!read_lock_or_restart(node, &version)) {
    *value_addr = NULL;
    *ok = false;
    return;
  }
  if (!read_unlock_or_restart(parent, parent_version)) {
    *value_addr = NULL;
    *ok = false;
    return;
  }

  if (check_prefix(node, key, key_len, depth) !=
      min(node->prefix_len, MAX_PREFIX_LEN)) {
    if (!read_unlock_or_restart(node, version)) {
      *value_addr = NULL;
      *ok = false;
      return;
    }
    *value_addr = NULL;
    *ok = true;
    return;
  }
  depth += node->prefix_len;

  if (depth == key_len) {
    *value_addr = NULL;
    // void* pl = __sync_val_compare_and_swap(&node->prefix_leaf, 0, 0);
    void* pl = node->prefix_leaf.load();
    if (pl != NULL) {
      art_leaf* l = LEAF_RAW(pl);
      if (leaf_matches(l, key, key_len) == 0) {
        *value_addr = l->value;
      }
    }
    if (!read_unlock_or_restart(node, version)) {
      *value_addr = NULL;
      *ok = false;
      return;
    }
    *ok = true;
    return;
  }

  if (depth > key_len) {
    *value_addr = NULL;
    *ok = read_unlock_or_restart(node, version);
    return;
  }

  int idx;
  find_child(node, key[depth], &nextnode, &idx);
  if (!check_or_restart(node, version)) {
    *value_addr = NULL;
    *ok = false;
    return;
  }

  if (nextnode == NULL) {
    if (!read_unlock_or_restart(node, version)) {
      *value_addr = NULL;
      *ok = false;
      return;
    }
    *value_addr = NULL;
    *ok = true;
    return;
  }

  if (IS_LEAF(nextnode)) {
    *value_addr = NULL;
    art_leaf* l = LEAF_RAW(nextnode);
    if (leaf_matches(l, key, key_len) == 0) {
      *value_addr = l->value;
    }
    if (!read_unlock_or_restart(node, version)) {
      *value_addr = NULL;
      *ok = false;
      return;
    }
    *ok = true;
    return;
  }

  depth++;
  parent = node;
  parent_version = version;
  node = nextnode;
  goto RECUR;
}

void* art_search(art_tree* t, const unsigned char* key, int key_len) {
  void* value;
  bool ok;
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    search_opt(key, key_len, 0, n, NULL, 0, &value, &ok);
    if (ok) return value;
  }
}

void insert_split_prefix(art_tree* tree, art_node* node,
                         const unsigned char* key, int key_len,
                         const unsigned char* full_key, int full_key_len,
                         void* value, int depth, int prefix_len,
                         art_node** node_loc) {
  art_node* newnode = alloc_node(tree, NODE4);
  int tmp_depth = depth + prefix_len;
  art_leaf* l = make_leaf(tree, key, key_len, value);
  if (key_len == tmp_depth)
    newnode->prefix_leaf.store(SET_LEAF(l));
  else
    add_child(tree, newnode, key[tmp_depth], SET_LEAF(l));

  newnode->prefix_len = prefix_len;
  memmove(newnode->prefix, node->prefix, min(MAX_PREFIX_LEN, node->prefix_len));
  if (node->prefix_len <= MAX_PREFIX_LEN) {
    add_child(tree, newnode, node->prefix[prefix_len], node);
    node->prefix_len -= prefix_len + 1;
    memmove(node->prefix, node->prefix + prefix_len + 1,
            min(MAX_PREFIX_LEN - prefix_len - 1, node->prefix_len));
  } else {
    add_child(tree, newnode, full_key[depth + prefix_len], node);
    node->prefix_len -= prefix_len + 1;
    memmove(node->prefix, full_key + depth + prefix_len + 1,
            min(full_key_len - depth - prefix_len - 1, node->prefix_len));
  }
  // __sync_lock_test_and_set(node_loc, newnode);
  *node_loc = newnode;
}

const unsigned char* full_key(art_node* n, uint32_t version, int* key_len,
                              bool* ok) {
  // void* pl = __sync_val_compare_and_swap(&n->prefix_leaf, 0, 0);
  void* pl = n->prefix_leaf.load();
  if (pl != NULL) {
    art_leaf* l = LEAF_RAW(pl);
    if (!read_unlock_or_restart(n, version)) {
      *ok = false;
      return NULL;
    }
    *ok = true;
    *key_len = l->key_len;
    return l->key;
  }

  art_node* next = first_child(n);
  if (!check_or_restart(n, version)) {
    *ok = false;
    return NULL;
  }

  if (IS_LEAF(next)) {
    art_leaf* l = LEAF_RAW(next);
    if (!read_unlock_or_restart(n, version)) {
      *ok = false;
      return NULL;
    }
    *key_len = l->key_len;
    *ok = true;
    return l->key;
  }

  bool ok2 = read_lock_or_restart(next, &version);
  if (!ok2) {
    *ok = false;
    return NULL;
  }
  return full_key(next, version, key_len, ok);
}

bool prefix_mismatch(art_node* n, const unsigned char* key, int key_len,
                     int depth, art_node* parent, uint32_t version,
                     uint32_t parent_version, int* prefix_diff,
                     const unsigned char** fk, int* full_key_len) {
  if (n->prefix_len <= MAX_PREFIX_LEN) {
    *prefix_diff = check_prefix(n, key, key_len, depth);
    *fk = NULL;
    *full_key_len = 0;
    return true;
  }

  bool ok = false;
  while (true) {
    if (!check_or_restart(n, version) ||
        !check_or_restart(parent, parent_version)) {
      *prefix_diff = 0;
      *fk = NULL;
      *full_key_len = 0;
      return false;
    }
    if (ok) break;
    *fk = full_key(n, version, full_key_len, &ok);
  }

  int i = depth;
  int l = min(key_len, depth + n->prefix_len);
  for (; i < l; i++) {
    if (key[i] != (*fk)[i]) break;
  }
  *prefix_diff = i - depth;
  return true;
}

bool insert_opt(art_tree* tree, art_node* n, const unsigned char* key,
                int key_len, void* value, int depth, art_node* parent,
                uint32_t parent_version, art_node** node_loc,
                void** value_addr) {
  uint32_t version = 0;
  const unsigned char* full_key;
  int full_key_len;
  int prefix_diff;
  bool ok;
  art_node* nextnode;
  art_node** nextloc;

RECUR:
  if (!read_lock_or_restart(n, &version)) return false;

  ok = prefix_mismatch(n, key, key_len, depth, parent, version, parent_version,
                       &prefix_diff, &full_key, &full_key_len);
  if (!ok) return false;

  if (prefix_diff != n->prefix_len) {
    if (!upgrade_to_write_lock_or_restart(parent, parent_version)) return false;
    if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;

    insert_split_prefix(tree, n, key, key_len, full_key, full_key_len, value,
                        depth, prefix_diff, node_loc);
    if (value_addr) *value_addr = value;
    write_unlock(n);
    write_unlock(parent);
    return true;
  }
  depth += n->prefix_len;

  if (depth == key_len) {
    if (!upgrade_to_write_lock_or_restart(n, version)) return false;
    if (!read_unlock_or_restart(parent, parent_version, n)) return false;

    update_prefix_leaf(tree, n, key, key_len, value, value_addr);
    write_unlock(n);
    return true;
  }

  int idx;
  nextloc = find_child(n, key[depth], &nextnode, &idx);
  if (!check_or_restart(n, version)) return false;

  if (nextnode == NULL) {
    if (node_full(n)) {
      if (!upgrade_to_write_lock_or_restart(parent, parent_version))
        return false;
      if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;

      grow_and_insert(tree, n, key[depth],
                      SET_LEAF(make_leaf(tree, key, key_len, value)), node_loc);
      write_unlock_obsolete(n);
      write_unlock(parent);
    } else {
      if (!upgrade_to_write_lock_or_restart(n, version)) return false;
      if (!read_unlock_or_restart(parent, parent_version, n)) return false;

      add_child(tree, n, key[depth],
                SET_LEAF(make_leaf(tree, key, key_len, value)));
      write_unlock(n);
    }
    if (value_addr) *value_addr = value;
    return true;
  }

  if (!read_unlock_or_restart(parent, parent_version)) return false;

  if (IS_LEAF(nextnode)) {
    if (!upgrade_to_write_lock_or_restart(n, version)) return false;

    art_leaf* l = LEAF_RAW(nextnode);
    update_or_expand(tree, l, key, key_len, value, depth + 1, nextloc,
                     value_addr);
    write_unlock(n);
    return true;
  }

  depth++;
  parent = n;
  parent_version = version;
  node_loc = nextloc;
  n = nextnode;
  goto RECUR;
}

void art_insert(art_tree* t, const unsigned char* key, int key_len,
                void* value) {
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    if (insert_opt(t, n, key, key_len, value, 0, NULL, 0, &t->root, NULL))
      return;
  }
}

void art_insert_no_update(art_tree* t, const unsigned char* key, int key_len,
                          void* value, void** return_value) {
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    if (insert_opt(t, n, key, key_len, value, 0, NULL, 0, &t->root,
                   return_value))
      return;
  }
}

bool remove_opt(art_tree* tree, art_node* n, const unsigned char* key,
                int key_len, int depth, art_node* parent,
                uint32_t parent_version, art_node** node_loc,
                art_leaf** leaf_addr) {
  uint32_t version;
  bool ok;
  art_node* nextnode;
  art_node** nextloc;

RECUR:
  if (!read_lock_or_restart(n, &version)) return false;
  if (!read_unlock_or_restart(parent, parent_version)) return false;

  if (check_prefix(n, key, key_len, depth) !=
      min(n->prefix_len, MAX_PREFIX_LEN)) {
    if (!read_unlock_or_restart(n, version)) return false;
    return true;
  }
  depth += n->prefix_len;

  if (depth == key_len) {
    void* l = n->prefix_leaf.load();
    if (l == NULL || leaf_matches(LEAF_RAW(l), key, key_len) != 0) {
      if (!read_unlock_or_restart(n, version)) return false;
      return true;
    }

    *leaf_addr = LEAF_RAW(l);
    if (should_compress(n, parent)) {
      if (!upgrade_to_write_lock_or_restart(parent, parent_version))
        return false;
      if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;
      n->prefix_leaf.store(NULL);
      if (!compress_child(tree, n, 0, node_loc)) {
        write_unlock(n);
        write_unlock(parent);
        return false;
      }
      write_unlock_obsolete(n);
      write_unlock(parent);
    } else {
      if (!upgrade_to_write_lock_or_restart(n, version)) return false;
      n->prefix_leaf.store(NULL);
      write_unlock(n);
    }
    return true;
  }

  if (depth > key_len) return read_unlock_or_restart(n, version);

  int idx;
  nextloc = find_child(n, key[depth], &nextnode, &idx);
  if (!check_or_restart(n, version)) return false;

  if (nextnode == NULL) {
    if (!read_unlock_or_restart(n, version)) return false;
    return true;
  }

  if (IS_LEAF(nextnode)) {
    art_leaf* l = LEAF_RAW(nextnode);
    if (leaf_matches(l, key, key_len) != 0) {
      if (!read_unlock_or_restart(n, version)) return false;
      return true;
    }

    *leaf_addr = l;
    if (should_shrink(n, parent)) {
      if (!upgrade_to_write_lock_or_restart(parent, parent_version))
        return false;
      if (!upgrade_to_write_lock_or_restart(n, version, parent)) return false;
      if (!remove_child_and_shrink(tree, n, key[depth], node_loc)) {
        write_unlock(n);
        write_unlock(parent);
        return false;
      }
      write_unlock_obsolete(n);
      write_unlock(parent);
    } else {
      if (!upgrade_to_write_lock_or_restart(n, version)) return false;
      remove_child(n, idx);
      write_unlock(n);
    }
    return true;
  }

  depth++;
  parent = n;
  parent_version = version;
  node_loc = nextloc;
  n = nextnode;
  goto RECUR;
}

void* art_delete(art_tree* t, const unsigned char* key, int key_len) {
  art_leaf* l = NULL;
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    if (remove_opt(t, n, key, key_len, 0, NULL, 0, &t->root, &l)) {
      if (l) {
        void* old = l->value;
        // free(l);
        t->mem_size.fetch_sub(sizeof(art_leaf) + key_len);
        return old;
      }
      return NULL;
    }
  }
}

/*************************** Iteration **************************/
struct Iterator;
std::pair<bool, bool> iter_opt(art_node* n, Iterator* it, art_node* parent,
                               uint32_t parent_version, int depth,
                               int begin_cmp, int end_cmp, void* data);

int str_cmp_helper(const unsigned char* k1, int l1, const unsigned char* k2,
                   int l2) {
  int cmp = memcmp(k1, k2, min(l1, l2));
  if (cmp == 0) {
    if (l1 < l2)
      return -1;
    else if (l1 == l2)
      return 0;
    else
      return 1;
  }
  return cmp;
}

struct Iterator {
  const unsigned char* begin;
  int begin_len;
  const unsigned char* end;
  int end_len;
  bool include_begin;
  bool include_end;
  int k;
  art_callback cb;
  bool topk;
  const unsigned char* max;
  int max_len;

  // prev record the last applied key.
  // When iterate restart due to conflict use prev as new begin key.
  const unsigned char* prev;
  int prev_len;

  Iterator(const unsigned char* _begin = NULL, int _begin_len = 0,
           const unsigned char* _end = NULL, int _end_len = 0,
           bool _include_begin = false, bool _include_end = false, int _k = 0,
           art_callback _cb = NULL)
      : begin(_begin),
        begin_len(_begin_len),
        end(_end),
        end_len(_end_len),
        include_begin(_include_begin),
        include_end(_include_end),
        k(_k),
        cb(_cb),
        topk(false),
        max(NULL),
        max_len(0),
        prev(NULL),
        prev_len(0) {}

  std::pair<const unsigned char*, int> get_begin() {
    if (prev == NULL) return std::make_pair(begin, begin_len);
    return std::make_pair(prev, prev_len);
  }

  std::pair<const unsigned char*, int> get_end() {
    return std::make_pair(end, end_len);
  }

  bool is_include_bein() {
    if (prev == NULL) return include_begin;
    // Always start at prev's next key.
    return false;
  }

  bool is_include_end() { return include_end; }

  bool topk_cb(void* data, const unsigned char* key, uint32_t key_len,
               void* value) {
    k--;
    if (cb(data, key, key_len, value)) return true;
    if (k == 0) return true;
    return false;
  }

  void set_topk(art_callback _cb) {
    topk = true;
    cb = _cb;
  }

  // return end, ok.
  std::pair<bool, bool> access_child(art_node* n, art_node* child,
                                     uint32_t version, int depth, int begin_cmp,
                                     int end_cmp, unsigned char bkey,
                                     unsigned char ekey, unsigned char key,
                                     void* data) {
    if (IS_LEAF(child)) {
      art_leaf* l = LEAF_RAW(child);
      const unsigned char* k = l->key;
      int kl = l->key_len;
      void* v = l->value;
      if (!check_or_restart(n, version)) return std::make_pair(false, false);
      if (begin_cmp == 0 && key == bkey) {
        int cmp =
            str_cmp_helper(k + depth, kl - depth, get_begin().first + depth,
                           get_begin().second - depth);
        if (cmp < 0) {
          return std::make_pair(false, true);
        }
        if (cmp == 0 && !is_include_bein()) {
          if (str_cmp_helper(k, kl, max, max_len) == 0)
            return std::make_pair(true, true);
          else {
            max = k;
            max_len = kl;
            return std::make_pair(false, true);
          }
        }
      }
      if (end_cmp == 0 && key == ekey) {
        int cmp = str_cmp_helper(k + depth, kl - depth, get_end().first + depth,
                                 get_end().second - depth);
        if (cmp > 0 || (cmp == 0 && !is_include_end()))
          return std::make_pair(true, true);
      }
      prev = k;
      prev_len = kl;
      if (topk) return std::make_pair(topk_cb(data, k, kl, v), true);
      return std::make_pair(cb(data, k, kl, v), true);
    } else {
      if (begin_cmp == 0 && key > bkey) begin_cmp = 1;
      if (end_cmp == 0 && key < ekey) end_cmp = -1;
      return iter_opt(child, this, n, version, depth + 1, begin_cmp, end_cmp,
                      data);
    }
  }
};

bool full_compare(art_node* n, uint32_t version, const unsigned char* key,
                  int key_len, int depth, int* cmp) {
  int remain = key_len - depth;
  int check_len = min(n->prefix_len, min(MAX_PREFIX_LEN, remain));
  *cmp = memcmp(n->prefix, key + depth, check_len);
  if (*cmp == 0) {
    if (remain > MAX_PREFIX_LEN && n->prefix_len > MAX_PREFIX_LEN) {
      int fk_len;
      bool ok;
      const unsigned char* fk = full_key(n, version, &fk_len, &ok);
      if (!ok) {
        *cmp = 0;
        return false;
      }
      int l = min(n->prefix_len, key_len);
      *cmp = memcmp(fk + depth + check_len, key + depth + check_len,
                    l - check_len);
    }
  }
  if (*cmp == 0) {
    if (n->prefix_len > remain) {
      *cmp = 1;
      return true;
    }
    *cmp = 0;
    return true;
  }
  return true;
}

std::pair<bool, bool> iter_child4(art_node4* n, Iterator* it, uint32_t version,
                                  int depth, int begin_cmp, int end_cmp,
                                  void* data) {
  unsigned char bkey = 0, ekey = 0, key;
  if (begin_cmp == 0) bkey = it->get_begin().first[depth];
  if (end_cmp == 0) ekey = it->get_end().first[depth];

  art_node* child;
  for (int i = 0; i < n->n.num_children; i++) {
    key = n->keys[i];
    child = (art_node*)(n->children[i]);
    if (!check_or_restart((art_node*)(n), version))
      return std::make_pair(false, false);
    if (begin_cmp == 0 && key < bkey) continue;
    if (end_cmp == 0 && key > ekey) return std::make_pair(true, true);
    std::pair<bool, bool> p =
        it->access_child((art_node*)(n), child, version, depth, begin_cmp,
                         end_cmp, bkey, ekey, key, data);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child16(art_node16* n, Iterator* it,
                                   uint32_t version, int depth, int begin_cmp,
                                   int end_cmp, void* data) {
  unsigned char bkey = 0, ekey = 0, key;
  if (begin_cmp == 0) bkey = it->get_begin().first[depth];
  if (end_cmp == 0) ekey = it->get_end().first[depth];

  art_node* child;
  for (int i = 0; i < n->n.num_children; i++) {
    key = n->keys[i];
    child = (art_node*)(n->children[i]);
    if (!check_or_restart((art_node*)(n), version))
      return std::make_pair(false, false);
    if (begin_cmp == 0 && key < bkey) continue;
    if (end_cmp == 0 && key > ekey) return std::make_pair(true, true);
    std::pair<bool, bool> p =
        it->access_child((art_node*)(n), child, version, depth, begin_cmp,
                         end_cmp, bkey, ekey, key, data);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child48(art_node48* n, Iterator* it,
                                   uint32_t version, int depth, int begin_cmp,
                                   int end_cmp, void* data) {
  unsigned char bkey = 0, ekey = 0;
  if (begin_cmp == 0) bkey = it->get_begin().first[depth];
  if (end_cmp == 0) ekey = it->get_end().first[depth];

  art_node* child;
  for (int key = bkey; key < 256; key++) {
    if (end_cmp == 0 && key > ekey) return std::make_pair(true, true);

    int pos = n->keys[key];
    if (!check_or_restart((art_node*)(n), version))
      return std::make_pair(false, false);
    if (pos == 0) continue;

    child = (art_node*)(n->children[pos - 1]);
    if (!check_or_restart((art_node*)(n), version))
      return std::make_pair(false, false);
    std::pair<bool, bool> p =
        it->access_child((art_node*)(n), child, version, depth, begin_cmp,
                         end_cmp, bkey, ekey, key, data);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child256(art_node256* n, Iterator* it,
                                    uint32_t version, int depth, int begin_cmp,
                                    int end_cmp, void* data) {
  unsigned char bkey = 0, ekey = 0;
  if (begin_cmp == 0) bkey = it->get_begin().first[depth];
  if (end_cmp == 0) ekey = it->get_end().first[depth];

  art_node* child;
  for (int key = bkey; key < 256; key++) {
    if (end_cmp == 0 && key > ekey) return std::make_pair(true, true);

    child = (art_node*)(n->children[key]);
    if (!check_or_restart((art_node*)(n), version))
      return std::make_pair(false, false);
    if (child == NULL) continue;

    std::pair<bool, bool> p =
        it->access_child((art_node*)(n), child, version, depth, begin_cmp,
                         end_cmp, bkey, ekey, key, data);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child(art_node* n, Iterator* it, uint32_t version,
                                 int depth, int begin_cmp, int end_cmp,
                                 void* data) {
  switch (n->type) {
    case NODE4:
      return iter_child4((art_node4*)n, it, version, depth, begin_cmp, end_cmp,
                         data);
    case NODE16:
      return iter_child16((art_node16*)n, it, version, depth, begin_cmp,
                          end_cmp, data);
    case NODE48:
      return iter_child48((art_node48*)n, it, version, depth, begin_cmp,
                          end_cmp, data);
    case NODE256:
      return iter_child256((art_node256*)n, it, version, depth, begin_cmp,
                           end_cmp, data);
    default:
      printf("abort in iter_child()\n");
      abort();
  }
}

// return end, cont.
std::pair<bool, bool> iter_opt(art_node* n, Iterator* it, art_node* parent,
                               uint32_t parent_version, int depth,
                               int begin_cmp, int end_cmp, void* data) {
  uint32_t version;
  if (!read_lock_or_restart(n, &version)) return std::make_pair(false, false);
  if (!read_unlock_or_restart(parent, parent_version))
    return std::make_pair(false, false);

  if (begin_cmp == 0) {
    if (!full_compare(n, version, it->get_begin().first, it->get_begin().second,
                      depth, &begin_cmp))
      return std::make_pair(false, false);
  } else if (begin_cmp < 0)
    return std::make_pair(false, true);

  if (end_cmp == 0) {
    if (!full_compare(n, version, it->get_end().first, it->get_end().second,
                      depth, &end_cmp))
      return std::make_pair(false, false);
  } else if (end_cmp > 0)
    return std::make_pair(true, true);
  depth += n->prefix_len;

  bool use_prefix_leaf = true;
  if (begin_cmp == 0) {
    if (depth < it->get_begin().second)
      use_prefix_leaf = false;
    else {
      use_prefix_leaf = it->is_include_bein();
      begin_cmp = 1;
    }
  }
  if (end_cmp == 0 && depth == it->get_end().second) {
    use_prefix_leaf = it->is_include_end();
    end_cmp = 1;
  }

  art_leaf* prefix_leaf = LEAF_RAW(n->prefix_leaf.load());
  if (!check_or_restart(n, version)) return std::make_pair(false, false);
  if (use_prefix_leaf && prefix_leaf) {
    const unsigned char* k = prefix_leaf->key;
    int kl = prefix_leaf->key_len;
    void* v = prefix_leaf->value;
    if (!check_or_restart(n, version)) return std::make_pair(false, false);
    it->prev = k;
    it->prev_len = kl;
    if (it->topk) {
      if (it->topk_cb(data, k, kl, v)) return std::make_pair(true, true);
    } else {
      if (it->cb(data, k, kl, v)) return std::make_pair(true, true);
    }
  }
  if (end_cmp > 0) return std::make_pair(true, true);

  return iter_child(n, it, version, depth, begin_cmp, end_cmp, data);
}

void art_range(art_tree* t, const unsigned char* begin, int begin_len,
               const unsigned char* end, int end_len, bool include_begin,
               bool include_end, art_callback cb, void* data) {
  Iterator it(begin, begin_len, end, end_len, include_begin, include_end, 0,
              cb);
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    std::pair<bool, bool> p = iter_opt(n, &it, NULL, 0, 0, 0, 0, data);
    if (p.first && p.second) return;
  }
}

void art_prefix(art_tree* t, const unsigned char* key, int key_len,
                art_callback cb, void* data) {
  std::string end(reinterpret_cast<const char*>(key), key_len);
  if ((int)((uint8_t)(end[end.size() - 1])) < 255)
    end[end.size() - 1] = (char)((uint8_t)(end[end.size() - 1]) + 1);
  else
    end.push_back(0);
  art_range(t, key, key_len,
            reinterpret_cast<const unsigned char*>(end.c_str()), end.size(),
            true, false, cb, data);
}

}  // namespace mem
}  // namespace tsdb