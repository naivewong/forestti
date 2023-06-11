#include "mem/go_art.h"
#include "mem/inverted_index.h"
#include "mem/mmap.h"

namespace tsdb {
namespace mem {

extern int min(int a, int b);
extern art_leaf* make_leaf(art_tree* t, const unsigned char* key, int key_len,
                           void* value);
extern void copy_header(art_node* dest, art_node* src);
extern void add_child(art_tree* tree, art_node* n, unsigned char c,
                      void* child);
extern void add_child4(art_tree* tree, art_node4* n, unsigned char c,
                       void* child);
extern void add_child16(art_tree* tree, art_node16* n, unsigned char c,
                        void* child);
extern void add_child48(art_tree* tree, art_node48* n, unsigned char c,
                        void* child);
extern void add_child256(art_tree* tree, art_node256* n, unsigned char c,
                         void* child);
extern void grow_and_insert256(art_tree* tree, art_node256* n, unsigned char c,
                               void* child, art_node** node_loc);
extern const unsigned char* full_key(art_node* n, uint32_t version,
                                     int* key_len, bool* ok);
extern bool prefix_mismatch(art_node* n, const unsigned char* key, int key_len,
                            int depth, art_node* parent, uint32_t version,
                            uint32_t parent_version, int* prefix_diff,
                            const unsigned char** fk, int* full_key_len);

extern void remove_child(art_node* n, int i);
extern bool should_shrink(art_node* n, art_node* parent);
extern bool should_compress(art_node* n, art_node* parent);
extern art_node* first_child(art_node* n);
extern void update_prefix_leaf(art_tree* t, art_node* n,
                               const unsigned char* key, int key_len,
                               void* value, void** value_addr);
extern bool node_full(art_node* n);
extern int check_prefix(const art_node* n, const unsigned char* key,
                        int key_len, int depth);
extern int leaf_matches(const art_leaf* n, const unsigned char* key,
                        int key_len);
extern art_node** find_child(art_node* n, unsigned char c, art_node** nextnode,
                             int* idx);

extern art_node* alloc_node(art_tree* tree, uint8_t type);
art_node* alloc_node(uint8_t type) {
  art_node* n;
  switch (type) {
#ifdef MMAP_ART_NODE
    case NODE4:
      n = (art_node*)(mmap_node4->alloc());
      break;
    case NODE16:
      n = (art_node*)(mmap_node16->alloc());
      break;
    case NODE48:
      n = (art_node*)(mmap_node48->alloc());
      ((art_node48*)(n))->slots = NODE48_EMPTY_SLOTS;
      break;
    case NODE256:
      n = (art_node*)(mmap_node256->alloc());
      break;
#else
    case NODE4:
      n = (art_node*)calloc(1, sizeof(art_node4));
      break;
    case NODE16:
      n = (art_node*)calloc(1, sizeof(art_node16));
      break;
    case NODE48:
      n = (art_node*)calloc(1, sizeof(art_node48));
      ((art_node48*)(n))->slots = NODE48_EMPTY_SLOTS;
      break;
    case NODE256:
      n = (art_node*)calloc(1, sizeof(art_node256));
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

void grow_and_insert48(art_tree* tree, art_node48* n, unsigned char c,
                       void* child, art_node** node_loc, uint64_t epoch,
                       ARTObjectPool* pool,
                       moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_node256* new_node;
  if (pool)
    new_node = pool->get_node256(&(tree->mem_size));
  else
    new_node = (art_node256*)alloc_node(tree, NODE256);
  for (int i = 0; i < 256; i++) {
    if (n->keys[i]) {
      new_node->children[i] = n->children[n->keys[i] - 1];
    }
  }
  copy_header((art_node*)new_node, (art_node*)n);
  add_child256(tree, new_node, c, child);
  // __sync_lock_test_and_set(node_loc, (art_node*)new_node);
  *node_loc = (art_node*)new_node;
  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE48));
  tree->mem_size.fetch_sub(sizeof(art_node48));
}

void grow_and_insert16(art_tree* tree, art_node16* n, unsigned char c,
                       void* child, art_node** node_loc, uint64_t epoch,
                       ARTObjectPool* pool,
                       moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_node48* new_node;
  if (pool)
    new_node = pool->get_node48(&(tree->mem_size));
  else
    new_node = (art_node48*)alloc_node(tree, NODE48);
  new_node->slots = NODE48_GROW_SLOTS;

  // Copy the child pointers and populate the key map
  memcpy(new_node->children, n->children, sizeof(void*) * n->n.num_children);
  for (int i = 0; i < n->n.num_children; i++) {
    new_node->keys[n->keys[i]] = i + 1;
  }
  copy_header((art_node*)new_node, (art_node*)n);
  add_child48(tree, new_node, c, child);
  // __sync_lock_test_and_set(node_loc, (art_node*)new_node);
  *node_loc = (art_node*)new_node;
  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE16));
  tree->mem_size.fetch_sub(sizeof(art_node16));
}

void grow_and_insert4(art_tree* tree, art_node4* n, unsigned char c,
                      void* child, art_node** node_loc, uint64_t epoch,
                      ARTObjectPool* pool,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_node16* new_node;
  if (pool)
    new_node = pool->get_node16(&(tree->mem_size));
  else
    new_node = (art_node16*)alloc_node(tree, NODE16);

  // Copy the child pointers and the key map
  memcpy(new_node->children, n->children, sizeof(void*) * n->n.num_children);
  memcpy(new_node->keys, n->keys, sizeof(unsigned char) * n->n.num_children);
  copy_header((art_node*)new_node, (art_node*)n);
  add_child16(tree, new_node, c, child);
  // __sync_lock_test_and_set(node_loc, (art_node*)new_node);
  *node_loc = (art_node*)new_node;
  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE4));
  tree->mem_size.fetch_sub(sizeof(art_node4));
}

void grow_and_insert(art_tree* tree, art_node* n, unsigned char c, void* child,
                     art_node** node_loc, uint64_t epoch, ARTObjectPool* pool,
                     moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  switch (n->type) {
    case NODE4:
      grow_and_insert4(tree, (art_node4*)n, c, child, node_loc, epoch, pool,
                       gc_queue);
      break;
    case NODE16:
      grow_and_insert16(tree, (art_node16*)n, c, child, node_loc, epoch, pool,
                        gc_queue);
      break;
    case NODE48:
      grow_and_insert48(tree, (art_node48*)n, c, child, node_loc, epoch, pool,
                        gc_queue);
      break;
    case NODE256:
      grow_and_insert256(tree, (art_node256*)n, c, child, node_loc);
      break;
    default:
      printf("abort in add_child()\n");
      abort();
  }
}

bool compress_child(art_tree* tree, art_node* n, int idx, art_node** node_loc,
                    uint64_t epoch,
                    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
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

  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE4));
  tree->mem_size.fetch_sub(sizeof(art_node4));
  // __sync_lock_test_and_set(node_loc, child);
  *node_loc = child;
  return true;
}

// Actually removes node4 itself.
bool remove_child_and_shrink4(
    art_tree* tree, art_node4* n, unsigned char c, art_node** node_loc,
    uint64_t epoch, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  if (n->n.prefix_leaf.load()) {
    // __sync_lock_test_and_set(node_loc, n->n.prefix_leaf.load());
    *node_loc = (art_node*)(n->n.prefix_leaf.load());
    return true;
  }

  for (int i = 0; i < n->n.num_children; i++) {
    if (n->keys[i] != c)
      return compress_child(tree, (art_node*)(n), i, node_loc, epoch, gc_queue);
  }

  printf("abort in remove_child_and_shrink4()\n");
  abort();
}

bool remove_child_and_shrink16(
    art_tree* tree, art_node16* n, unsigned char c, art_node** node_loc,
    uint64_t epoch, ARTObjectPool* pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_node4* newnode;
  if (pool)
    newnode = pool->get_node4(&(tree->mem_size));
  else
    newnode = (art_node4*)alloc_node(tree, NODE4);
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

  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE16));
  tree->mem_size.fetch_sub(sizeof(art_node16));
  // __sync_lock_test_and_set(node_loc, (art_node*)newnode);
  *node_loc = (art_node*)newnode;
  return true;
}

bool remove_child_and_shrink48(
    art_tree* tree, art_node48* n, unsigned char c, art_node** node_loc,
    uint64_t epoch, ARTObjectPool* pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_node16* newnode;
  if (pool)
    newnode = pool->get_node16(&(tree->mem_size));
  else
    newnode = (art_node16*)alloc_node(tree, NODE16);
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

  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE48));
  tree->mem_size.fetch_sub(sizeof(art_node48));
  // __sync_lock_test_and_set(node_loc, (art_node*)newnode);
  *node_loc = (art_node*)newnode;
  return true;
}

bool remove_child_and_shrink256(
    art_tree* tree, art_node256* n, unsigned char c, art_node** node_loc,
    uint64_t epoch, ARTObjectPool* pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_node48* newnode;
  if (newnode)
    newnode = pool->get_node48(&(tree->mem_size));
  else
    newnode = (art_node48*)alloc_node(tree, NODE48);
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

  gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE256));
  tree->mem_size.fetch_sub(sizeof(art_node256));
  // __sync_lock_test_and_set(node_loc, (art_node*)newnode);
  *node_loc = (art_node*)newnode;
  return true;
}

bool remove_child_and_shrink(art_tree* tree, art_node* n, unsigned char c,
                             art_node** node_loc, uint64_t epoch,
                             ARTObjectPool* pool,
                             moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  switch (n->type) {
    case NODE4:
      return remove_child_and_shrink4(tree, (art_node4*)n, c, node_loc, epoch,
                                      gc_queue);
    case NODE16:
      return remove_child_and_shrink16(tree, (art_node16*)n, c, node_loc, epoch,
                                       pool, gc_queue);
    case NODE48:
      return remove_child_and_shrink48(tree, (art_node48*)n, c, node_loc, epoch,
                                       pool, gc_queue);
    case NODE256:
      return remove_child_and_shrink256(tree, (art_node256*)n, c, node_loc,
                                        epoch, pool, gc_queue);
    default:
      printf("abort in remove_child_and_shrink()\n");
      abort();
  }
}

void update_or_expand(art_tree* t, art_leaf* l, const unsigned char* key,
                      int key_len, void* value, int depth, art_node** node_loc,
                      void** value_addr, ARTObjectPool* pool) {
  if (leaf_matches(l, key, key_len) == 0) {
    if (value_addr)
      *value_addr = l->value;
    else
      l->value = value;
    return;
  }

  int i;
  int prefix_len = min(key_len, l->key_len);
  art_node* newnode;
  if (pool)
    newnode = (art_node*)(pool->get_node4(&(t->mem_size)));
  else
    newnode = alloc_node(t, NODE4);

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

void insert_split_prefix(art_tree* tree, art_node* node,
                         const unsigned char* key, int key_len,
                         const unsigned char* full_key, int full_key_len,
                         void* value, int depth, int prefix_len,
                         art_node** node_loc, ARTObjectPool* pool) {
  art_node* newnode;
  if (pool)
    newnode = (art_node*)(pool->get_node4(&(tree->mem_size)));
  else
    newnode = alloc_node(tree, NODE4);
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

bool insert_opt(art_tree* tree, art_node* n, const unsigned char* key,
                int key_len, void* value, int depth, art_node* parent,
                uint32_t parent_version, art_node** node_loc, void** value_addr,
                uint64_t epoch, ARTObjectPool* pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
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
                        depth, prefix_diff, node_loc, pool);
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
                      SET_LEAF(make_leaf(tree, key, key_len, value)), node_loc,
                      epoch, pool, gc_queue);
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
                     value_addr, pool);
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

void art_insert(art_tree* t, const unsigned char* key, int key_len, void* value,
                uint64_t epoch, ARTObjectPool* pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    if (insert_opt(t, n, key, key_len, value, 0, NULL, 0, &t->root, NULL, epoch,
                   pool, gc_queue))
      return;
  }
}

void art_insert_no_update(art_tree* t, const unsigned char* key, int key_len,
                          void* value, void** return_value, uint64_t epoch,
                          ARTObjectPool* pool,
                          moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    if (insert_opt(t, n, key, key_len, value, 0, NULL, 0, &t->root,
                   return_value, epoch, pool, gc_queue))
      return;
  }
}

bool remove_opt(art_tree* tree, art_node* n, const unsigned char* key,
                int key_len, int depth, art_node* parent,
                uint32_t parent_version, art_node** node_loc,
                art_leaf** leaf_addr, uint64_t epoch, ARTObjectPool* pool,
                moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
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
      if (!compress_child(tree, n, 0, node_loc, epoch, gc_queue)) {
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
      if (!remove_child_and_shrink(tree, n, key[depth], node_loc, epoch, pool,
                                   gc_queue)) {
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

void* art_delete(art_tree* t, const unsigned char* key, int key_len,
                 uint64_t epoch, ARTObjectPool* pool,
                 moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  art_leaf* l = NULL;
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    if (remove_opt(t, n, key, key_len, 0, NULL, 0, &t->root, &l, epoch, pool,
                   gc_queue)) {
      if (l) {
        void* old = l->value;
        free(l);
        t->mem_size.fetch_sub(sizeof(art_leaf) + key_len);
        return old;
      }
      return NULL;
    }
  }
}

void destroy_node(art_node* n, uint64_t epoch,
                  moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  // Break if null
  if (!n) return;

  // Special case leafs
  if (IS_LEAF(n)) {
    // free(LEAF_RAW(n));
    if (gc_queue) gc_queue->enqueue(GCRequest(epoch, (void*)(LEAF_RAW(n)), ART_LEAF));
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
        destroy_node(p.p1->children[i], epoch, gc_queue);
      }
      if (gc_queue) gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE4));
      break;

    case NODE16:
      p.p2 = (art_node16*)n;
      for (i = 0; i < n->num_children; i++) {
        destroy_node(p.p2->children[i], epoch, gc_queue);
      }
      if (gc_queue) gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE16));
      break;

    case NODE48:
      p.p3 = (art_node48*)n;
      for (i = 0; i < 256; i++) {
        idx = ((art_node48*)n)->keys[i];
        if (!idx) continue;
        destroy_node(p.p3->children[idx - 1], epoch, gc_queue);
      }
      if (gc_queue) gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE48));
      break;

    case NODE256:
      p.p4 = (art_node256*)n;
      for (i = 0; i < 256; i++) {
        if (p.p4->children[i]) destroy_node(p.p4->children[i], epoch, gc_queue);
      }
      if (gc_queue) gc_queue->enqueue(GCRequest(epoch, (void*)(n), ART_NODE256));
      break;

    default:
      printf("abort in destroy_node()\n");
      abort();
  }

  // Free ourself on the way up
  // free(n);
}

int art_tree_destroy(art_tree* t, uint64_t epoch,
                     moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  destroy_node(t->root, epoch, gc_queue);
  return 0;
}

/**************************** Node Pool *******************************/
ARTObjectPool::~ARTObjectPool() {
  art_node4* n4;
  while (node4_queue_.try_dequeue(n4)) free(n4);
  art_node16* n16;
  while (node16_queue_.try_dequeue(n16)) free(n16);
  art_node48* n48;
  while (node48_queue_.try_dequeue(n48)) free(n48);
  art_node256* n256;
  while (node256_queue_.try_dequeue(n256)) free(n256);
}

void ARTObjectPool::put_node4(art_node4* n) {
  memset(n, 0, sizeof(art_node4));
  node4_queue_.enqueue(n);
}
art_node4* ARTObjectPool::get_node4() {
  art_node4* n;
  if (!node4_queue_.try_dequeue(n)) n = (art_node4*)alloc_node(NODE4);
  n->n.type = NODE4;
  return n;
}
art_node4* ARTObjectPool::get_node4(std::atomic<uint64_t>* size) {
  art_node4* n;
  if (!node4_queue_.try_dequeue(n)) n = (art_node4*)alloc_node(NODE4);
  n->n.type = NODE4;
  size->fetch_add(sizeof(art_node4));
  return n;
}

void ARTObjectPool::put_node16(art_node16* n) {
  memset(n, 0, sizeof(art_node16));
  node16_queue_.enqueue(n);
}
art_node16* ARTObjectPool::get_node16() {
  art_node16* n;
  if (!node16_queue_.try_dequeue(n)) n = (art_node16*)alloc_node(NODE16);
  n->n.type = NODE16;
  return n;
}
art_node16* ARTObjectPool::get_node16(std::atomic<uint64_t>* size) {
  art_node16* n;
  if (!node16_queue_.try_dequeue(n)) n = (art_node16*)alloc_node(NODE16);
  n->n.type = NODE16;
  size->fetch_add(sizeof(art_node16));
  return n;
}

void ARTObjectPool::put_node48(art_node48* n) {
  memset(n, 0, sizeof(art_node48));
  node48_queue_.enqueue(n);
}
art_node48* ARTObjectPool::get_node48() {
  art_node48* n;
  if (!node48_queue_.try_dequeue(n)) n = (art_node48*)alloc_node(NODE48);
  n->n.type = NODE48;
  return n;
}
art_node48* ARTObjectPool::get_node48(std::atomic<uint64_t>* size) {
  art_node48* n;
  if (!node48_queue_.try_dequeue(n)) n = (art_node48*)alloc_node(NODE48);
  n->n.type = NODE48;
  size->fetch_add(sizeof(art_node48));
  return n;
}

void ARTObjectPool::put_node256(art_node256* n) {
  memset(n, 0, sizeof(art_node256));
  node256_queue_.enqueue(n);
}
art_node256* ARTObjectPool::get_node256() {
  art_node256* n;
  if (!node256_queue_.try_dequeue(n)) n = (art_node256*)alloc_node(NODE256);
  n->n.type = NODE256;
  return n;
}
art_node256* ARTObjectPool::get_node256(std::atomic<uint64_t>* size) {
  art_node256* n;
  if (!node256_queue_.try_dequeue(n)) n = (art_node256*)alloc_node(NODE256);
  n->n.type = NODE256;
  size->fetch_add(sizeof(art_node256));
  return n;
}

art_node* ARTObjectPool::get_node(uint8_t type) {
  art_node* n;
  switch (type) {
    case NODE4:
      art_node4* tmp1;
      if (!node4_queue_.try_dequeue(tmp1))
        n = (art_node*)calloc(1, sizeof(art_node4));
      else
        n = (art_node*)(tmp1);
      break;
    case NODE16:
      art_node16* tmp2;
      if (!node16_queue_.try_dequeue(tmp2))
        n = (art_node*)calloc(1, sizeof(art_node16));
      else
        n = (art_node*)(tmp2);
      break;
    case NODE48:
      art_node48* tmp3;
      if (!node48_queue_.try_dequeue(tmp3))
        n = (art_node*)calloc(1, sizeof(art_node48));
      else
        n = (art_node*)(tmp3);
      break;
    case NODE256:
      art_node256* tmp4;
      if (!node256_queue_.try_dequeue(tmp4))
        n = (art_node*)calloc(1, sizeof(art_node256));
      else
        n = (art_node*)(tmp4);
      break;
    default:
      printf("abort in get_node()\n");
      abort();
  }
  n->type = type;
  return n;
}

void ARTObjectPool::put_node(art_node* n) {
  switch (n->type) {
    case NODE4:
      node4_queue_.enqueue((art_node4*)(n));
      break;
    case NODE16:
      node16_queue_.enqueue((art_node16*)(n));
      break;
    case NODE48:
      node48_queue_.enqueue((art_node48*)(n));
      break;
    case NODE256:
      node256_queue_.enqueue((art_node256*)(n));
      break;
    default:
      printf("abort in put_node()\n");
      abort();
  }
}

}  // namespace mem
}  // namespace tsdb