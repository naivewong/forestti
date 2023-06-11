#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>

#include "base/Logging.hpp"
#include "mem/inverted_index.h"
#include "mem/mem_postings.h"
#include "mem/mergeset.h"
#include "util/coding.h"

namespace tsdb {
namespace mem {

/********************* MemPostingsObjectPool **************************/
MemPostingsObjectPool::~MemPostingsObjectPool() {
  tag_values_node_flat* n1;
  tag_values_node_art* n2;
  postings_list* n3;
  while (flat_objs_.try_dequeue(n1)) delete n1;
  while (art_objs_.try_dequeue(n2)) delete n2;
  while (list_objs_.try_dequeue(n3)) delete n3;
}

void MemPostingsObjectPool::put_flat_node(tag_values_node_flat* n) {
  n->clear();
  flat_objs_.enqueue(n);
}

tag_values_node_flat* MemPostingsObjectPool::get_flat_node() {
  tag_values_node_flat* n;
  if (!flat_objs_.try_dequeue(n)) n = new tag_values_node_flat();
  return n;
}

void MemPostingsObjectPool::put_art_node(tag_values_node_art* n) {
  art_objs_.enqueue(n);
}

tag_values_node_art* MemPostingsObjectPool::get_art_node() {
  tag_values_node_art* n;
  if (!art_objs_.try_dequeue(n)) n = new tag_values_node_art();
  return n;
}

void MemPostingsObjectPool::put_postings(postings_list* l) {
  l->clear();
  list_objs_.enqueue(l);
}

postings_list* MemPostingsObjectPool::get_postings() {
  postings_list* n;
  if (!list_objs_.try_dequeue(n)) n = new postings_list();
  return n;
}

/************************* For SEDA. *************************/
void flat_convert(tag_values_node_flat* node, tag_values_node_art* artnode,
                  std::vector<postings_list*>* lists);

void MemPostings::set_mergeset_manager(const std::string& dir, leveldb::Options opts) {
  mergeset_manager_ = new MergeSetManager(dir, opts);
}

leveldb::Status MemPostings::art_convert(tag_values_node_art* node, const std::string& name, uint64_t epoch, ARTObjectPool* art_object_pool, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  if (mergeset_manager_ == nullptr)
    return leveldb::Status::IOError("MergeSetManager needs initialization");

  tag_values_node_mergeset* mnode = new tag_values_node_mergeset();
  mnode->ms_ = mergeset_manager_->new_mergeset(name);
  if (mnode->ms_ == nullptr) {
    LOG_DEBUG << "cannot open mergeset " << name;
    delete mnode;
    return leveldb::Status::IOError("cannot open mergeset");
  }

  mnode->ms_->TrieToL1SSTs(&node->tree_, true);
  ART_NS::art_tree_destroy(&node->tree_, epoch, gc_queue);

  ART_NS::art_insert(
      &tag_keys_, reinterpret_cast<const unsigned char*>(name.c_str()),
      name.size(), (void*)(mnode), epoch, art_object_pool, gc_queue);

  return leveldb::Status::OK();
}

int MemPostings::art_gc(double percentage, uint64_t epoch, ARTObjectPool* art_object_pool, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)> tagkeys(&gc_epoch_cmp);
  auto collect_key_epochs = [](void* data, const unsigned char* key,
                               uint32_t key_len, void* value) -> bool {
    if (((tag_values_node*)(value))->type == TAG_VALUES_NODE_ART)
      ((std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)>*)(data))
          ->emplace(std::string(reinterpret_cast<const char*>(key), key_len),
                    ((tag_values_node*)(value))->access_epoch.load());
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true,
                    collect_key_epochs, &tagkeys);

  LOG_DEBUG << "tagkeys.size(): " << tagkeys.size();
  int counter = 0, limit = (int)((double)(tagkeys.size()) * percentage / 100.0);
  if (limit <= 0)
    limit = 1;
  for (auto& p : tagkeys) {
    if (counter == limit)
      break;
    void* r;
    uint32_t version;
AGAIN:
    r = ART_NS::art_search(&tag_keys_, reinterpret_cast<const unsigned char*>(p.value.c_str()), p.value.size());
    if (r) {
      tag_values_node* node = (tag_values_node*)(r);
      assert(node->type == TAG_VALUES_NODE_ART);
      write_lock_or_restart(node);
      tag_values_node_art* anode = (tag_values_node_art*)(node);
      leveldb::Status s = art_convert(anode, p.value, epoch, art_object_pool, gc_queue);

      if (s.ok()) {
        // delete node; // TODO: GC.
        if (gc_queue) gc_queue->enqueue(GCRequest(epoch, (void*)(node), ART_NODE));
        write_unlock_obsolete(node);
        ++counter;
      }
      else {
        LOG_DEBUG << s.ToString();
        write_unlock(node);
      }
    }
    else
      continue;
  }
  return counter;
}

postings_list* MemPostings::_new_postings_list(MemPostingsObjectPool* pool) {
  postings_list* list;
  if (pool) {
    list = pool->get_postings();
    list->clear();
  } else
    list = new postings_list();
  ++postings_list_size_;
  return list;
}

void MemPostings::release_postings_list(
    postings_list* list, uint64_t epoch, ARTObjectPool* art_object_pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  --postings_list_size_;
  // delete list;
  gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
}

// Consider the case of flat_convert and del (which may release the list).
bool MemPostings::_flat_add(
    tag_values_node_flat* node, uint64_t id, const label::Label& l,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool* art_object_pool,
    MemPostingsObjectPool* m_pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  uint64_t addr;
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  addr = flat_get(node, l.value);
  if (addr == std::numeric_limits<uint64_t>::max()) {
    // NOTE(Alec): this version is to protect from concurrent add (same l.value,
    // different id).
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

    postings_list* list = _new_postings_list(m_pool);
    list->insert(id);
    flat_add(node, (uint64_t)((uintptr_t)list), l.value);
    if (node->pos_.size() > MAX_FLAT_CAPACITY) {
      tag_values_node_art* artnode = new tag_values_node_art();
      std::vector<postings_list*> lists;
      lists.reserve(MAX_FLAT_CAPACITY);
      flat_convert(node, artnode, &lists);

      ART_NS::art_insert(
          &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
          l.label.size(), (void*)(artnode), epoch, art_object_pool, gc_queue);

      // delete node; // TODO: GC.
      gc_queue->enqueue(GCRequest(epoch, (void*)(node), FLAT_NODE));
      write_unlock_obsolete(&node->node_);
    } else
      write_unlock(&node->node_);
  } else if (addr >> 63) {
    // Read from disk.
    postings_list* list = read_postings_from_log(addr);
    list->insert(id);

    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) {
      delete list;
      --postings_list_size_;
      return false;
    }

    bool ok = flat_add_update(node, (uint64_t)((uintptr_t)list), l.value);
    write_unlock(&node->node_);
    if (!ok) {
      delete list;
      --postings_list_size_;
      return false;
    }
    postings_log_->free_record(addr & 0x7fffffffffffffff);
  } else {
    postings_list* l = (postings_list*)((uintptr_t)addr);

    l->lock();
    // NOTE(Alec): posting_list has already supported sorted and deduplicated insertion.
    l->insert(id);
    l->unlock();
    if (!read_unlock_or_restart(&node->node_, version)) return false;
  }
  return true;
}

bool MemPostings::_art_add(
    tag_values_node_art* node, uint64_t id, const label::Label& l,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool* art_object_pool,
    MemPostingsObjectPool* m_pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  uint64_t addr;
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  addr = art_get(node, l.value);
  if (addr == std::numeric_limits<uint64_t>::max()) {
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

    // Note(Alec): Need to consider the case of two concurrent insertion
    // competing for this empty node.
    postings_list* list = _new_postings_list(m_pool);
    list->insert(id);

    void* value = (void*)(list);
    ART_NS::art_insert_no_update(
        &node->tree_, reinterpret_cast<const unsigned char*>(l.value.c_str()),
        l.value.size(), (void*)(list), &value, epoch, art_object_pool,
        gc_queue);
    write_unlock(&node->node_);
  } else if (addr >> 63) {
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

    // Read from disk.
    postings_list* list = read_postings_from_log(addr);
    list->insert(id);

    // NOTE(Alec): no need to double check.
    // if ((art_get(node, l.value) >> 63) == 0) {
    //   // Means another thread already reload the postings.
    //   delete list;
    //   goto AGAIN;
    // }

    ART_NS::art_insert(&node->tree_,
                       reinterpret_cast<const unsigned char*>(l.value.c_str()),
                       l.value.size(), (void*)(list));

    write_unlock(&node->node_);

    postings_log_->free_record(addr & 0x7fffffffffffffff);
  } else {
    postings_list* l = (postings_list*)((uintptr_t)addr);

    l->lock();
    // NOTE(Alec): posting_list has already supported sorted and deduplicated insertion.
    l->insert(id);
    l->unlock();
    if (!read_unlock_or_restart(&node->node_, version)) return false;
  }
  return true;
}

bool MemPostings::_flat_del(tag_values_node_flat* node, uint64_t id,
                            const label::Label& l, bool* deleted,
                            /* NOTE(Alec): for garbage collection */
                            uint64_t epoch,
                            moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  uint64_t addr = flat_get(node, l.value);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      // Read from disk.
      postings_list* list = read_postings_from_log(addr);
      *deleted = list->remove(id);

      if (!write_lock_or_restart(&node->node_)) {
        delete list;
        --postings_list_size_;
        return false;
      }

      if (list->empty()) {
        delete list;
        --postings_list_size_;
        flat_del(node, l.value);
        write_unlock(&node->node_);
        postings_log_->free_record(addr & 0x7fffffffffffffff);
        return true;
      }

      bool ok = flat_add_update(node, (uint64_t)((uintptr_t)list), l.value);
      write_unlock(&node->node_);
      if (!ok) {
        delete list;
        --postings_list_size_;
        *deleted = false;
        return false;
      }
      postings_log_->free_record(addr & 0x7fffffffffffffff);
      return true;
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->lock();
      *deleted = list->remove(id);
      if (list->empty()) {
        // NOTE(Alec): we need to return because the node may have been already
        // converted.
        if (!write_lock_or_restart(&node->node_)) {
          list->unlock();
          return false;
        }
        // remove from flat node.
        flat_del(node, l.value);
        // delete list; // TODO: GC.
        --postings_list_size_;
        gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
        write_unlock(&node->node_);
      }
      list->unlock();
      return true;
    }
  }
  *deleted = false;
  return true;
}

bool MemPostings::_art_del(
    tag_values_node_art* node, uint64_t id, const label::Label& l,
    /* NOTE(Alec): for garbage collection and reutilization */
    uint64_t epoch, ARTObjectPool* art_object_pool,
    moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  uint64_t addr;

  addr = art_get(node, l.value);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      if (!write_lock_or_restart(&node->node_)) return false;

      // Read from disk.
      postings_list* list = read_postings_from_log(addr);
      bool deleted = list->remove(id);

      if (list->empty()) {
        delete list;
        --postings_list_size_;
        ART_NS::art_delete(
            &node->tree_,
            reinterpret_cast<const unsigned char*>(l.value.data()),
            l.value.size(), epoch, art_object_pool, gc_queue);
        write_unlock(&node->node_);
        postings_log_->free_record(addr & 0x7fffffffffffffff);
        return true;
      }

      // NOTE(Alec): no need to double check.
      // if ((art_get(node, l.value) >> 63) == 0) {
      //   // Means another thread already reload the postings.
      //   delete list;
      //   goto AGAIN;
      // }

      ART_NS::art_insert(
          &node->tree_, reinterpret_cast<const unsigned char*>(l.value.c_str()),
          l.value.size(), (void*)(list));

      write_unlock(&node->node_);

      postings_log_->free_record(addr & 0x7fffffffffffffff);
      return deleted;
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->lock();
      bool deleted = list->remove(id);
      if (list->empty()) {
        if (!write_lock_or_restart(&node->node_)) {
          list->unlock();
          return false;
        }
        // remove from art node.
        ART_NS::art_delete(
            &node->tree_,
            reinterpret_cast<const unsigned char*>(l.value.data()),
            l.value.size(), epoch, art_object_pool, gc_queue);
        // delete list; // TODO: GC.
        --postings_list_size_;
        gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
        write_unlock(&node->node_);
      }
      list->unlock();
      return deleted;
    }
  }
  return false;
}

bool MemPostings::_mergeset_del(tag_values_node_mergeset* node, uint64_t id, const label::Label& l,
                                uint64_t epoch, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  uint64_t addr;

AGAIN:
  addr = node->get(l.value);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      if (!write_lock_or_restart(&node->node_)) goto AGAIN;

      // Read from disk.
      postings_list* list = read_postings_from_log(addr);
      bool deleted = list->remove(id);

      if (list->empty()) {
        delete list;
        --postings_list_size_;
        node->del(l.value);
        write_unlock(&node->node_);
        postings_log_->free_record(addr & 0x7fffffffffffffff);
        return true;
      }

      node->add(l.value, (uintptr_t)((void*)list));

      write_unlock(&node->node_);

      postings_log_->free_record(addr & 0x7fffffffffffffff);
      return deleted;
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->lock();
      bool deleted = list->remove(id);
      if (list->empty()) {
        // if (!write_lock_or_restart(list)) {
        if (!write_lock_or_restart(&node->node_)) {
          list->unlock();
          goto AGAIN;
        }

        // remove from mergeset node.
        node->del(l.value);

        gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
        write_unlock(&node->node_);
      }
      list->unlock();
      return deleted;
    }
  }
  return false;
}

void MemPostings::add(uint64_t id, const label::Label& l, uint64_t epoch,
                      ARTObjectPool* art_object_pool,
                      MemPostingsObjectPool* m_pool,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  void* r;

RECUR:
  r = ART_NS::art_search(
      &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
      l.label.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);

    switch (node->type) {
      case TAG_VALUES_NODE_FLAT:
        if (!_flat_add((tag_values_node_flat*)(node), id, l, epoch,
                       art_object_pool, m_pool, gc_queue))
          goto RECUR;
        break;
      case TAG_VALUES_NODE_ART:
        if (!_art_add((tag_values_node_art*)(node), id, l, epoch, art_object_pool,
                 m_pool, gc_queue))
          goto RECUR;
        break;
      case TAG_VALUES_NODE_MERGESET:
        _mergeset_add((tag_values_node_mergeset*)node, id, l);
        break;
    }
  } else {
    tag_values_node_flat* node;
    if (m_pool)
      node = m_pool->get_flat_node();
    else
      node = new tag_values_node_flat();
    node->node_.type = TAG_VALUES_NODE_FLAT;
    node->node_.version = 0;
    postings_list* list = _new_postings_list(m_pool);
    list->insert(id);
    flat_add(node, (uint64_t)((uintptr_t)list), l.value);

    void* value = (void*)(node);
    ART_NS::art_insert_no_update(
        &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
        l.label.size(), (void*)(node), &value, epoch, art_object_pool,
        gc_queue);
    if (value != (void*)(node)) {
      // Means another thread has already inserted.
      // We need to release the allocated objects.
      delete list;
      --postings_list_size_;
      delete node;
      goto RECUR;
    }
  }
}

void MemPostings::add(uint64_t id, const label::Labels& ls, uint64_t epoch,
                      ARTObjectPool* art_object_pool,
                      MemPostingsObjectPool* m_pool,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  for (const label::Label& l : ls)
    add(id, l, epoch, art_object_pool, m_pool, gc_queue);
  add(id, label::ALL_POSTINGS_KEYS, epoch, art_object_pool, m_pool, gc_queue);
}

bool MemPostings::del(uint64_t id, const label::Label& l, uint64_t epoch,
                      ARTObjectPool* art_object_pool,
                      MemPostingsObjectPool* m_pool,
                      moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  void* r;
  bool deleted;

RECUR:
  r = ART_NS::art_search(
      &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
      l.label.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);
    switch (node->type) {
      case TAG_VALUES_NODE_FLAT:
        if (!_flat_del((tag_values_node_flat*)(node), id, l, &deleted, epoch,
                       gc_queue))
          goto RECUR;
        return deleted;
      case TAG_VALUES_NODE_ART:
        if (!_art_del((tag_values_node_art*)(node), id, l, epoch,
                        art_object_pool, gc_queue))
          goto RECUR;
        return true;
      case TAG_VALUES_NODE_MERGESET:
        _mergeset_del((tag_values_node_mergeset*)(node), id, l, epoch, gc_queue);
        return true;
    }
  }
  return false;
}

}  // namespace mem
}  // namespace tsdb