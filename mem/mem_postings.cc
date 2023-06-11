#include "mem/mem_postings.h"

#include <algorithm>
#include <boost/filesystem.hpp>
#include <chrono>
#include <iostream>
#include <limits>
#include <unordered_map>

#include "base/Logging.hpp"
#include "disk/log_manager.h"
#include "mem/inverted_index.h"
#include "mem/mergeset.h"
#include "util/coding.h"

namespace tsdb {
namespace mem {

/********************* intersectPostingsLists **************************/
class intersectPostingsLists : public ::tsdb::index::PostingsInterface {
 private:
  std::vector<postings_list*> p;

 public:
  intersectPostingsLists(std::vector<postings_list*>&& _p) : p(std::move(_p)) {
    for (postings_list* pl : p) {
      pl->lock();
      pl->list_.reset_cursor();
    }
  }

  ~intersectPostingsLists() {
    for (postings_list* pl : p) pl->unlock();
  }

  bool recursive_next(uint64_t max) const {
    while (true) {
      bool find = true;
      for (size_t i = 0; i < p.size(); i++) {
        bool has_next = p[i]->list_.seek(max);
        if (!has_next) return false;
        if (p[i]->list_.at() > max) {
          max = p[i]->list_.at();
          find = false;
        }
      }
      if (find) return true;
    }
  }

  bool next() const {
    uint64_t max = 0;
    for (size_t i = 0; i < p.size(); i++) {
      if (!p[i]->list_.next()) return false;
      if (p[i]->list_.at() > max) max = p[i]->list_.at();
    }
    return recursive_next(max);
  }

  bool seek(uint64_t v) const {
    uint64_t max = 0;

    for (size_t i = 0; i < p.size(); i++) {
      if (!p[i]->list_.seek(v)) return false;
      if (p[i]->list_.at() > max) max = p[i]->list_.at();
    }
    return recursive_next(max);
  }

  uint64_t at() const { return p[0]->list_.at(); }
};

void intersect_postings_lists(std::vector<postings_list*>&& pls,
                              std::vector<uint64_t>* pids) {
  intersectPostingsLists p(std::move(pls));
  while (p.next()) pids->push_back(p.at());
}

/********************* tag_values_node_flat **************************/
int _flat_compare(tag_values_node_flat* node, int idx, const std::string& s) {
  int j = 0;
  for (int i = node->pos_[idx];
       i < node->pos_[idx] + std::min(node->size_[idx], (int)(s.size())); i++) {
    if (node->data_[i] < s[j])
      return -1;
    else if (node->data_[i] > s[j])
      return 1;
    j++;
  }
  if (s.size() == node->size_[idx])
    return 0;
  else if (s.size() < node->size_[idx])
    return -1;
  else
    return 1;
}

int _flat_upper_bound(tag_values_node_flat* node, const std::string& s) {
  int count = node->pos_.size(), it, first = 0, step;
  while (count > 0) {
    it = first;
    step = count / 2;
    it += step;
    if (!(_flat_compare(node, it, s) > 0)) {
      first = ++it;
      count -= step + 1;
    } else
      count = step;
  }
  return first;
}

void flat_add(tag_values_node_flat* node, uint64_t addr, const std::string& s) {
  base::RWLockGuard lock(node->mutex_, 1);
  int idx = _flat_upper_bound(node, s);
  if (idx >= node->pos_.size()) {
    node->pos_.push_back(node->data_.size());
    node->size_.push_back(s.size());
    node->data_.append(s);
    leveldb::PutFixed64(&node->data_, addr);
  } else {
    std::string tmp = s;
    leveldb::PutFixed64(&tmp, addr);
    node->data_ = node->data_.substr(0, node->pos_[idx]) + tmp +
                  node->data_.substr(node->pos_[idx]);
    node->pos_.insert(node->pos_.begin() + idx, node->pos_[idx]);
    for (size_t i = idx + 1; i < node->pos_.size(); i++)
      node->pos_[i] += tmp.size();
    node->size_.insert(node->size_.begin() + idx, s.size());
  }
}

bool flat_add_update(tag_values_node_flat* node, uint64_t addr,
                     const std::string& s) {
  base::RWLockGuard lock(node->mutex_, 1);
  int idx = _flat_upper_bound(node, s);
  if (idx >= node->pos_.size()) {
    if (!node->pos_.empty() &&
        node->data_.substr(node->pos_[idx - 1], node->size_[idx - 1]) == s) {
      uint64_t v;
      leveldb::GetFixed64Ptr(
          node->data_.data() + node->pos_[idx - 1] + node->size_[idx - 1], &v);

      // Update.
      std::string tmp;
      leveldb::PutFixed64(&tmp, addr);
      node->data_ =
          node->data_.substr(0, node->pos_[idx - 1] + node->size_[idx - 1]) +
          tmp;
      return true;
    }
  } else {
    if (idx > 0 &&
        node->data_.substr(node->pos_[idx - 1], node->size_[idx - 1]) == s) {
      uint64_t v;
      leveldb::GetFixed64Ptr(
          node->data_.data() + node->pos_[idx - 1] + node->size_[idx - 1], &v);

      // Update.
      std::string tmp;
      leveldb::PutFixed64(&tmp, addr);
      node->data_ =
          node->data_.substr(0, node->pos_[idx - 1] + node->size_[idx - 1]) +
          tmp + node->data_.substr(node->pos_[idx]);
      return true;
    }
  }
  return false;
}

bool flat_add_no_update(tag_values_node_flat* node, uint64_t addr,
                        const std::string& s) {
  base::RWLockGuard lock(node->mutex_, 1);
  int idx = _flat_upper_bound(node, s);
  if (idx >= node->pos_.size()) {
    if (node->pos_.empty() ||
        node->data_.substr(node->pos_[idx - 1], node->size_[idx - 1]) != s) {
      node->pos_.push_back(node->data_.size());
      node->size_.push_back(s.size());
      node->data_.append(s);
      leveldb::PutFixed64(&node->data_, addr);
      return true;
    }
    return false;
  }
  if (idx == 0 ||
      node->data_.substr(node->pos_[idx - 1], node->size_[idx - 1]) != s) {
    std::string tmp = s;
    leveldb::PutFixed64(&tmp, addr);
    node->data_ = node->data_.substr(0, node->pos_[idx]) + tmp +
                  node->data_.substr(node->pos_[idx]);
    node->pos_.insert(node->pos_.begin() + idx, node->pos_[idx]);
    for (size_t i = idx + 1; i < node->pos_.size(); i++)
      node->pos_[i] += tmp.size();
    node->size_.insert(node->size_.begin() + idx, s.size());
    return true;
  }
  return false;
}

uint64_t flat_get(tag_values_node_flat* node, const std::string& s) {
  base::RWLockGuard lock(node->mutex_, 0);
  // printf("flat_add %d\n", node->pos_.size());
  int idx = _flat_upper_bound(node, s) - 1;
  if (idx >= 0 && idx < node->pos_.size() && _flat_compare(node, idx, s) == 0) {
    uint64_t v;
    leveldb::GetFixed64Ptr(
        node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
    return v;
  }
  return std::numeric_limits<uint64_t>::max();
}

void flat_del(tag_values_node_flat* node, const std::string& s) {
  base::RWLockGuard lock(node->mutex_, 1);
  int idx = _flat_upper_bound(node, s) - 1;
  if (idx >= 0 && idx < node->pos_.size() && _flat_compare(node, idx, s) == 0) {
    if (idx + 1 < node->pos_.size()) {
      node->data_ = node->data_.substr(0, node->pos_[idx]) +
                    node->data_.substr(node->pos_[idx + 1]);
      int diff = node->pos_[idx + 1] - node->pos_[idx];
      for (size_t i = idx + 1; i < node->pos_.size(); i++)
        node->pos_[i] -= diff;
    } else
      node->data_ = node->data_.substr(0, node->pos_[idx]);
    node->pos_.erase(node->pos_.begin() + idx);
    node->size_.erase(node->size_.begin() + idx);
  }
}

void flat_remove_with_addr(tag_values_node_flat* node, uint64_t addr) {
  base::RWLockGuard lock(node->mutex_, 1);
  int idx = 0;
  while (idx < node->pos_.size()) {
    uint64_t v;
    leveldb::GetFixed64Ptr(
        node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
    if (v == addr) {
      for (size_t i = idx + 1; i < node->pos_.size(); i++)
        node->pos_[i] -= node->pos_[idx + 1] - node->pos_[idx];
      if (idx + 1 < node->pos_.size())
        node->data_ = node->data_.substr(0, node->pos_[idx]) +
                      node->data_.substr(node->pos_[idx + 1]);
      else
        node->data_ = node->data_.substr(0, node->pos_[idx]);
      node->pos_.erase(node->pos_.begin() + idx);
      node->size_.erase(node->size_.begin() + idx);
    }
    idx++;
  }
}

void flat_get_key_with_addr(tag_values_node_flat* node, uint64_t addr,
                            std::string* s) {
  base::RWLockGuard lock(node->mutex_, 0);
  int idx = 0;
  while (idx < node->pos_.size()) {
    uint64_t v;
    leveldb::GetFixed64Ptr(
        node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
    if (v == addr) {
      s->append(node->data_.data() + node->pos_[idx], node->size_[idx]);
    }
    idx++;
  }
}

void flat_convert(tag_values_node_flat* node, tag_values_node_art* artnode,
                  std::vector<postings_list*>* lists) {
  ART_NS::art_tree_init(&artnode->tree_);
  int idx = 0;
  uint32_t version;
  base::RWLockGuard lock(node->mutex_, 0);
  while (idx < node->pos_.size()) {
    std::string tmp = node->data_.substr(node->pos_[idx], node->size_[idx]);
    uint64_t v;
    leveldb::GetFixed64Ptr(
        node->data_.data() + node->pos_[idx] + node->size_[idx], &v);

    // postings_list* list = (postings_list*)((uintptr_t)v);

    // if (!write_lock_or_restart(list)) {
    //   ++idx;
    //   continue;
    // }
    ART_NS::art_insert(&artnode->tree_,
                       reinterpret_cast<const unsigned char*>(tmp.c_str()),
                       tmp.size(), (void*)((uintptr_t)v));

    ++idx;

    // lists->push_back(list);
  }
  artnode->node_.type = TAG_VALUES_NODE_ART;
  artnode->node_.version = 0;
}

/********************* tag_values_node_art **************************/
uint64_t art_get(tag_values_node_art* node, const std::string& s) {
  void* r = ART_NS::art_search(
      &node->tree_, reinterpret_cast<const unsigned char*>(s.c_str()),
      s.size());
  if (r) return (uint64_t)((uintptr_t)(r));
  return std::numeric_limits<uint64_t>::max();
}

/*********************** tag_values_node_mergeset *********************/
uint64_t tag_values_node_mergeset::get(const std::string& s) {
  std::string v;
  leveldb::Status st = ms_->Get(leveldb::ReadOptions(), s, &v);
  if (!st.ok())
    return std::numeric_limits<uint64_t>::max();
  return leveldb::DecodeFixed64(v.c_str());
}

void tag_values_node_mergeset::add(const std::string& s, uint64_t addr) {
  char v[8];
  leveldb::EncodeFixed64(v, addr);
  ms_->Put(leveldb::WriteOptions(), s, leveldb::Slice(v, 8));
}

void tag_values_node_mergeset::del(const std::string& s) {
  ms_->Delete(leveldb::WriteOptions(), s);
}

/********************* MemPostings **************************/
MemPostings::MemPostings() : postings_list_size_(0), mergeset_manager_(nullptr) {
  ART_NS::art_tree_init(&tag_keys_);
}

MemPostings::~MemPostings() {
  if (ART_NS::art_empty(&tag_keys_)) return;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    if ((uintptr_t)value >> 63) return false;
    int type = ((tag_values_node*)(value))->type;
    if (type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* node = (tag_values_node_flat*)(value);
      int idx = 0;
      while (idx < node->pos_.size()) {
        uint64_t v;
        leveldb::GetFixed64Ptr(
            node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
        // ignore on-disk postings.
        if ((v >> 63) == 0) delete (postings_list*)((uintptr_t)v);
        idx++;
      }
      delete (tag_values_node_flat*)(value);
    } else if (type == TAG_VALUES_NODE_ART) {
      tag_values_node_art* node = (tag_values_node_art*)(value);

      if (ART_NS::art_empty(&node->tree_)) return false;

      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        // ignore on-disk postings.
        if ((((uint64_t)((uintptr_t)value)) >> 63) == 0)
          delete (postings_list*)(value);
        return false;
      };
      unsigned char left[1] = {0};
      unsigned char right[1] = {255};
      ART_NS::art_range(&node->tree_, left, 1, right, 1, true, true, cb, NULL);
      ART_NS::art_tree_destroy(&node->tree_);
      delete (tag_values_node_art*)(value);
    }
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, cb, NULL);
  ART_NS::art_tree_destroy(&tag_keys_);

  if (mergeset_manager_)
    delete mergeset_manager_;
}

// void MemPostings::release_postings_list(postings_list* list) {
//   if (list->node_->type == TAG_VALUES_NODE_FLAT) {
//     flat_remove_with_addr((tag_values_node_flat*)(list->node_), list->leaf_);
//   } else if (list->node_->type == TAG_VALUES_NODE_ART) {
//     ART_LEAF_TYPE* leaf = (ART_LEAF_TYPE*)((uintptr_t)(list->leaf_));
//     ART_NS::art_delete(&(((tag_values_node_art*)(list->node_))->tree_),
//     ART_LEAF_KEY(leaf), ART_LEAF_KEY_LEN(leaf));
//   }
//   --postings_list_size_;
//   delete list;
// }

// void MemPostings::get_postings_list_tag(postings_list* list, std::string*
// tag) {
//   tag->append(reinterpret_cast<const char*>(ART_LEAF_KEY(list->node_->leaf)),
//   ART_LEAF_KEY_LEN(list->node_->leaf)); tag->append(leveldb::TAG_SEPARATOR);
//   if (list->node_->type == TAG_VALUES_NODE_FLAT) {
//     flat_get_key_with_addr((tag_values_node_flat*)(list->node_), list->leaf_,
//     tag);
//   } else if (list->node_->type == TAG_VALUES_NODE_ART) {
//     ART_LEAF_TYPE* leaf = (ART_LEAF_TYPE*)((uintptr_t)(list->leaf_));
//     tag->append(reinterpret_cast<const char*>(ART_LEAF_KEY(leaf)),
//     ART_LEAF_KEY_LEN(leaf));
//   }
// }

void MemPostings::set_log(const std::string& dir) {
  postings_log_.reset(
      new disk::LogManager(leveldb::Env::Default(), dir, "postings"));
}

postings_list* MemPostings::read_postings_from_log(uint64_t pos) {
  std::pair<leveldb::Slice, char*> p =
      postings_log_->read_record(pos & 0x7fffffffffffffff);
  std::string tn, tv;
  index::PrefixPostingsV2 pl;
  decode_postings(p.first, &tn, &tv, &pl);
  delete[] p.second;

  postings_list* list = new postings_list();
  ++postings_list_size_;
  list->list_ = std::move(pl);
  return list;
}

void MemPostings::read_postings_from_log(uint64_t pos,
                                         std::vector<uint64_t>* list) {
  std::pair<leveldb::Slice, char*> p =
      postings_log_->read_record(pos & 0x7fffffffffffffff);
  std::string tn, tv;
  decode_postings(p.first, &tn, &tv, list);
  delete[] p.second;
}

// Consider the case of flat_convert and del (which may release the list).
bool MemPostings::_flat_add(tag_values_node_flat* node, uint64_t id,
                            const label::Label& l) {
  uint64_t addr;
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  addr = flat_get(node, l.value);
  if (addr == std::numeric_limits<uint64_t>::max()) {
    // NOTE(Alec): this version is to protect from concurrent add (same l.value,
    // different id).
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

    postings_list* list = new postings_list();
    ++postings_list_size_;
    list->insert(id);
    flat_add(node, (uint64_t)((uintptr_t)list), l.value);
    if (node->pos_.size() > MAX_FLAT_CAPACITY) {
      tag_values_node_art* artnode = new tag_values_node_art();
      std::vector<postings_list*> lists;
      lists.reserve(MAX_FLAT_CAPACITY);
      flat_convert(node, artnode, &lists);

      ART_NS::art_insert(
          &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
          l.label.size(), (void*)(artnode));

      // Note(Alec): we can only unlock after the node is in the tag_keys
      // (protect del). Because del will remove the postings from the node. for
      // (size_t i = 0; i < lists.size(); i++)
      //   write_unlock(lists[i]);

      artnode->node_.access_epoch.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());

      // delete node; // TODO: GC.
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

    // NOTE(Alec): we need to return because the node may have been already
    // converted. We also protect from adding to a deleted postings. (Side
    // effect): we cannot add to a postings that is undergoing flat_convert.
    // uint32_t cur_delete_version;
    // if (!read_lock_or_restart(l, &cur_delete_version)) return false;
    l->lock();
    // NOTE(Alec): posting_list has already supported sorted and deduplicated insertion.
    l->insert(id);
    l->unlock();
    // if (!read_unlock_or_restart(l, cur_delete_version)) return false;
    if (!read_unlock_or_restart(&node->node_, version)) return false;
  }
  return true;
}

bool MemPostings::_art_add(tag_values_node_art* node, uint64_t id,
                           const label::Label& l) {
  uint64_t addr;
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  addr = art_get(node, l.value);
  if (addr == std::numeric_limits<uint64_t>::max()) {
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

    // Note(Alec): Need to consider the case of two concurrent insertion
    // competing for this empty node.
    // only one thread can get the lock, others will get false in upgrade_to_write_lock_or_restart.
    postings_list* list = new postings_list();
    ++postings_list_size_;
    list->insert(id);

    void* value = (void*)(list);
    ART_NS::art_insert_no_update(
        &node->tree_, reinterpret_cast<const unsigned char*>(l.value.c_str()),
        l.value.size(), (void*)(list), &value);
    // if (value != (void*)(list)) {
    //   delete list;
    //   --postings_list_size_;
    //   goto AGAIN;
    // }

    write_unlock(&node->node_);
  } else if (addr >> 63) {
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

    // Read from disk.
    postings_list* list = read_postings_from_log(addr);
    list->insert(id);

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

void MemPostings::_mergeset_add(tag_values_node_mergeset* node, uint64_t id, const label::Label& l) {
  uint64_t addr;
  uint32_t version;

AGAIN:
  if (!read_lock_or_restart(&node->node_, &version)) goto AGAIN;

  addr = node->get(l.value);
  if (addr == std::numeric_limits<uint64_t>::max()) {
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) goto AGAIN;

    // Note(Alec): Need to consider the case of two concurrent insertion
    // competing for this empty node.
    postings_list* list = new postings_list();
    ++postings_list_size_;
    list->insert(id);

    node->add(l.value, (uint64_t)((uintptr_t)list));

    write_unlock(&node->node_);
  } else if (addr >> 63) {
    if (!upgrade_to_write_lock_or_restart(&node->node_, version)) goto AGAIN;

    // Read from disk.
    postings_list* list = read_postings_from_log(addr);
    list->insert(id);


    node->add(l.value, (uint64_t)((uintptr_t)list));

    write_unlock(&node->node_);

    postings_log_->free_record(addr & 0x7fffffffffffffff);
  } else {
    postings_list* l = (postings_list*)((uintptr_t)addr);

    l->lock();
    // NOTE(Alec): posting_list has already supported sorted and deduplicated insertion.
    l->insert(id);
    l->unlock();
    if (!read_unlock_or_restart(&node->node_, version)) goto AGAIN;
  }
}

bool MemPostings::_flat_get(tag_values_node_flat* node, const std::string& l,
                            std::vector<uint64_t>* pids) {
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  uint64_t addr = flat_get(node, l);
  int idx1 = 0, idx2 = 0;
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      // Read from disk.
      std::vector<uint64_t> p;
      read_postings_from_log(addr, &p);

      idx1 = pids->size();
      idx2 = p.size();
      pids->insert(pids->end(), p.begin(), p.end());
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->read_epoch_.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());
      list->lock();
      idx1 = pids->size();
      idx2 = list->size();
      // pids->insert(pids->end(), list->list_.begin(), list->list_.end());
      list->list_.reset_cursor();
      while (list->list_.next()) pids->push_back(list->list_.at());
      list->unlock();
    }
  }

  if (!read_unlock_or_restart(&node->node_, version)) {
    // We need to deduplicate get.
    if (idx2 > 0)
      pids->erase(pids->begin() + idx1, pids->begin() + idx1 + idx2);
    return false;
  }
  return true;
}

bool MemPostings::_flat_get(tag_values_node_flat* node, const std::string& l,
                            postings_list** pl) {
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  uint64_t addr = flat_get(node, l);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      // Read from disk.
      postings_list* list = read_postings_from_log(addr);

      if (!upgrade_to_write_lock_or_restart(&node->node_, version))
        return false;

      bool ok = flat_add_update(node, (uint64_t)((uintptr_t)list), l);
      write_unlock(&node->node_);
      if (!ok) {
        delete list;
        --postings_list_size_;
        return false;
      }
      postings_log_->free_record(addr & 0x7fffffffffffffff);
      *pl = list;
      return true;
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->read_epoch_.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());
      *pl = list;
    }
  }

  if (!read_unlock_or_restart(&node->node_, version)) return false;
  return true;
}

bool MemPostings::_art_get(tag_values_node_art* node, const std::string& l,
                           std::vector<uint64_t>* pids) {
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  uint64_t addr = art_get(node, l);
  int idx1 = 0, idx2 = 0;
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      // Read from disk.
      std::vector<uint64_t> p;
      read_postings_from_log(addr, &p);

      idx1 = pids->size();
      idx2 = p.size();
      pids->insert(pids->end(), p.begin(), p.end());
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->read_epoch_.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());
      list->lock();
      idx1 = pids->size();
      idx2 = list->size();
      // pids->insert(pids->end(), list->list_.begin(), list->list_.end());
      list->list_.reset_cursor();
      while (list->list_.next()) pids->push_back(list->list_.at());
      list->unlock();
    }
  }

  if (!read_unlock_or_restart(&node->node_, version)) {
    // We need to deduplicate get.
    if (idx2 > 0)
      pids->erase(pids->begin() + idx1, pids->begin() + idx1 + idx2);
    return false;
  }
  return true;
}

bool MemPostings::_art_get(tag_values_node_art* node, const std::string& l,
                           postings_list** pl) {
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  uint64_t addr = art_get(node, l);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

      // Read from disk.
      postings_list* list = read_postings_from_log(addr);

      // TODO(Alec): we need to re-insert because the new pl is returned,
      // and we need somewhere to hold it.
      ART_NS::art_insert(&node->tree_,
                         reinterpret_cast<const unsigned char*>(l.c_str()),
                         l.size(), (void*)(list));

      write_unlock(&node->node_);

      postings_log_->free_record(addr & 0x7fffffffffffffff);
      *pl = list;
      return true;
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->read_epoch_.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());
      *pl = list;
    }
  }

  if (!read_unlock_or_restart(&node->node_, version)) {
    return false;
  }
  return true;
}

bool MemPostings::_mergeset_get(tag_values_node_mergeset* node, const std::string& l,
                                std::vector<uint64_t>* pids) {
  uint32_t version;

AGAIN:
  if (!read_lock_or_restart(&node->node_, &version)) goto AGAIN;

  uint64_t addr = node->get(l);
  int idx1 = 0, idx2 = 0;
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      // Read from disk.
      std::vector<uint64_t> p;
      read_postings_from_log(addr, &p);

      idx1 = pids->size();
      idx2 = p.size();
      pids->insert(pids->end(), p.begin(), p.end());
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->read_epoch_.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());
      list->lock();
      idx1 = pids->size();
      idx2 = list->size();
      // pids->insert(pids->end(), list->list_.begin(), list->list_.end());
      list->list_.reset_cursor();
      while (list->list_.next()) pids->push_back(list->list_.at());
      list->unlock();
    }
  }

  if (!read_unlock_or_restart(&node->node_, version)) {
    // We need to deduplicate get.
    if (idx2 > 0)
      pids->erase(pids->begin() + idx1, pids->begin() + idx1 + idx2);
    goto AGAIN;
  }
  return true;
}

bool MemPostings::_mergeset_get(tag_values_node_mergeset* node, const std::string& l,
                                postings_list** pl) {
  uint32_t version;

AGAIN:
  if (!read_lock_or_restart(&node->node_, &version)) goto AGAIN;

  uint64_t addr = node->get(l);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      if (!upgrade_to_write_lock_or_restart(&node->node_, version)) goto AGAIN;

      // Read from disk.
      postings_list* list = read_postings_from_log(addr);

      // TODO(Alec): we need to re-insert because the new pl is returned,
      // and we need somewhere to hold it.
      node->add(l, (uintptr_t)((void*)list));

      write_unlock(&node->node_);

      postings_log_->free_record(addr & 0x7fffffffffffffff);
      *pl = list;
      return true;
    } else {
      postings_list* list = (postings_list*)((uintptr_t)addr);
      list->read_epoch_.store(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count());
      *pl = list;
    }
  }

  if (!read_unlock_or_restart(&node->node_, version)) {
    goto AGAIN;
  }
  return true;
}

bool MemPostings::_flat_del(tag_values_node_flat* node, uint64_t id,
                            const label::Label& l, bool* deleted) {
  uint64_t addr = flat_get(node, l.value);
  if (addr != std::numeric_limits<uint64_t>::max()) {
    if (addr >> 63) {
      if (!write_lock_or_restart(&node->node_)) return false;

      // Read from disk.
      postings_list* list = read_postings_from_log(addr);
      *deleted = list->remove(id);

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
        // converted. if (!write_lock_or_restart(list)) {
        if (!write_lock_or_restart(&node->node_)) {
          list->unlock();
          return false;
        }
        // remove from flat node.
        flat_del(node, l.value);
        // delete list; // TODO: GC.
        // write_unlock_obsolete(list);
        --postings_list_size_;
        write_unlock(&node->node_);
      }
      list->unlock();
      return true;
    }
  }
  *deleted = false;
  return true;
}

bool MemPostings::_art_del(tag_values_node_art* node, uint64_t id,
                           const label::Label& l) {
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
            l.value.size());
        write_unlock(&node->node_);
        postings_log_->free_record(addr & 0x7fffffffffffffff);
        return true;
      }

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
        // if (!write_lock_or_restart(list)) {
        if (!write_lock_or_restart(&node->node_)) {
          list->unlock();
          return false;
        }
        // remove from art node.
        ART_NS::art_delete(
            &node->tree_,
            reinterpret_cast<const unsigned char*>(l.value.data()),
            l.value.size());
        // delete list; // TODO: GC.
        // write_unlock_obsolete(list);
        --postings_list_size_;
        write_unlock(&node->node_);
      }
      list->unlock();
      return deleted;
    }
  }
  return false;
}

bool MemPostings::_mergeset_del(tag_values_node_mergeset* node, uint64_t id, const label::Label& l) {
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

        // delete list; // TODO: GC.
        // write_unlock_obsolete(list);
        write_unlock(&node->node_);
      }
      list->unlock();
      return deleted;
    }
  }
  return false;
}

void MemPostings::add(uint64_t id, const label::Label& l) {
  void* r;

RECUR:
  r = ART_NS::art_search(
      &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
      l.label.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);
    node->access_epoch.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count());

    switch (node->type) {
      case TAG_VALUES_NODE_FLAT:
        if (!_flat_add((tag_values_node_flat*)(node), id, l)) goto RECUR;
        break;
      case TAG_VALUES_NODE_ART:
        if (!_art_add((tag_values_node_art*)(node), id, l)) goto RECUR;
        break;
      case TAG_VALUES_NODE_MERGESET:
        _mergeset_add((tag_values_node_mergeset*)node, id, l);
        break;
    }
  } else {
    tag_values_node_flat* node = new tag_values_node_flat();
    node->node_.type = TAG_VALUES_NODE_FLAT;
    node->node_.version = 0;
    postings_list* list = new postings_list();
    ++postings_list_size_;
    list->insert(id);
    flat_add(node, (uint64_t)((uintptr_t)list), l.value);
    void* value = (void*)(node);
    ART_NS::art_insert_no_update(
        &tag_keys_, reinterpret_cast<const unsigned char*>(l.label.c_str()),
        l.label.size(), (void*)(node), &value);
    if (value != (void*)(node)) {
      // Means another thread has already inserted.
      // We need to release the allocated objects.
      delete list;
      --postings_list_size_;
      delete node;
      goto RECUR;
    }
    node->node_.access_epoch.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count());
  }
}

void MemPostings::add(uint64_t id, const label::Labels& ls) {
  for (const label::Label& l : ls) add(id, l);
  add(id, label::ALL_POSTINGS_KEYS);
}

void MemPostings::get(const std::string& name, const std::string& value,
                      postings_list** pl) {
  void* r;

RECUR:
  r = ART_NS::art_search(&tag_keys_,
                         reinterpret_cast<const unsigned char*>(name.c_str()),
                         name.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);
    node->access_epoch.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count());
    switch (node->type) {
      case TAG_VALUES_NODE_FLAT:
        if (!_flat_get((tag_values_node_flat*)(node), value, pl)) goto RECUR;
        break;
      case TAG_VALUES_NODE_ART:
        if (!_art_get((tag_values_node_art*)(node), value, pl)) goto RECUR;
        break;
      case TAG_VALUES_NODE_MERGESET:
        if (!_mergeset_get((tag_values_node_mergeset*)(node), value, pl)) goto RECUR;
        break;
    }
  }
}

void MemPostings::get(const std::string& name, const std::string& value,
                      std::vector<uint64_t>* pids) {
  void* r;

RECUR:
  r = ART_NS::art_search(&tag_keys_,
                         reinterpret_cast<const unsigned char*>(name.c_str()),
                         name.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);
    node->access_epoch.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count());
    switch (node->type) {
      case TAG_VALUES_NODE_FLAT:
        if (!_flat_get((tag_values_node_flat*)(node), value, pids)) goto RECUR;
        break;
      case TAG_VALUES_NODE_ART:
        if (!_art_get((tag_values_node_art*)(node), value, pids)) goto RECUR;
        break;
      case TAG_VALUES_NODE_MERGESET:
        if (!_mergeset_get((tag_values_node_mergeset*)(node), value, pids)) goto RECUR;
        break;
    }
  }
}

void MemPostings::get(const label::Label& l, std::vector<uint64_t>* pids) {
  get(l.label, l.value, pids);
}

bool MemPostings::del(uint64_t id, const label::Label& l) {
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
        if (!_flat_del((tag_values_node_flat*)(node), id, l, &deleted))
          goto RECUR;
        return deleted;
      case TAG_VALUES_NODE_ART:
        if (!_art_del((tag_values_node_art*)(node), id, l))
          goto RECUR;
        return true;
      case TAG_VALUES_NODE_MERGESET:
        _mergeset_del((tag_values_node_mergeset*)(node), id, l);
        return true;
    }
  }
  return false;
}

uint64_t MemPostings::mem_size() {
  uint64_t s = art_mem_size(&tag_keys_);
  if (ART_NS::art_empty(&tag_keys_)) return s;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    int type = ((tag_values_node*)(value))->type;
    if (type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* node = (tag_values_node_flat*)(value);
      base::RWLockGuard lock(node->mutex_, 0);
      // flat node.
      *((uint64_t*)(data)) += node->pos_.size() * sizeof(uint64_t) +
                              node->data_.size() + sizeof(tag_values_node_flat);

      // postings.
      int idx = 0;
      while (idx < node->pos_.size()) {
        uint64_t v;
        leveldb::GetFixed64Ptr(
            node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
        // ignore on-disk postings.
        if (v >> 63) {
          idx++;
          continue;
        }
        postings_list* l = (postings_list*)((uintptr_t)v);
        l->lock();
        *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
                                sizeof(index::PrefixPostingsV2);
        l->unlock();
        idx++;
      }
    } else if (type == TAG_VALUES_NODE_ART) {
      tag_values_node_art* node = (tag_values_node_art*)(value);

      if (ART_NS::art_empty(&node->tree_)) return false;

      // art node.
      *((uint64_t*)(data)) += art_mem_size(&node->tree_);

      // postings.
      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        // ignore on-disk postings.
        if (((uint64_t)((uintptr_t)value)) >> 63) return false;
        postings_list* l = (postings_list*)(value);

        l->lock();
        *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
                                sizeof(index::PrefixPostingsV2);
        l->unlock();

        return false;
      };
      unsigned char left[1] = {0};
      unsigned char right[1] = {255};
      ART_NS::art_range(&node->tree_, left, 1, right, 1, true, true, cb, data);
    }
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, cb, &s);

  return s;
}

uint64_t MemPostings::postings_size() {
  uint64_t s = 0;
  if (ART_NS::art_empty(&tag_keys_)) return s;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    int type = ((tag_values_node*)(value))->type;
    if (type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* node = (tag_values_node_flat*)(value);
      base::RWLockGuard lock(node->mutex_, 0);

      // postings.
      int idx = 0;
      while (idx < node->pos_.size()) {
        uint64_t v;
        leveldb::GetFixed64Ptr(
            node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
        // ignore on-disk postings.
        if (v >> 63) {
          idx++;
          continue;
        }
        postings_list* l = (postings_list*)((uintptr_t)v);
        l->lock();
        *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
                                sizeof(index::PrefixPostingsV2);
        l->unlock();
        idx++;
      }
    } else if (type == TAG_VALUES_NODE_ART) {
      tag_values_node_art* node = (tag_values_node_art*)(value);

      if (ART_NS::art_empty(&node->tree_)) return false;

      // postings.
      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        // ignore on-disk postings.
        if (((uint64_t)((uintptr_t)value)) >> 63) return false;
        postings_list* l = (postings_list*)(value);

        l->lock();
        *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
                                sizeof(index::PrefixPostingsV2);
        l->unlock();

        return false;
      };
      unsigned char left[1] = {0};
      unsigned char right[1] = {255};
      ART_NS::art_range(&node->tree_, left, 1, right, 1, true, true, cb, data);
    }
    else if (type == TAG_VALUES_NODE_MERGESET) {
      tag_values_node_mergeset* node = (tag_values_node_mergeset*)(value);

      uint64_t id;
      leveldb::Iterator* iter = node->ms_->NewIterator(leveldb::ReadOptions());
      iter->SeekToFirst();
      while (iter->Valid()) {
        id = leveldb::DecodeFixed64(iter->value().data());
        if (id >> 63) {
          iter->Next();
          continue;
        }
        postings_list* l = (postings_list*)((uintptr_t)id);

        l->lock();
        *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
                                sizeof(index::PrefixPostingsV2);
        l->unlock();

        iter->Next();
      }
      delete iter;
    }
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, cb, &s);
  return s;
}

int MemPostings::postings_num() {
  int s = 0;
  if (ART_NS::art_empty(&tag_keys_)) return s;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    // printf("key:%s\n", key);
    int type = ((tag_values_node*)(value))->type;
    if (type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* node = (tag_values_node_flat*)(value);
      base::RWLockGuard lock(node->mutex_, 0);

      int idx = 0;
      while (idx < node->pos_.size()) {
        uint64_t v;
        leveldb::GetFixed64Ptr(
            node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
        // printf("value:%s %lu\n", std::string(node->data_.data() +
        // node->pos_[idx], node->size_[idx]).c_str(), v); ignore on-disk
        // postings.
        if (v >> 63) {
          idx++;
          continue;
        }
        *((int*)(data)) += 1;
        idx++;
      }
    } else if (type == TAG_VALUES_NODE_ART) {
      tag_values_node_art* node = (tag_values_node_art*)(value);

      if (ART_NS::art_empty(&node->tree_)) return false;

      // postings.
      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        if (((uint64_t)((uintptr_t)value)) >> 63) return false;
        *((int*)(data)) += 1;
        return false;
      };
      unsigned char left[1] = {0};
      unsigned char right[1] = {255};
      ART_NS::art_range(&node->tree_, left, 1, right, 1, true, true, cb, data);
    }
    else if (type == TAG_VALUES_NODE_MERGESET) {
      tag_values_node_mergeset* node = (tag_values_node_mergeset*)(value);

      uint64_t id;
      leveldb::Iterator* iter = node->ms_->NewIterator(leveldb::ReadOptions());
      iter->SeekToFirst();
      while (iter->Valid()) {
        id = leveldb::DecodeFixed64(iter->value().data());
        if (id >> 63) {
          iter->Next();
          continue;
        }
        *((int*)(data)) += 1;
        iter->Next();
      }
      delete iter;
    }
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, cb, &s);
  return s;
}

void MemPostings::get_values(const std::string& key,
                             std::vector<std::string>* values) {
  void* r;
  uint32_t version;

RECUR:
  r = ART_NS::art_search(&tag_keys_,
                         reinterpret_cast<const unsigned char*>(key.c_str()),
                         key.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);

    if (!read_lock_or_restart(node, &version)) goto RECUR;

    if (node->type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* fnode = (tag_values_node_flat*)node;
      base::RWLockGuard lock(fnode->mutex_, 0);
      int idx = 0;
      while (idx < fnode->pos_.size()) {
        values->emplace_back(fnode->data_.data() + fnode->pos_[idx],
                             fnode->size_[idx]);
        idx++;
      }
    } else if (node->type == TAG_VALUES_NODE_ART) {
      tag_values_node_art* anode = (tag_values_node_art*)node;

      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        ((std::vector<std::string>*)(data))
            ->emplace_back(reinterpret_cast<const char*>(key), key_len);
        return false;
      };
      unsigned char left[1] = {0};
      unsigned char right[1] = {255};
      ART_NS::art_range(&anode->tree_, left, 1, right, 1, true, true, cb,
                        values);
    }
    else if (node->type == TAG_VALUES_NODE_MERGESET) {
      tag_values_node_mergeset* mnode = (tag_values_node_mergeset*)node;
      leveldb::Iterator* iter = mnode->ms_->NewIterator(leveldb::ReadOptions());
      iter->SeekToFirst();
      while (iter->Valid()) {
        values->emplace_back(iter->key().ToString());
        iter->Next();
      }
      delete iter;
    }

    if (!read_unlock_or_restart(node, version)) {
      values->clear();
      goto RECUR;
    }
  }
}

int MemPostings::num_tag_values_nodes() {
  int num = 0;
  if (ART_NS::art_empty(&tag_keys_)) return num;
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    *((int*)(data)) += 1;
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, cb, &num);
  return num;
}

bool gc_epoch_cmp(const LastAccessTimePair& l, const LastAccessTimePair& r) {
  if (l.epoch == r.epoch) return l.value < r.value;
  return l.epoch < r.epoch;
}

void MemPostings::gc_get_tag_values(
    const std::string& key,
    std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)>* values) {
  void* r = ART_NS::art_search(
      &tag_keys_, reinterpret_cast<const unsigned char*>(key.c_str()),
      key.size());
  if (r != NULL) {
    tag_values_node* node = (tag_values_node*)(r);
    if (node->type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* fnode = (tag_values_node_flat*)(node);
      base::RWLockGuard lock(fnode->mutex_, 0);
      int idx = 0;
      while (idx < fnode->pos_.size()) {
        uint64_t v;
        leveldb::GetFixed64Ptr(
            fnode->data_.data() + fnode->pos_[idx] + fnode->size_[idx], &v);
        if ((v >> 63) == 0)
          values->emplace(std::string(fnode->data_.data() + fnode->pos_[idx],
                                      fnode->size_[idx]),
                          ((postings_list*)((uintptr_t)v))->read_epoch_.load());
        idx++;
      }
    } else if (node->type == TAG_VALUES_NODE_ART) {
      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        if ((((uint64_t)((uintptr_t)value)) >> 63) == 0)
          ((std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)>*)(data))
              ->emplace(
                  std::string(reinterpret_cast<const char*>(key), key_len),
                  ((postings_list*)(value))->read_epoch_.load());
        return false;
      };
      unsigned char left[1] = {0};
      unsigned char right[1] = {255};
      ART_NS::art_range(&(((tag_values_node_art*)(node))->tree_), left, 1,
                        right, 1, true, true, cb, values);
    }
    else if (node->type == TAG_VALUES_NODE_MERGESET) {
      tag_values_node_mergeset* node = (tag_values_node_mergeset*)(r);

      uint64_t id;
      leveldb::Iterator* iter = node->ms_->NewIterator(leveldb::ReadOptions());
      iter->SeekToFirst();
      while (iter->Valid()) {
        id = leveldb::DecodeFixed64(iter->value().data());
        if (id >> 63) {
          iter->Next();
          continue;
        }
        values->emplace(
                  iter->key().ToString(),
                  ((postings_list*)((uintptr_t)id))->read_epoch_.load());
        iter->Next();
      }
      delete iter;
    }
  }
}

void MemPostings::select_postings_for_gc(std::vector<std::string>* tag_names,
                                         std::vector<std::string>* tag_values,
                                         int num) {
  std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)> tagkeys(&gc_epoch_cmp);
  auto collect_key_epochs = [](void* data, const unsigned char* key,
                               uint32_t key_len, void* value) -> bool {
    ((std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)>*)(data))
        ->emplace(std::string(reinterpret_cast<const char*>(key), key_len),
                  ((tag_values_node*)(value))->access_epoch.load());
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true,
                    collect_key_epochs, &tagkeys);

  int counter = 0;
  std::set<LastAccessTimePair, decltype(&gc_epoch_cmp)> tagvalues(
      &gc_epoch_cmp);
  for (auto& p : tagkeys) {
    tagvalues.clear();
    gc_get_tag_values(p.value, &tagvalues);
    for (auto& p2 : tagvalues) {
      tag_names->push_back(p.value);
      tag_values->push_back(p2.value);
      counter++;
      if (counter >= num) break;
    }
    if (counter >= num) break;
  }
}

void encode_postings(std::string* dst, const std::string& tn,
                     const std::string& tv, const std::vector<uint64_t>& list) {
  leveldb::PutLengthPrefixedSlice(dst, tn);
  leveldb::PutLengthPrefixedSlice(dst, tv);
  leveldb::PutFixed32(dst, list.size());
  for (uint64_t id : list) leveldb::PutFixed64(dst, id);
}

void encode_postings(std::string* dst, const std::string& tn,
                     const std::string& tv, const index::PrefixPostingsV2& p) {
  leveldb::PutLengthPrefixedSlice(dst, tn);
  leveldb::PutLengthPrefixedSlice(dst, tv);
  leveldb::PutFixed32(dst, p.size());
  p.reset_cursor();
  while (p.next()) leveldb::PutFixed64(dst, p.at());
}

void decode_postings(leveldb::Slice src, std::string* tn, std::string* tv,
                     std::vector<uint64_t>* list) {
  leveldb::Slice result;
  leveldb::GetLengthPrefixedSlice(&src, &result);
  tn->append(result.data(), result.size());
  leveldb::GetLengthPrefixedSlice(&src, &result);
  tv->append(result.data(), result.size());
  uint32_t size;
  leveldb::GetFixed32(&src, &size);
  uint64_t id;
  for (int i = 0; i < size; i++) {
    leveldb::GetFixed64(&src, &id);
    list->push_back(id);
  }
}

void decode_postings(leveldb::Slice src, std::string* tn, std::string* tv,
                     index::PrefixPostingsV2* list) {
  leveldb::Slice result;
  leveldb::GetLengthPrefixedSlice(&src, &result);
  tn->append(result.data(), result.size());
  leveldb::GetLengthPrefixedSlice(&src, &result);
  tv->append(result.data(), result.size());
  uint32_t size;
  leveldb::GetFixed32(&src, &size);
  uint64_t id;
  for (int i = 0; i < size; i++) {
    leveldb::GetFixed64(&src, &id);
    list->insert(id);
  }
}

void MemPostings::postings_gc(
    std::vector<std::string>* tag_names, std::vector<std::string>* tag_values,
    uint64_t epoch, moodycamel::ConcurrentQueue<GCRequest>* gc_queue) {
  for (size_t i = 0; i < tag_names->size(); i++) {
  AGAIN:
    void* r = ART_NS::art_search(
        &tag_keys_,
        reinterpret_cast<const unsigned char*>(tag_names->at(i).c_str()),
        tag_names->at(i).size());
    if (r != NULL) {
      tag_values_node* node = (tag_values_node*)(r);
      if (node->type == TAG_VALUES_NODE_FLAT) {
        tag_values_node_flat* fnode = (tag_values_node_flat*)(node);
        uint64_t addr = flat_get(fnode, tag_values->at(i));
        if (addr != std::numeric_limits<uint64_t>::max() && (addr >> 63) == 0) {
          postings_list* list = (postings_list*)((uintptr_t)addr);

          if (!write_lock_or_restart(node)) goto AGAIN;
          std::string record;
          encode_postings(&record, tag_names->at(i), tag_values->at(i),
                          list->list_);
          uint64_t pos = postings_log_->add_record(record);
          flat_add_update(fnode, pos | 0x8000000000000000, tag_values->at(i));
          write_unlock(node);
          --postings_list_size_;

          if (gc_queue)
            gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
        }
      } else if (node->type == TAG_VALUES_NODE_ART) {
        tag_values_node_art* anode = (tag_values_node_art*)(node);
        uint64_t addr = art_get(anode, tag_values->at(i));
        if (addr != std::numeric_limits<uint64_t>::max() && (addr >> 63) == 0) {
          postings_list* list = (postings_list*)((uintptr_t)addr);

          if (!write_lock_or_restart(node)) goto AGAIN;
          std::string record;
          encode_postings(&record, tag_names->at(i), tag_values->at(i),
                          list->list_);
          uint64_t pos = postings_log_->add_record(record);
          ART_NS::art_insert(
              &anode->tree_,
              reinterpret_cast<const unsigned char*>(tag_values->at(i).c_str()),
              tag_values->at(i).size(),
              (void*)((uintptr_t)(pos | 0x8000000000000000)));
          write_unlock(node);
          --postings_list_size_;

          if (gc_queue)
            gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
        }
      } else if (node->type == TAG_VALUES_NODE_MERGESET) {
        tag_values_node_mergeset* anode = (tag_values_node_mergeset*)(node);
        uint64_t addr = anode->get(tag_values->at(i));
        if (addr != std::numeric_limits<uint64_t>::max() && (addr >> 63) == 0) {
          postings_list* list = (postings_list*)((uintptr_t)addr);

          if (!write_lock_or_restart(node)) goto AGAIN;
          std::string record;
          encode_postings(&record, tag_names->at(i), tag_values->at(i),
                          list->list_);
          uint64_t pos = postings_log_->add_record(record);
          anode->add(tag_values->at(i), pos | 0x8000000000000000);
          write_unlock(node);
          --postings_list_size_;

          if (gc_queue)
            gc_queue->enqueue(GCRequest(epoch, (void*)(list), POSTINGS));
        }
      }
    }
  }
}

// void MemPostings::ensure_order() {
//   if (ART_NS::art_empty(&tag_keys_)) return s;
//   auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
//                void* value) -> bool {
//     int type = ((tag_values_node*)(value))->type;
//     if (type == TAG_VALUES_NODE_FLAT) {
//       tag_values_node_flat* node = (tag_values_node_flat*)(value);
//       base::RWLockGuard lock(node->mutex_, 0);

//       // postings.
//       int idx = 0;
//       uint32_t version;
//       while (idx < node->pos_.size()) {
//         uint64_t v;
//         leveldb::GetFixed64Ptr(
//             node->data_.data() + node->pos_[idx] + node->size_[idx], &v);
//         // ignore on-disk postings.
//         if (v >> 63) {
//           while (!write_lock_or_restart(&node->node_, version)) {
//             if (is_obsolete(node->node_.version.load()))
//               return false;
//           }
//           postings_list* list = read_postings_from_log(v);
          

//           bool ok = flat_add_update(node, (uint64_t)((uintptr_t)list), l.value);
//           write_unlock(&node->node_);
//           if (!ok) {
//             delete list;
//             --postings_list_size_;
//             return false;
//           }
//           postings_log_->free_record(addr & 0x7fffffffffffffff);
//           idx++;
//           continue;
//         }
//         postings_list* l = (postings_list*)((uintptr_t)v);
//         l->lock();
//         *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
//                                 sizeof(index::PrefixPostingsV2);
//         l->unlock();
//         idx++;
//       }
//     } else if (type == TAG_VALUES_NODE_ART) {
//       tag_values_node_art* node = (tag_values_node_art*)(value);

//       if (ART_NS::art_empty(&node->tree_)) return false;

//       // postings.
//       auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
//                    void* value) -> bool {
//         // ignore on-disk postings.
//         if (((uint64_t)((uintptr_t)value)) >> 63) return false;
//         postings_list* l = (postings_list*)(value);

//         l->lock();
//         *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
//                                 sizeof(index::PrefixPostingsV2);
//         l->unlock();

//         return false;
//       };
//       unsigned char left[1] = {0};
//       unsigned char right[1] = {255};
//       ART_NS::art_range(&node->tree_, left, 1, right, 1, true, true, cb, data);
//     }
//     else if (type == TAG_VALUES_NODE_MERGESET) {
//       tag_values_node_mergeset* node = (tag_values_node_mergeset*)(value);

//       uint64_t id;
//       leveldb::Iterator* iter = node->ms_->NewIterator(leveldb::ReadOptions());
//       iter->SeekToFirst();
//       while (iter->Valid()) {
//         id = leveldb::DecodeFixed64(iter->value().data());
//         if (id >> 63) {
//           iter->Next();
//           continue;
//         }
//         postings_list* l = (postings_list*)((uintptr_t)id);

//         l->lock();
//         *((uint64_t*)(data)) += sizeof(postings_list) + l->list_.mem_size() -
//                                 sizeof(index::PrefixPostingsV2);
//         l->unlock();

//         iter->Next();
//       }
//     }
//     return false;
//   };
//   unsigned char left[1] = {0};
//   unsigned char right[1] = {255};
//   ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, cb, nullptr);
// }

void serialize_flat_node(tag_values_node_flat* node, std::string* s) {
  s->push_back(node->node_.type);
  leveldb::PutVarint64(s, node->pos_.size());
  for (size_t i = 0; i < node->pos_.size(); i++) {
    leveldb::PutVarint64(s, node->pos_[i]);
    leveldb::PutVarint64(s, node->size_[i]);
  }
  leveldb::PutLengthPrefixedSlice(s, node->data_);
}

void deserialize_flat_node(tag_values_node_flat* node,
                           const leveldb::Slice& s) {
  const char* p = reinterpret_cast<const char*>(s.data());
  node->node_.type = p[0];
  uint64_t v;
  uint64_t v_size;
  const char* tmp = leveldb::GetVarint64Ptr(p + 1, p + s.size(), &v_size);
  for (size_t i = 0; i < v_size; i++) {
    tmp = leveldb::GetVarint64Ptr(tmp, p + s.size(), &v);
    node->pos_.push_back(v);
    tmp = leveldb::GetVarint64Ptr(tmp, p + s.size(), &v);
    node->size_.push_back(v);
  }
  leveldb::Slice data;
  leveldb::GetLengthPrefixedSlice(tmp, p + s.size(), &data);
  node->data_.append(data.data(), data.size());
}

void serialize_art_node(tag_values_node_art* node, uint64_t snapshot_id,
                        std::string* s) {
  s->push_back(node->node_.type);
  leveldb::PutVarint64(s, snapshot_id);
}

void deserialize_art_node(tag_values_node_flat* node, uint64_t* snapshot_id,
                          const leveldb::Slice& s) {
  const char* p = reinterpret_cast<const char*>(s.data());
  node->node_.type = p[0];
  leveldb::GetVarint64Ptr(p + 1, p + s.size(), snapshot_id);
}

leveldb::Status MemPostings::snapshot_index(const std::string& dir) {
  std::unordered_map<std::string, uint64_t> node_pointers;  // flat/art nodes.
  auto collect_nodes = [](void* data, const unsigned char* key,
                          uint32_t key_len, void* value) -> bool {
    (*((std::unordered_map<std::string, uint64_t>*)
           data))[std::string(reinterpret_cast<const char*>(key), key_len)] =
        ((uint64_t)((uintptr_t)value));
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, collect_nodes,
                    &node_pointers);

  disk::SequentialLogManager snapshot_postings_log(leveldb::Env::Default(), dir,
                                                   "postings");
  // Snaphot the postings lists and update the flat/art nodes.
  for (auto& p : node_pointers) {
    tag_values_node* node = (tag_values_node*)((uintptr_t)(p.second));
    if (node->type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* fnode = (tag_values_node_flat*)(node);
      int idx = 0;
      while (idx < fnode->pos_.size()) {
        std::string tag_value =
            fnode->data_.substr(fnode->pos_[idx], fnode->size_[idx]);
        uint64_t v;
        leveldb::GetFixed64Ptr(
            fnode->data_.data() + fnode->pos_[idx] + fnode->size_[idx], &v);

        std::string record;
        if (v >> 63) {  // If the postings is on disk.
          std::vector<uint64_t> pl;
          read_postings_from_log(v, &pl);
          encode_postings(&record, p.first, tag_value, pl);
        } else
          encode_postings(&record, p.first, tag_value,
                          ((postings_list*)v)->list_);

        uint64_t pos = snapshot_postings_log.add_record(record);
        flat_add_update(fnode, pos | 0x8000000000000000, tag_value);
        ++idx;
      }
    } else if (node->type == TAG_VALUES_NODE_ART) {
      tag_values_node_art* anode = (tag_values_node_art*)(node);
      std::unordered_map<std::string, uint64_t> postings_pointers;
      auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
                   void* value) -> bool {
        (*((std::unordered_map<std::string, uint64_t>*)data))[std::string(
            reinterpret_cast<const char*>(key), key_len)] =
            ((uint64_t)((uintptr_t)value));
        return false;
      };
      ART_NS::art_range(&anode->tree_, left, 1, right, 1, true, true, cb,
                        &postings_pointers);

      for (auto& p2 : postings_pointers) {
        std::string record;
        if (p2.second >> 63) {
          std::vector<uint64_t> pl;
          read_postings_from_log(p2.second, &pl);
          encode_postings(&record, p.first, p2.first, pl);
        } else
          encode_postings(&record, p.first, p2.first,
                          ((postings_list*)p2.second)->list_);

        uint64_t pos = snapshot_postings_log.add_record(record);
        ART_NS::art_insert(
            &anode->tree_,
            reinterpret_cast<const unsigned char*>(p2.first.c_str()),
            p2.first.size(), (void*)((uintptr_t)(pos | 0x8000000000000000)));
      }
    }
  }

  // Snapshot flat/art nodes and value tries.
  leveldb::WritableFile* node_file;
  uint64_t value_trie_counter = 0;
  uint64_t offset = 0;
  std::unordered_map<uint64_t, uint64_t>
      offset_map;  // node pointer -> disk location.
  leveldb::Env* env = leveldb::Env::Default();
  leveldb::Status s = env->NewAppendableFile(dir + "/nodes", &node_file);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    return s;
  }
  for (auto& p : node_pointers) {
    tag_values_node* node = (tag_values_node*)((uintptr_t)(p.second));
    std::string node_record;
    std::string header;
    if (node->type == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* fnode = (tag_values_node_flat*)(node);
      serialize_flat_node(fnode, &node_record);
    } else if (node->type == TAG_VALUES_NODE_ART) {
      // Snapshot value trie.
      leveldb::WritableFile* value_file;
      leveldb::RandomRWFile* value_rwfile;
      s = env->NewAppendableFile(
          dir + "/value" + std::to_string(value_trie_counter), &value_file);
      if (!s.ok()) {
        LOG_ERROR << s.ToString();
        return s;
      }
      s = env->NewRandomRWFile(
          dir + "/value" + std::to_string(value_trie_counter), &value_rwfile);
      if (!s.ok()) {
        LOG_ERROR << s.ToString();
        return s;
      }
      tag_values_node_art* anode = (tag_values_node_art*)(node);
      art_serialization(&anode->tree_, value_file, value_rwfile);
      delete value_file;
      delete value_rwfile;

      serialize_art_node(anode, value_trie_counter, &node_record);

      ++value_trie_counter;
    } else {
      LOG_ERROR << "type error";
      abort();
    }
    leveldb::PutVarint64(&header, node_record.size());
    offset_map[p.second] = offset;
    node_file->Append(header);
    node_file->Append(node_record);
    offset += header.size() + node_record.size();
  }
  delete node_file;

  // Update tag keys trie.
  for (auto& p : node_pointers) {
    auto it = offset_map.find(p.second);
    if (it == offset_map.end()) {
      LOG_ERROR << "node not found:" << p.second;
      abort();
    }
    ART_NS::art_insert(
        &tag_keys_, reinterpret_cast<const unsigned char*>(p.first.c_str()),
        p.first.size(), (void*)((uintptr_t)(it->second) | 0x8000000000000000));
  }

  // Snapshot tag keys trie.
  leveldb::WritableFile* key_file;
  leveldb::RandomRWFile* key_rwfile;
  s = env->NewAppendableFile(dir + "/key", &key_file);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    return s;
  }
  s = env->NewRandomRWFile(dir + "/key", &key_rwfile);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    return s;
  }
  art_serialization(&tag_keys_, key_file, key_rwfile);
  delete key_file;
  delete key_rwfile;
  return leveldb::Status::OK();
}

leveldb::Status MemPostings::recover_from_snapshot(
    const std::string& dir, const std::string& main_dir) {
  leveldb::Env* env = leveldb::Env::Default();

  // Recover tag values tries.
  std::vector<tag_values_node_art*> art_nodes;
  std::vector<std::string> value_tries;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > 5 &&
          memcmp(current_file.c_str(), "value", 5) == 0)
        value_tries.push_back(current_file);
    }
  }
  std::sort(value_tries.begin(), value_tries.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(5)) < std::stoi(r.substr(5));
            });
  for (size_t i = 0; i < value_tries.size(); i++) {
    assert(i ==
           std::stoi(value_tries[i].substr(5)));  // It should be sequential.
    leveldb::RandomAccessFile* value_file;
    leveldb::Status s =
        env->NewRandomAccessFile(dir + "/" + value_tries[i], &value_file);
    if (!s.ok()) return s;
    art_nodes.push_back(new tag_values_node_art());
    art_deserialization(&art_nodes.back()->tree_, value_file);
    delete value_file;
  }

  // Recover flat/art nodes.
  std::unordered_map<uint64_t, uint64_t> offset_map;  // offset -> node pointer.
  leveldb::RandomAccessFile* node_file;
  uint64_t offset = 0;
  uint64_t record_size;
  char scratch[4096];
  char* buf;
  leveldb::Slice record;
  leveldb::Status st;
  leveldb::Status s = env->NewRandomAccessFile(dir + "/nodes", &node_file);
  if (!s.ok()) return s;
  while (offset < node_file->FileSize()) {
    uint64_t record_starting_offset = offset;
    st = node_file->Read(offset, 10, &record, scratch);
    if (!st.ok()) return st;
    const char* p = leveldb::GetVarint64Ptr(
        reinterpret_cast<const char*>(record.data()),
        reinterpret_cast<const char*>(record.data()) + 10, &record_size);
    offset += p - record.data();

    if (record_size > 4096)
      buf = new char[record_size];
    else
      buf = scratch;

    st = node_file->Read(offset, record_size, &record, buf);
    if (!st.ok()) return st;
    if (record.data()[0] == TAG_VALUES_NODE_FLAT) {
      tag_values_node_flat* fnode = new tag_values_node_flat();
      deserialize_flat_node(fnode, record);
      offset_map[record_starting_offset] = (uint64_t)((uintptr_t)fnode);
    } else if (record.data()[0] == TAG_VALUES_NODE_ART) {
      uint64_t trie_id;
      leveldb::GetVarint64Ptr(
          reinterpret_cast<const char*>(record.data()) + 1,
          reinterpret_cast<const char*>(record.data()) + record.size(),
          &trie_id);
      offset_map[record_starting_offset] =
          (uint64_t)((uintptr_t)(art_nodes[trie_id]));
    }

    if (record_size > 4096) delete[] buf;
    offset += record_size;
  }
  delete node_file;

  // Recover tag names trie.
  leveldb::RandomAccessFile* key_file;
  s = env->NewRandomAccessFile(dir + "/key", &key_file);
  if (!s.ok()) return s;
  art_deserialization(&tag_keys_, key_file);
  delete key_file;

  // Update the pointers in tag names trie.
  std::unordered_map<std::string, uint64_t> node_pointers;  // flat/art nodes.
  auto collect_nodes = [](void* data, const unsigned char* key,
                          uint32_t key_len, void* value) -> bool {
    (*((std::unordered_map<std::string, uint64_t>*)
           data))[std::string(reinterpret_cast<const char*>(key), key_len)] =
        ((uint64_t)((uintptr_t)value));
    return false;
  };
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  ART_NS::art_range(&tag_keys_, left, 1, right, 1, true, true, collect_nodes,
                    &node_pointers);
  for (auto& p : node_pointers) {
    auto it = offset_map.find(p.second & 0x7fffffffffffffff);
    if (it == offset_map.end()) {
      LOG_ERROR << "node not found:" << p.second;
      abort();
    }
    ART_NS::art_insert(&tag_keys_,
                       reinterpret_cast<const unsigned char*>(p.first.c_str()),
                       p.first.size(), (void*)((uintptr_t)(it->second)));
  }

  // We need to copy the files the the main folder.
  if (dir != main_dir) {
    std::vector<std::string> logs;
    boost::filesystem::path p(main_dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      // If it's not a directory, list it. If you want to list directories too,
      // just remove this check.
      if (boost::filesystem::is_regular_file(itr->path())) {
        // assign current file name to current_file and echo it out to the
        // console.
        std::string current_file = itr->path().filename().string();
        if (current_file.size() > 11 &&
            memcmp(current_file.c_str(), "postingslog", 11) == 0)
          logs.push_back(current_file);
      }
    }
    for (size_t i = 0; i < logs.size(); i++)
      boost::filesystem::remove(main_dir + "/" + logs[i]);

    logs.clear();
    p = boost::filesystem::path(dir);
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      // If it's not a directory, list it. If you want to list directories too,
      // just remove this check.
      if (boost::filesystem::is_regular_file(itr->path())) {
        // assign current file name to current_file and echo it out to the
        // console.
        std::string current_file = itr->path().filename().string();
        if (current_file.size() > 11 &&
            memcmp(current_file.c_str(), "postingslog", 11) == 0)
          logs.push_back(current_file);
      }
    }
    for (size_t i = 0; i < logs.size(); i++)
      boost::filesystem::copy_file(dir + "/" + logs[i],
                                   main_dir + "/" + logs[i]);
  }
  postings_log_.reset(
      new disk::LogManager(leveldb::Env::Default(), dir, "postings"));
  postings_list_size_ = postings_num();

  return leveldb::Status::OK();
}

}  // namespace mem
}  // namespace tsdb