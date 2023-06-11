#include <unordered_map>
#include <unordered_set>

// #include "disk/log_manager.h"
#include "leveldb/env.h"
#include "mem/go_art.h"
// #include "mem/mem_postings.h"
#include "util/coding.h"

namespace tsdb {
namespace mem {

extern const unsigned char* full_key(art_node* n, uint32_t version,
                                     int* key_len, bool* ok);
extern art_node* alloc_node(art_tree* tree, uint8_t type);
extern art_leaf* make_leaf(art_tree* t, const unsigned char* key, int key_len,
                           void* value);
void art_node_serialization(art_node* n, std::string* s, uint64_t prefix_leaf);
void art_leaf_serialization(art_leaf* l, std::string* s);

namespace serialization {

// void encode_postings(std::string* dst, const std::vector<uint64_t>& list);
// void decode_postings(leveldb::Slice src, std::vector<uint64_t>* list);

int min(int a, int b) { return (a < b) ? a : b; }

class Iterator;
std::pair<bool, bool> iter_opt(
    art_node* n, Iterator* it, art_node* parent, uint32_t parent_version,
    int depth, int begin_cmp, int end_cmp, uint64_t* offset,
    leveldb::WritableFile* file,
    std::unordered_map<uint64_t, uint64_t>* offset_map);

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
  const unsigned char* max;
  int max_len;

  // prev record the last applied key.
  // When iterate restart due to conflict use prev as new begin key.
  const unsigned char* prev;
  int prev_len;

  uint64_t* offset;
  leveldb::WritableFile* file;
  std::unordered_map<uint64_t, uint64_t>* offset_map;

  Iterator(const unsigned char* _begin = NULL, int _begin_len = 0,
           const unsigned char* _end = NULL, int _end_len = 0,
           bool _include_begin = false, bool _include_end = false,
           uint64_t* offset = nullptr, leveldb::WritableFile* file = nullptr,
           std::unordered_map<uint64_t, uint64_t>* offset_map = nullptr)
      : begin(_begin),
        begin_len(_begin_len),
        end(_end),
        end_len(_end_len),
        include_begin(_include_begin),
        include_end(_include_end),
        max(NULL),
        max_len(0),
        prev(NULL),
        prev_len(0),
        offset(offset),
        file(file),
        offset_map(offset_map) {}

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

  // return end, ok.
  std::pair<bool, bool> access_child(art_node* n, art_node* child,
                                     uint32_t version, int depth, int begin_cmp,
                                     int end_cmp, unsigned char bkey,
                                     unsigned char ekey, unsigned char key) {
    if (IS_LEAF(child)) {
      art_leaf* l = LEAF_RAW(child);

      // Serialization.
      if (offset_map->find((uint64_t)((uintptr_t)(SET_LEAF(l)))) ==
          offset_map->end()) {
        (*offset_map)[(uint64_t)((uintptr_t)(SET_LEAF(l)))] = *offset;
        std::string buf;
        art_leaf_serialization(l, &buf);
        std::string header;
        header.push_back(2);  // type leaf.
        leveldb::PutVarint64(&header, buf.size());
        file->Append(header);
        file->Append(buf);
        // printf("leaf off:%lu header:%u buf:%u\n", *offset, header.size(),
        // buf.size());
        *offset += header.size() + buf.size();
      }

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
      return std::make_pair(false, true);
    } else {
      if (begin_cmp == 0 && key > bkey) begin_cmp = 1;
      if (end_cmp == 0 && key < ekey) end_cmp = -1;
      return iter_opt(child, this, n, version, depth + 1, begin_cmp, end_cmp,
                      offset, file, offset_map);
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
                                  int depth, int begin_cmp, int end_cmp) {
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
                         end_cmp, bkey, ekey, key);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child16(art_node16* n, Iterator* it,
                                   uint32_t version, int depth, int begin_cmp,
                                   int end_cmp) {
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
                         end_cmp, bkey, ekey, key);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child48(art_node48* n, Iterator* it,
                                   uint32_t version, int depth, int begin_cmp,
                                   int end_cmp) {
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
                         end_cmp, bkey, ekey, key);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child256(art_node256* n, Iterator* it,
                                    uint32_t version, int depth, int begin_cmp,
                                    int end_cmp) {
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
                         end_cmp, bkey, ekey, key);
    if (!p.second) return std::make_pair(false, false);
    if (p.first) return std::make_pair(true, true);
  }
  return std::make_pair(false, true);
}

std::pair<bool, bool> iter_child(art_node* n, Iterator* it, uint32_t version,
                                 int depth, int begin_cmp, int end_cmp) {
  switch (n->type) {
    case NODE4:
      return iter_child4((art_node4*)n, it, version, depth, begin_cmp, end_cmp);
    case NODE16:
      return iter_child16((art_node16*)n, it, version, depth, begin_cmp,
                          end_cmp);
    case NODE48:
      return iter_child48((art_node48*)n, it, version, depth, begin_cmp,
                          end_cmp);
    case NODE256:
      return iter_child256((art_node256*)n, it, version, depth, begin_cmp,
                           end_cmp);
    default:
      printf("abort in iter_child()\n");
      abort();
  }
}

// return end, cont.
std::pair<bool, bool> iter_opt(
    art_node* n, Iterator* it, art_node* parent, uint32_t parent_version,
    int depth, int begin_cmp, int end_cmp, uint64_t* offset,
    leveldb::WritableFile* file,
    std::unordered_map<uint64_t, uint64_t>* offset_map) {
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
  }

  // Serialization.
  uint64_t prefix_leaf_disk_pos = 0xffffffffffffffff;
  if (prefix_leaf) {
    if (offset_map->find((uint64_t)((uintptr_t)(SET_LEAF(prefix_leaf)))) ==
        offset_map->end()) {
      prefix_leaf_disk_pos = *offset;
      (*offset_map)[(uint64_t)((uintptr_t)(SET_LEAF(prefix_leaf)))] = *offset;
      std::string buf;
      art_leaf_serialization(prefix_leaf, &buf);
      std::string l;
      l.push_back(2);  // type leaf.
      leveldb::PutVarint64(&l, buf.size());
      file->Append(l);
      file->Append(buf);
      // printf("leaf off:%lu header:%u buf:%u\n", *offset, l.size(),
      // buf.size());
      *offset += l.size() + buf.size();
    } else
      prefix_leaf_disk_pos =
          (*offset_map)[(uint64_t)((uintptr_t)(SET_LEAF(prefix_leaf)))];
    // printf("prefix_leaf:%s pos:%lu\n", prefix_leaf->key,
    // prefix_leaf_disk_pos);
  }
  if (offset_map->find((uint64_t)((uintptr_t)n)) == offset_map->end()) {
    (*offset_map)[(uint64_t)((uintptr_t)n)] = *offset;
    std::string buf;
    art_node_serialization(n, &buf, prefix_leaf_disk_pos);
    std::string l;
    l.push_back(1);  // type node.
    leveldb::PutVarint64(&l, buf.size());
    file->Append(l);
    file->Append(buf);
    // printf("node off:%lu header:%u buf:%u\n", *offset, l.size(), buf.size());
    *offset += l.size() + buf.size();
  }

  if (end_cmp > 0) return std::make_pair(true, true);

  return iter_child(n, it, version, depth, begin_cmp, end_cmp);
}

void art_range(art_tree* t, const unsigned char* begin, int begin_len,
               const unsigned char* end, int end_len, bool include_begin,
               bool include_end, uint64_t* offset, leveldb::WritableFile* file,
               std::unordered_map<uint64_t, uint64_t>* offset_map) {
  Iterator it(begin, begin_len, end, end_len, include_begin, include_end,
              offset, file, offset_map);
  while (true) {
    art_node* n = __sync_val_compare_and_swap(&t->root, 0, 0);
    std::pair<bool, bool> p =
        iter_opt(n, &it, NULL, 0, 0, 0, 0, offset, file, offset_map);
    if (p.first && p.second) return;
  }
}

}  // namespace serialization.

void art_node_serialization(art_node* n, std::string* s, uint64_t prefix_leaf) {
  s->push_back(n->type);                  // type;
  s->push_back(n->num_children);          // num_children.
  leveldb::PutFixed32(s, n->prefix_len);  // prefix_len.
  leveldb::PutFixed64(s, prefix_leaf);    // prefix_leaf.
  s->append(reinterpret_cast<const char*>(n->prefix), MAX_PREFIX_LEN);

  if (n->type == NODE4) {
    art_node4* n4 = (art_node4*)n;
    s->append(reinterpret_cast<const char*>(n4->keys), 4);
    for (int i = 0; i < n->num_children; i++) {
      // printf("art_node_serialization 4 p:%lu\n",
      // (uint64_t)((uintptr_t)(n4->children[i])));
      leveldb::PutFixed64(s, (uint64_t)((uintptr_t)(n4->children[i])));
    }
    for (int i = n->num_children; i < 4; i++) leveldb::PutFixed64(s, 0);
  } else if (n->type == NODE16) {
    art_node16* n16 = (art_node16*)n;
    s->append(reinterpret_cast<const char*>(n16->keys), 16);
    for (int i = 0; i < n->num_children; i++) {
      // printf("art_node_serialization 16 p:%lu\n",
      // (uint64_t)((uintptr_t)(n16->children[i])));
      leveldb::PutFixed64(s, (uint64_t)((uintptr_t)(n16->children[i])));
    }
    for (int i = n->num_children; i < 16; i++) leveldb::PutFixed64(s, 0);
  } else if (n->type == NODE48) {
    art_node48* n48 = (art_node48*)n;
    s->append(reinterpret_cast<const char*>(n48->keys), 256);
    leveldb::PutFixed64(s, n48->slots);
    std::unordered_set<int> idx;
    for (int i = 0; i < 256; i++)
      if (n48->keys[i] > 0) idx.insert(n48->keys[i] - 1);
    for (int i = 0; i < 48; i++) {
      if (idx.count(i)) {
        // printf("art_node_serialization 48 p:%lu\n",
        // (uint64_t)((uintptr_t)(n48->children[i])));
        leveldb::PutFixed64(s, (uint64_t)((uintptr_t)(n48->children[i])));
      } else
        leveldb::PutFixed64(s, 0);
    }
  } else if (n->type == NODE256) {
    art_node256* n256 = (art_node256*)n;
    for (int i = 0; i < 256; i++) {
      if (n256->children[i])
        leveldb::PutFixed64(s, (uint64_t)((uintptr_t)(n256->children[i])));
      else
        leveldb::PutFixed64(s, 0);
    }
  } else {
    printf("type error in art_node_serialization\n");
    abort();
  }
}

// void encode_postings(std::string* dst, const std::vector<uint64_t>& list) {
//   leveldb::PutFixed32(dst, list.size());
//   for (uint64_t id : list)
//     leveldb::PutFixed64(dst, id);
// }

// void decode_postings(leveldb::Slice src, std::vector<uint64_t>* list) {
//   leveldb::Slice result;
//   leveldb::GetLengthPrefixedSlice(&src, &result);
//   leveldb::GetLengthPrefixedSlice(&src, &result);
//   uint32_t size;
//   leveldb::GetFixed32(&src, &size);
//   uint64_t id;
//   for (int i = 0; i < size; i++) {
//     leveldb::GetFixed64(&src, &id);
//     list->push_back(id);
//   }
// }

void art_leaf_serialization(art_leaf* l, std::string* s) {
  // if ((uint64_t)((uintptr_t)(l->value)) >> 63) {
  //   std::pair<leveldb::Slice, char*> p =
  //   read_log->read_record((uint64_t)((uintptr_t)(l->value)) &
  //   0x7fffffffffffffff); std::string tn, tv; std::vector<uint64_t> list;
  //   decode_postings(p.first, &tn, &tv, &list);
  //   delete [] p.second;

  //   std::string postings_record;
  //   encode_postings(&postings_record, list);
  //   uint64_t pos = write_log->add_record(postings_record);

  //   leveldb::PutVarint64(&s, pos);
  // }
  // else {
  //   postings_list* pl = (postings_list*)((uintptr_t)(l->value));
  //   std::string postings_record;
  //   encode_postings(&postings_record, pl->list_);
  //   uint64_t pos = write_log->add_record(postings_record);

  //   leveldb::PutVarint64(&s, pos);
  // }

  leveldb::PutVarint64(s, (uint64_t)((uintptr_t)(l->value)));
  leveldb::PutVarint64(s, l->key_len);
  s->append(reinterpret_cast<const char*>(l->key), l->key_len);
}

void art_serialization(art_tree* t, leveldb::WritableFile* file,
                       leveldb::RandomRWFile* rwfile) {
  uint64_t offset = 0;
  std::unordered_map<uint64_t, uint64_t> offset_map;

  // First round: sequtially append all nodes.
  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  serialization::art_range(t, left, 1, right, 1, true, true, &offset, file,
                           &offset_map);

  file->Flush();

  // Second round: update the pointers of children.
  uint64_t read_offset = 0;
  leveldb::Slice record;
  uint64_t record_size;
  char scratch[16 * 1024];
  char write_scratch[4096];
  char* buf;
  while (read_offset < offset) {
    rwfile->Read(read_offset, 11, &record, scratch);
    // printf("%u %u %u\n", (uint8_t)(record.data()[0]),
    // (uint8_t)(record.data()[1]), (uint8_t)(record.data()[2]));
    const char* p = leveldb::GetVarint64Ptr(
        reinterpret_cast<const char*>(record.data()) + 1,
        reinterpret_cast<const char*>(record.data()) + 11, &record_size);
    if (record.data()[0] == 2) {  // Leaf.
      read_offset += (p - record.data()) + record_size;
      continue;
    }

    if (record_size > 16 * 1024)
      buf = new char[record_size];
    else
      buf = scratch;

    // printf("-off:%lu header:%u buf:%u\n", read_offset, p - record.data(),
    // record_size);
    read_offset += (p - record.data());
    rwfile->Read(read_offset, record_size, &record, buf);

    int type = record.data()[0];
    int num_children = record.data()[1];
    int start_off = 14 + MAX_PREFIX_LEN;
    if (type == NODE4) {
      for (int i = 0; i < num_children; i++) {
        uint64_t tmp;
        leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) +
                                   start_off + 4 + i * 8,
                               &tmp);
        leveldb::EncodeFixed64(write_scratch, offset_map[tmp]);
        // printf("4off:%lu tmp:%lu child_pos:%lu\n", read_offset + start_off +
        // 4 + i * 8, tmp, offset_map[tmp]);
        rwfile->Write(read_offset + start_off + 4 + i * 8,
                      leveldb::Slice(write_scratch, 8));
      }
    } else if (type == NODE16) {
      for (int i = 0; i < num_children; i++) {
        uint64_t tmp;
        leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) +
                                   start_off + 16 + i * 8,
                               &tmp);
        leveldb::EncodeFixed64(write_scratch, offset_map[tmp]);
        // printf("16off:%lu tmp:%lu child_pos:%lu\n", read_offset + start_off +
        // 16 + i * 8, tmp, offset_map[tmp]);
        rwfile->Write(read_offset + start_off + 16 + i * 8,
                      leveldb::Slice(write_scratch, 8));
      }
    } else if (type == NODE48) {
      int idx;
      for (int i = 0; i < 256; i++) {
        idx = record.data()[start_off + i];
        if (idx > 0) {
          uint64_t tmp;
          leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) +
                                     start_off + 264 + (idx - 1) * 8,
                                 &tmp);
          leveldb::EncodeFixed64(write_scratch, offset_map[tmp]);
          // printf("48off:%lu tmp:%lu child_pos:%lu\n", read_offset + start_off
          // + 264 + (idx - 1) * 8, tmp, offset_map[tmp]);
          rwfile->Write(read_offset + start_off + 264 + (idx - 1) * 8,
                        leveldb::Slice(write_scratch, 8));
        }
      }
    } else if (type == NODE256) {
      for (int i = 0; i < 256; i++) {
        uint64_t tmp;
        leveldb::GetFixed64Ptr(
            reinterpret_cast<const char*>(record.data()) + start_off + i * 8,
            &tmp);
        if (tmp > 0) {
          leveldb::EncodeFixed64(write_scratch, offset_map[tmp]);
          // printf("256off:%lu tmp:%lu child_pos:%lu\n", read_offset +
          // start_off + i * 8, tmp, offset_map[tmp]);
          rwfile->Write(read_offset + start_off + i * 8,
                        leveldb::Slice(write_scratch, 8));
        }
      }
    } else {
      printf("type error in art_serialization\n");
      abort();
    }

    if (record_size > 16 * 1024) delete[] buf;

    read_offset += record_size;
  }
}

art_leaf* read_art_leaf(art_tree* t, const leveldb::Slice& record) {
  uint64_t postings_pos;
  const char* tmp_p1 =
      leveldb::GetVarint64Ptr(reinterpret_cast<const char*>(record.data()),
                              record.data() + 10, &postings_pos);

  uint64_t key_len;
  const char* tmp_p2 = leveldb::GetVarint64Ptr(tmp_p1, tmp_p1 + 10, &key_len);

  return make_leaf(t, reinterpret_cast<const unsigned char*>(tmp_p2), key_len,
                   (void*)((uintptr_t)postings_pos));
}

art_leaf* read_art_leaf(art_tree* t, leveldb::RandomAccessFile* file,
                        uint64_t offset) {
  char buf1[11];
  leveldb::Slice record;
  uint64_t record_size;
  file->Read(offset, 11, &record, buf1);
  const char* p = leveldb::GetVarint64Ptr(
      reinterpret_cast<const char*>(record.data()) + 1,
      reinterpret_cast<const char*>(record.data()) + 11, &record_size);
  offset += 1 + (p - record.data());

  char* buf2 = new char[record_size];
  file->Read(offset, record_size, &record, buf2);
  art_leaf* leaf = read_art_leaf(t, record);
  delete[] buf2;
  return leaf;
}

// art_leaf* read_art_leaf(const leveldb::Slice& record, disk::LogManager*
// read_log) {
//   uint64_t postings_pos;
//   char* tmp_p1 = leveldb::GetVarint64Ptr(record.data(), record.data() + 10,
//   &postings_pos); std::pair<leveldb::Slice, char*> p =
//   read_log->read_record(postings_pos); postings_list* pl = new
//   postings_list(); decode_postings(p.first, &pl->list_); delete [] p.second;

//   uint64_t key_len;
//   char* tmp_p2 = leveldb::GetVarint64Ptr(tmp_p1, tmp_p1 + 10, &key_len);

//   return make_leaf(t, tmp_p2, key_len, pl);
// }

// art_leaf* read_art_leaf(leveldb::RandomAccessFile* file, uint64_t offset,
// disk::LogManager* read_log) {
//   char buf1[11];
//   leveldb::Slice record;
//   uint64_t record_size;
//   file->Read(offset, 11, &record, buf);
//   char* p = leveldb::GetVarint64Ptr(record.data() + 1, record.data() + 11,
//   &record_size); offset += 1 + (p - record.data());

//   char* buf2 = new char[record_size];
//   file->Read(offset, record_size, &record, buf2);
//   art_leaf* leaf = read_art_leaf(record, read_log);
//   delete [] buf2;
//   return leaf;
// }

void art_deserialization(art_tree* t, leveldb::RandomAccessFile* file) {
  t->size = 0;
  t->mem_size = 0;
  uint64_t offset = 0;
  std::unordered_map<uint64_t, uint64_t> offset_map;
  uint64_t root_offset = 0xffffffffffffffff;

  leveldb::Slice record;
  uint64_t record_size;
  char scratch[16 * 1024];
  char* buf;

  // Round 1: read all objects.
  while (offset < file->FileSize()) {
    file->Read(offset, 11, &record, scratch);
    const char* p = leveldb::GetVarint64Ptr(
        reinterpret_cast<const char*>(record.data()) + 1,
        reinterpret_cast<const char*>(record.data()) + 11, &record_size);
    uint64_t record_starting_offset = offset;
    // printf("read off:%lu header:%u buf:%u\n", offset, p - record.data(),
    // record_size);
    offset += (p - record.data());

    if (record_size > 16 * 1024)
      buf = new char[record_size];
    else
      buf = scratch;

    if (record.data()[0] == 2) {  // Leaf.
      file->Read(offset, record_size, &record, buf);
      art_leaf* leaf = read_art_leaf(t, record);
      offset_map[record_starting_offset] =
          (uint64_t)((uintptr_t)(SET_LEAF(leaf)));

      offset += record_size;
      if (record_size > 16 * 1024) delete[] buf;
      continue;
    }

    if (root_offset == 0xffffffffffffffff) root_offset = record_starting_offset;
    file->Read(offset, record_size, &record, buf);

    int type = record.data()[0];
    art_node* n = alloc_node(t, type);
    offset_map[record_starting_offset] = (uint64_t)((uintptr_t)n);
    n->num_children = record.data()[1];
    // printf("num_children:%d\n", n->num_children);
    leveldb::GetFixed32Ptr(reinterpret_cast<const char*>(record.data()) + 2,
                           &n->prefix_len);
    memcpy(n->prefix, reinterpret_cast<const char*>(record.data()) + 14,
           MAX_PREFIX_LEN);
    int start_off = 14 + MAX_PREFIX_LEN;

    // Load the keys.
    if (type == NODE4) {
      art_node4* n4 = (art_node4*)n;
      memcpy(n4->keys, record.data() + start_off, 4);
    } else if (type == NODE16) {
      art_node16* n16 = (art_node16*)n;
      memcpy(n16->keys, record.data() + start_off, 16);
    } else if (type == NODE48) {
      art_node48* n48 = (art_node48*)n;
      memcpy(n48->keys, record.data() + start_off, 256);
      leveldb::GetFixed64Ptr(
          reinterpret_cast<const char*>(record.data()) + start_off + 256,
          &n48->slots);
    }

    if (record_size > 16 * 1024) delete[] buf;
    offset += record_size;
  }

  // Round 2.
  offset = 0;
  while (offset < file->FileSize()) {
    file->Read(offset, 11, &record, scratch);
    const char* p = leveldb::GetVarint64Ptr(
        reinterpret_cast<const char*>(record.data()) + 1,
        reinterpret_cast<const char*>(record.data()) + 11, &record_size);
    uint64_t record_starting_offset = offset;
    offset += (p - record.data());

    if (record.data()[0] == 2) {  // Leaf.
      offset += record_size;
      continue;
    }

    if (record_size > 16 * 1024)
      buf = new char[record_size];
    else
      buf = scratch;

    file->Read(offset, record_size, &record, buf);

    art_node* n = (art_node*)((uintptr_t)(offset_map[record_starting_offset]));
    uint64_t prefix_leaf;
    leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) + 6,
                           &prefix_leaf);
    // printf("prefix_leaf off:%lu cur:%lu\n", prefix_leaf, offset);
    if (prefix_leaf == 0xFFFFFFFFFFFFFFFFul)
      n->prefix_leaf.store(NULL);
    else {
      if (offset_map.find(prefix_leaf) != offset_map.end()) {
        n->prefix_leaf.store(
            SET_LEAF((art_leaf*)((uintptr_t)offset_map[prefix_leaf])));
        // printf("%s %u %lu\n", LEAF_RAW(n->prefix_leaf.load())->key,
        // LEAF_RAW(n->prefix_leaf.load())->key_len,
        // (uint64_t)((uintptr_t)(LEAF_RAW(n->prefix_leaf.load())->value)));
      } else {
        art_leaf* leaf = read_art_leaf(t, file, prefix_leaf);
        offset_map[prefix_leaf] = (uint64_t)((uintptr_t)(SET_LEAF(leaf)));
        n->prefix_leaf.store(SET_LEAF(leaf));
      }
    }
    int start_off = 14 + MAX_PREFIX_LEN;

    if (n->type == NODE4) {
      art_node4* n4 = (art_node4*)n;
      for (int i = 0; i < n->num_children; i++) {
        uint64_t child_pos;
        leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) +
                                   start_off + 4 + i * 8,
                               &child_pos);
        // printf("4off:%lu child_pos:%lu\n", offset + start_off + 4 + i * 8,
        // child_pos);
        n4->children[i] = (art_node*)((uintptr_t)(offset_map[child_pos]));
      }
    } else if (n->type == NODE16) {
      art_node16* n16 = (art_node16*)n;
      for (int i = 0; i < n->num_children; i++) {
        uint64_t child_pos;
        leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) +
                                   start_off + 16 + i * 8,
                               &child_pos);
        // printf("16off:%lu child_pos:%lu\n", offset + start_off + 16 + i * 8,
        // child_pos);
        n16->children[i] = (art_node*)((uintptr_t)(offset_map[child_pos]));
      }
    } else if (n->type == NODE48) {
      art_node48* n48 = (art_node48*)n;
      int idx;
      for (int i = 0; i < 256; i++) {
        idx = record.data()[start_off + i];
        if (idx > 0) {
          uint64_t child_pos;
          leveldb::GetFixed64Ptr(reinterpret_cast<const char*>(record.data()) +
                                     start_off + 264 + (idx - 1) * 8,
                                 &child_pos);
          // printf("48off:%lu child_pos:%lu p:%lu\n", offset + start_off + 264
          // + (idx - 1) * 8, child_pos, (uintptr_t)(offset_map[child_pos]));
          n48->children[idx - 1] =
              (art_node*)((uintptr_t)(offset_map[child_pos]));
        }
      }
    } else if (n->type == NODE256) {
      art_node256* n256 = (art_node256*)n;
      for (int i = 0; i < 256; i++) {
        uint64_t child_pos;
        leveldb::GetFixed64Ptr(
            reinterpret_cast<const char*>(record.data()) + start_off + i * 8,
            &child_pos);
        if (child_pos > 0) {
          // printf("256off:%lu i:%d child_pos:%lu p:%lu\n", offset + start_off
          // + i * 8, i, child_pos, (uintptr_t)(offset_map[child_pos]));
          n256->children[i] = (art_node*)((uintptr_t)(offset_map[child_pos]));
        } else
          n256->children[i] = NULL;
      }
    } else {
      printf("type error in art_deserialization\n");
      abort();
    }

    if (record_size > 16 * 1024) delete[] buf;
    offset += record_size;
  }

  t->root = (art_node*)((uintptr_t)(offset_map[root_offset]));
  // printf("root:%lu\n", (uintptr_t)(t->root));
}

}  // namespace mem
}  // namespace tsdb