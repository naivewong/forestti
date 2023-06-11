#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "index/PostingsInterface.hpp"
#include "util/coding.h"

namespace tsdb {
namespace index {

class PrefixPostings : public PostingsInterface {
 private:
  std::vector<std::string> blocks;
  mutable int cur_block;
  mutable int cur_idx;

  uint64_t prefix_postings_block_id(const std::string& s) const {
    return (((uint64_t)((uint8_t)(s[0]))) << 56) |
           (((uint64_t)((uint8_t)(s[1]))) << 48) |
           (((uint64_t)((uint8_t)(s[2]))) << 40) |
           (((uint64_t)((uint8_t)(s[3]))) << 32) |
           (((uint64_t)((uint8_t)(s[4]))) << 24) |
           (((uint64_t)((uint8_t)(s[5]))) << 16);
  }

  uint64_t prefix_postings_offset(int i, const std::string& s) const {
    return (((uint64_t)((uint8_t)(s[6 + i * 2]))) << 8) |
           (uint64_t)((uint8_t)(s[7 + i * 2]));
  }

  int search_inside_block(int b, uint64_t value) const {
    int count = (blocks[b].size() - 6) / 2, step, it, first = 0;

    while (count > 0) {
      it = first;
      step = count / 2;
      it += step;
      if (prefix_postings_offset(it, blocks[b]) < value) {
        first = ++it;
        count -= step + 1;
      } else
        count = step;
    }
    return first;
  }

 public:
  PrefixPostings() : cur_block(-1), cur_idx(-1) {}

  void insert(uint64_t id) {
    auto it = std::lower_bound(blocks.begin(), blocks.end(), id,
                               [&](const std::string& s, uint64_t v) {
                                 return this->prefix_postings_block_id(s) <
                                        (v & 0xffffffffffff0000);
                               });
    if (it != blocks.end()) {
      if (prefix_postings_block_id(*it) == (id & 0xffffffffffff0000)) {
        int idx =
            search_inside_block(it - blocks.begin(), id & 0x000000000000ffff);
        if (idx == (it->size() - 6) / 2) {
          it->push_back((id & 0x000000000000ffff) >> 8);
          it->push_back(id & 0x00000000000000ff);
        } else {
          char v[2] = {(char)((id & 0x000000000000ffff) >> 8),
                       (char)(id & 0x00000000000000ff)};
          it->insert(6 + idx * 2, v, 2);
        }
      } else {
        std::string s;
        leveldb::PutFixed64BE(&s, id);
        blocks.insert(it, std::move(s));
      }
    } else {
      std::string s;
      leveldb::PutFixed64BE(&s, id);
      blocks.insert(it, std::move(s));
    }
  }

  void reset_cursor() {
    cur_block = -1;
    cur_idx = -1;
  }

  bool next() const override {
    if (cur_block == -1) {
      cur_block = 0;
      cur_idx = 0;
      if (blocks.empty()) return false;
      return true;
    }

    ++cur_idx;
    if (cur_idx == (blocks[cur_block].size() - 6) / 2) {
      if (cur_block == blocks.size() - 1) return false;
      ++cur_block;
      cur_idx = 0;
    }
    return true;
  }

  bool seek(uint64_t val) const override {
    if (cur_block == -1) {
      cur_block = 0;
      cur_idx = 0;
      if (blocks.empty()) return false;
      return true;
    }

    if (at() >= val) return true;

    auto it = std::lower_bound(blocks.begin(), blocks.end(), val,
                               [&](const std::string& s, uint64_t v) {
                                 return this->prefix_postings_block_id(s) <
                                        (v & 0xffffffffffff0000);
                               });

    if (it == blocks.end()) return false;

    int b = it - blocks.begin();
    if (prefix_postings_block_id(*it) != (val & 0xffffffffffff0000)) {
      cur_block = b;
      cur_idx = 0;
    } else {
      int idx = search_inside_block(b, val & 0x000000000000ffff);

      if (idx == (blocks[b].size() - 6) / 2) {
        cur_block = b + 1;
        cur_idx = 0;
      } else {
        cur_block = b;
        cur_idx = idx;
      }
      if (cur_block == blocks.size()) return false;
    }
    return true;
  }

  uint64_t at() const override {
    return prefix_postings_block_id(blocks[cur_block]) |
           prefix_postings_offset(cur_idx, blocks[cur_block]);
  }

  size_t mem_size() {
    size_t s = sizeof(PrefixPostings);
    for (size_t i = 0; i < blocks.size(); i++) s += blocks[i].size();
    return s;
  }
};

class PrefixPostingsV2 : public PostingsInterface {
 private:
  std::vector<std::string> blocks;
  std::vector<uint64_t> block_prefixes;
  mutable int cur_block;
  mutable int cur_idx;
  mutable int num;

  uint64_t prefix_postings_offset(int i, const std::string& s) const {
    return (((uint64_t)((uint8_t)(s[i * 2]))) << 8) |
           (uint64_t)((uint8_t)(s[1 + i * 2]));
  }

  int search_inside_block(int b, uint64_t value) const {
    int count = blocks[b].size() / 2, step, it, first = 0;

    while (count > 0) {
      it = first;
      step = count / 2;
      it += step;
      if (prefix_postings_offset(it, blocks[b]) < value) {
        first = ++it;
        count -= step + 1;
      } else
        count = step;
    }
    return first;
  }

 public:
  PrefixPostingsV2() : cur_block(-1), cur_idx(-1), num(0) {}

  void insert(uint64_t id) {
    auto it = std::lower_bound(
        block_prefixes.begin(), block_prefixes.end(), id,
        [&](uint64_t s, uint64_t v) { return s < (v & 0xffffffffffff0000); });
    if (it != block_prefixes.end()) {
      if (*it == (id & 0xffffffffffff0000)) {
        int block_idx = it - block_prefixes.begin();
        int idx = search_inside_block(block_idx, id & 0x000000000000ffff);
        if (idx == blocks[block_idx].size() / 2) {
          blocks[block_idx].push_back((id & 0x000000000000ffff) >> 8);
          blocks[block_idx].push_back(id & 0x00000000000000ff);
        } else {
          char v[2] = {(char)((id & 0x000000000000ffff) >> 8),
                       (char)(id & 0x00000000000000ff)};
          // NOTE(Alec): deduplicate here.
          if (v[0] != blocks[block_idx][idx * 2] || v[1] != blocks[block_idx][idx * 2 + 1])
            blocks[block_idx].insert(idx * 2, v, 2);
          else
            return;
        }
      } else {
        block_prefixes.push_back(id & 0xffffffffffff0000);
        std::string s;
        s.push_back((id & 0x000000000000ffff) >> 8);
        s.push_back(id & 0x00000000000000ff);
        blocks.push_back(std::move(s));
      }
    } else {
      block_prefixes.push_back(id & 0xffffffffffff0000);
      std::string s;
      s.push_back((id & 0x000000000000ffff) >> 8);
      s.push_back(id & 0x00000000000000ff);
      blocks.push_back(std::move(s));
    }
    ++num;
  }

  bool remove(uint64_t id) {
    auto it = std::lower_bound(
        block_prefixes.begin(), block_prefixes.end(), id,
        [&](uint64_t s, uint64_t v) { return s < (v & 0xffffffffffff0000); });
    if (it != block_prefixes.end()) {
      if (*it == (id & 0xffffffffffff0000)) {
        int block_idx = it - block_prefixes.begin();
        int idx = search_inside_block(block_idx, id & 0x000000000000ffff);
        if (idx < blocks[block_idx].size() / 2) {
          if (blocks[block_idx].size() == 2) {
            block_prefixes.erase(block_prefixes.begin() + block_idx);
            blocks.erase(blocks.begin() + block_idx);
          } else
            blocks[block_idx].erase(idx * 2, 2);
          --num;
          return true;
        }
      }
    }
    return false;
  }

  void reset_cursor() const {
    cur_block = -1;
    cur_idx = -1;
  }

  void reset() {
    blocks.clear();
    block_prefixes.clear();
    cur_block = -1;
    cur_idx = -1;
  }

  bool next() const override {
    if (cur_block == -1) {
      cur_block = 0;
      cur_idx = 0;
      if (blocks.empty()) return false;
      return true;
    }

    ++cur_idx;
    if (cur_idx == blocks[cur_block].size() / 2) {
      if (cur_block == blocks.size() - 1) return false;
      ++cur_block;
      cur_idx = 0;
    }
    return true;
  }

  bool seek(uint64_t val) const override {
    if (cur_block == -1) {
      cur_block = 0;
      cur_idx = 0;
      if (blocks.empty()) return false;
      return true;
    }

    if (at() >= val) return true;

    auto it = std::lower_bound(
        block_prefixes.begin(), block_prefixes.end(), val,
        [&](uint64_t s, uint64_t v) { return s < (v & 0xffffffffffff0000); });

    if (it == block_prefixes.end()) return false;

    int b = it - block_prefixes.begin();
    if (*it != (val & 0xffffffffffff0000)) {
      cur_block = b;
      cur_idx = 0;
    } else {
      int idx = search_inside_block(b, val & 0x000000000000ffff);

      if (idx == blocks[b].size() / 2) {
        cur_block = b + 1;
        cur_idx = 0;
      } else {
        cur_block = b;
        cur_idx = idx;
      }
      if (cur_block == blocks.size()) return false;
    }
    return true;
  }

  uint64_t at() const override {
    return block_prefixes[cur_block] |
           prefix_postings_offset(cur_idx, blocks[cur_block]);
  }

  size_t size() const { return num; }

  size_t mem_size() {
    size_t s =
        sizeof(PrefixPostingsV2) + sizeof(uint64_t) * block_prefixes.size();
    for (size_t i = 0; i < blocks.size(); i++) s += blocks[i].size();
    return s;
  }
};

// 256 slots
class PrefixPostingsV3 : public PostingsInterface {
 private:
  std::vector<std::string> blocks;
  std::vector<uint64_t> block_prefixes;
  mutable int cur_block;
  mutable int cur_idx;
  mutable int num;

  uint64_t prefix_postings_offset(int i, const std::string& s) const {
    return ((uint64_t)((uint8_t)(s[i])));
  }

  int search_inside_block(int b, uint64_t value) const {
    int count = blocks[b].size(), step, it, first = 0;

    while (count > 0) {
      it = first;
      step = count / 2;
      it += step;
      if (prefix_postings_offset(it, blocks[b]) < value) {
        first = ++it;
        count -= step + 1;
      } else
        count = step;
    }
    return first;
  }

 public:
  PrefixPostingsV3() : cur_block(-1), cur_idx(-1), num(0) {}

  void insert(uint64_t id) {
    auto it = std::lower_bound(
        block_prefixes.begin(), block_prefixes.end(), id,
        [&](uint64_t s, uint64_t v) { return s < (v & 0xffffffffffffff00); });
    if (it != block_prefixes.end()) {
      if (*it == (id & 0xffffffffffffff00)) {
        int block_idx = it - block_prefixes.begin();
        int idx = search_inside_block(block_idx, id & 0x000000000000ffff);
        if (idx == blocks[block_idx].size()) {
          blocks[block_idx].push_back(id & 0x00000000000000ff);
        } else {
          char v[1] = {(char)(id & 0x00000000000000ff)};
          blocks[block_idx].insert(idx, v, 1);
        }
      } else {
        block_prefixes.push_back(id & 0xffffffffffffff00);
        std::string s;
        s.push_back(id & 0x00000000000000ff);
        blocks.push_back(std::move(s));
      }
    } else {
      block_prefixes.push_back(id & 0xffffffffffffff00);
      std::string s;
      s.push_back(id & 0x00000000000000ff);
      blocks.push_back(std::move(s));
    }
    ++num;
  }

  bool remove(uint64_t id) {
    auto it = std::lower_bound(
        block_prefixes.begin(), block_prefixes.end(), id,
        [&](uint64_t s, uint64_t v) { return s < (v & 0xffffffffffffff00); });
    if (it != block_prefixes.end()) {
      if (*it == (id & 0xffffffffffffff00)) {
        int block_idx = it - block_prefixes.begin();
        int idx = search_inside_block(block_idx, id & 0x00000000000000ff);
        if (idx < blocks[block_idx].size()) {
          if (blocks[block_idx].size() == 1) {
            block_prefixes.erase(block_prefixes.begin() + block_idx);
            blocks.erase(blocks.begin() + block_idx);
          } else
            blocks[block_idx].erase(idx, 1);
          --num;
          return true;
        }
      }
    }
    return false;
  }

  void reset_cursor() const {
    cur_block = -1;
    cur_idx = -1;
  }

  void reset() {
    blocks.clear();
    block_prefixes.clear();
    cur_block = -1;
    cur_idx = -1;
  }

  bool next() const override {
    if (cur_block == -1) {
      cur_block = 0;
      cur_idx = 0;
      if (blocks.empty()) return false;
      return true;
    }

    ++cur_idx;
    if (cur_idx == blocks[cur_block].size()) {
      if (cur_block == blocks.size() - 1) return false;
      ++cur_block;
      cur_idx = 0;
    }
    return true;
  }

  bool seek(uint64_t val) const override {
    if (cur_block == -1) {
      cur_block = 0;
      cur_idx = 0;
      if (blocks.empty()) return false;
      return true;
    }

    if (at() >= val) return true;

    auto it = std::lower_bound(
        block_prefixes.begin(), block_prefixes.end(), val,
        [&](uint64_t s, uint64_t v) { return s < (v & 0xffffffffffffff00); });

    if (it == block_prefixes.end()) return false;

    int b = it - block_prefixes.begin();
    if (*it != (val & 0xffffffffffffff00)) {
      cur_block = b;
      cur_idx = 0;
    } else {
      int idx = search_inside_block(b, val & 0x00000000000000ff);

      if (idx == blocks[b].size()) {
        cur_block = b + 1;
        cur_idx = 0;
      } else {
        cur_block = b;
        cur_idx = idx;
      }
      if (cur_block == blocks.size()) return false;
    }
    return true;
  }

  uint64_t at() const override {
    return block_prefixes[cur_block] |
           prefix_postings_offset(cur_idx, blocks[cur_block]);
  }

  size_t size() const { return num; }

  size_t mem_size() {
    size_t s =
        sizeof(PrefixPostingsV3) + sizeof(uint64_t) * block_prefixes.size();
    for (size_t i = 0; i < blocks.size(); i++) s += blocks[i].size();
    return s;
  }
};

}  // namespace index
}  // namespace tsdb