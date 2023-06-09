#ifndef VECTORPOSTINGS_H
#define VECTORPOSTINGS_H

#include <iostream>
#include <vector>

#include "index/PostingsInterface.hpp"

namespace tsdb {
namespace index {

class VectorPostings : public PostingsInterface {
 private:
  std::vector<uint64_t> list;
  mutable int i;

 public:
  VectorPostings() : i(-1) {}
  VectorPostings(int size) : i(-1) { list.reserve(size); }
  VectorPostings(const std::vector<uint64_t>& v) : i(-1), list(v) {}
  VectorPostings(std::vector<uint64_t>&& v) : i(-1), list(std::move(v)) {}

  bool next() const override {
    // NOTICE(Alec), static_cast to int first.
    if (i >= static_cast<int>(list.size()) - 1) return false;
    ++i;
    return true;
  }

  bool seek(uint64_t v) const override {
    if (list.empty() || i >= static_cast<int>(list.size())) return false;
    if (i < 0) i = 0;
    if (list[i] >= v) return true;
    auto it = std::lower_bound(list.begin() + i, list.end(), v);
    if (it == list.end()) {
      i = list.size();
      return false;
    }
    i = it - list.begin();
    return true;
  }

  // next() or seek() must be called first.
  uint64_t at() const override { return list[i]; }

  void push_back(uint64_t t) { list.push_back(t); }

  int size() const { return list.size(); }

  void sort() { std::sort(list.begin(), list.end()); }
  void reset_cursor() { i = -1; }

  size_t mem_size() {
    return sizeof(VectorPostings) + sizeof(uint64_t) * list.size();
  }
};

class VectorPtrPostings : public PostingsInterface {
 private:
  const std::vector<uint64_t>* list;
  mutable int i;

 public:
  VectorPtrPostings(const std::vector<uint64_t>* v) : i(-1), list(v) {}

  bool next() const override {
    if (i >= static_cast<int>(list->size()) - 1) return false;
    ++i;
    return true;
  }

  bool seek(uint64_t v) const override {
    if (list->empty() || i >= static_cast<int>(list->size())) return false;
    if (i < 0) i = 0;
    if (list->at(i) >= v) return true;
    auto it = std::lower_bound(list->begin() + i, list->end(), v);
    if (it == list->end()) {
      i = list->size();
      return false;
    }
    i = it - list->begin();
    return true;
  }

  // next() or seek() must be called first.
  uint64_t at() const override { return list->at(i); }

  int size() const { return list->size(); }
};

class SharedVectorPostings : public PostingsInterface {
 private:
  std::shared_ptr<std::vector<uint64_t>> list;
  mutable int i;

 public:
  SharedVectorPostings() : i(-1) {}
  SharedVectorPostings(const std::shared_ptr<std::vector<uint64_t>>& v)
      : i(-1), list(v) {}

  bool next() const override {
    // NOTICE(Alec), static_cast to int first.
    if (i >= static_cast<int>(list->size()) - 1) return false;
    ++i;
    return true;
  }

  bool seek(uint64_t v) const override {
    if (list->empty() || i >= static_cast<int>(list->size())) return false;
    if (i < 0) i = 0;
    if (list->at(i) >= v) return true;
    auto it = std::lower_bound(list->begin() + i, list->end(), v);
    if (it == list->end()) {
      i = list->size();
      return false;
    }
    i = it - list->begin();
    return true;
  }

  // next() or seek() must be called first.
  uint64_t at() const override { return list->at(i); }

  int size() const { return list->size(); }
};

}  // namespace index
}  // namespace tsdb

#endif