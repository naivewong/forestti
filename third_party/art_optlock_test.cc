#include "third_party/art_optlock.h"

#include <iostream>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "util/random.h"
#include "util/testutil.h"

namespace artoptlock {

class ARTOptLockTest : public testing::Test {};

TEST_F(ARTOptLockTest, Test1) {
  art_tree tree;
  art_tree_init(&tree);
  art_insert(&tree, reinterpret_cast<const unsigned char*>("hello"), 5,
             (void*)((uintptr_t)(1)));
  art_insert(&tree, reinterpret_cast<const unsigned char*>("helly"), 5,
             (void*)((uintptr_t)(2)));
  void* value =
      art_search(&tree, reinterpret_cast<const unsigned char*>("hell"), 4);
  ASSERT_EQ(NULL, value);
  value = art_search(&tree, reinterpret_cast<const unsigned char*>("hello"), 5);
  ASSERT_EQ((void*)((uintptr_t)(1)), value);
  value = art_search(&tree, reinterpret_cast<const unsigned char*>("helly"), 5);
  ASSERT_EQ((void*)((uintptr_t)(2)), value);
}

void insert(art_tree* tree, int num, std::vector<std::string>* vec, int seed) {
  leveldb::Random rnd(seed);
  for (int i = 0; i < num; i++) {
    vec->emplace_back();
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &vec->back());
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
}

TEST_F(ARTOptLockTest, Test2) {
  art_tree tree;
  art_tree_init(&tree);

  std::vector<std::string> v1, v2, v3, v4;
  std::thread t1(insert, &tree, 100, &v1, 1);
  std::thread t2(insert, &tree, 100, &v2, 2);
  std::thread t3(insert, &tree, 100, &v3, 3);
  std::thread t4(insert, &tree, 100, &v4, 4);
  t1.join();
  t2.join();
  t3.join();
  t4.join();

  // insert(&tree, 10, &v1);
  for (size_t i = 0; i < v1.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v1[i].data()),
                   v1[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v2.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v2[i].data()),
                   v2[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v3.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v3[i].data()),
                   v3[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v4.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v4[i].data()),
                   v4[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  art_tree_destroy(&tree);
}

void insert_and_read(art_tree* tree, int num, std::vector<std::string>* vec,
                     int seed) {
  leveldb::Random rnd(seed);
  for (int i = 0; i < num / 2; i++) {
    vec->emplace_back();
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &vec->back());
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
  for (int i = 0; i < num / 2; i++) {
    void* value = art_search(
        tree, reinterpret_cast<const unsigned char*>(vec->at(i).data()),
        vec->at(i).size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (int i = 0; i < num / 2; i++) {
    vec->emplace_back();
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &vec->back());
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
  for (int i = 0; i < num; i++) {
    void* value = art_search(
        tree, reinterpret_cast<const unsigned char*>(vec->at(i).data()),
        vec->at(i).size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
}

TEST_F(ARTOptLockTest, Test3) {
  art_tree tree;
  art_tree_init(&tree);

  std::vector<std::string> v1, v2, v3, v4;
  std::thread t1(insert_and_read, &tree, 100000, &v1, 1);
  std::thread t2(insert_and_read, &tree, 100000, &v2, 2);
  std::thread t3(insert_and_read, &tree, 100000, &v3, 3);
  std::thread t4(insert_and_read, &tree, 100000, &v4, 4);
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  for (size_t i = 0; i < v1.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v1[i].data()),
                   v1[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v2.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v2[i].data()),
                   v2[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v3.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v3[i].data()),
                   v3[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v4.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v4[i].data()),
                   v4[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  art_tree_destroy(&tree);
}

TEST_F(ARTOptLockTest, Test4) {
  art_tree tree;
  art_tree_init(&tree);
  art_insert(&tree, reinterpret_cast<const unsigned char*>("hello"), 5,
             (void*)((uintptr_t)(1)));
  art_insert(&tree, reinterpret_cast<const unsigned char*>("helly"), 5,
             (void*)((uintptr_t)(2)));
  void* value =
      art_search(&tree, reinterpret_cast<const unsigned char*>("hell"), 4);
  ASSERT_EQ(NULL, value);
  value = art_search(&tree, reinterpret_cast<const unsigned char*>("hello"), 5);
  ASSERT_EQ((void*)((uintptr_t)(1)), value);
  value = art_search(&tree, reinterpret_cast<const unsigned char*>("helly"), 5);
  ASSERT_EQ((void*)((uintptr_t)(2)), value);

  value = art_delete(&tree, reinterpret_cast<const unsigned char*>("hello"), 5);
  ASSERT_EQ((void*)((uintptr_t)(1)), value);
  value = art_delete(&tree, reinterpret_cast<const unsigned char*>("helly"), 5);
  ASSERT_EQ((void*)((uintptr_t)(2)), value);
  art_tree_destroy(&tree);
}

void insert_and_search_and_delete(art_tree* tree, int num,
                                  std::vector<std::string>* vec,
                                  const std::vector<std::string>& data) {
  for (int i = 0; i < num / 2; i++) {
    vec->push_back(data[i]);
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
  for (int i = 0; i < num / 2; i++) {
    void* value = art_search(
        tree, reinterpret_cast<const unsigned char*>(vec->at(i).data()),
        vec->at(i).size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (int i = 0; i < num / 2; i++) {
    void* value = art_delete(
        tree, reinterpret_cast<const unsigned char*>(vec->at(i).data()),
        vec->at(i).size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  vec->clear();
  for (int i = 0; i < num / 2; i++) {
    vec->push_back(data[i + num / 2]);
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
  for (int i = 0; i < vec->size(); i++) {
    void* value = art_search(
        tree, reinterpret_cast<const unsigned char*>(vec->at(i).data()),
        vec->at(i).size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
}

TEST_F(ARTOptLockTest, Test5) {
  art_tree tree;
  art_tree_init(&tree);

  std::vector<std::string> v1, v2, v3, v4, in1, in2, in3, in4;
  int num = 100000;
  leveldb::Random rnd(1996);
  std::unordered_set<std::string> dataset;
  while (dataset.size() < num * 4) {
    std::string s;
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &s);
    dataset.insert(s);
  }

  for (const std::string& str : dataset) {
    if (in1.size() < num) {
      in1.push_back(str);
      continue;
    }
    if (in2.size() < num) {
      in2.push_back(str);
      continue;
    }
    if (in3.size() < num) {
      in3.push_back(str);
      continue;
    }
    if (in4.size() < num) {
      in4.push_back(str);
      continue;
    }
  }
  std::thread t1(insert_and_search_and_delete, &tree, num, &v1, in1);
  std::thread t2(insert_and_search_and_delete, &tree, num, &v2, in2);
  std::thread t3(insert_and_search_and_delete, &tree, num, &v3, in3);
  std::thread t4(insert_and_search_and_delete, &tree, num, &v4, in4);
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  for (size_t i = 0; i < v1.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v1[i].data()),
                   v1[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v2.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v2[i].data()),
                   v2[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v3.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v3[i].data()),
                   v3[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  for (size_t i = 0; i < v4.size(); i++) {
    void* value =
        art_search(&tree, reinterpret_cast<const unsigned char*>(v4[i].data()),
                   v4[i].size());
    ASSERT_EQ((void*)((uintptr_t)(1)), value);
  }
  art_tree_destroy(&tree);
}

}  // namespace artoptlock

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}