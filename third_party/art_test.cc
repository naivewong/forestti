#include "third_party/art.h"

#include <iostream>
#include <thread>

#include "base/Mutex.hpp"
#include "gtest/gtest.h"
#include "label/Label.hpp"
#include "leveldb/third_party/thread_pool.h"
#include "util/random.h"
#include "util/testutil.h"

namespace art {

class ARTOptLockTest : public testing::Test {
 public:
  void load_simple_labels1(std::vector<tsdb::label::Labels>* lsets,
                           int num_ts) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lsets->push_back(lset);
    }
  }
  void load_simple_labels2(std::vector<std::string>* lsets, int num_ts) {
    for (int i = 0; i < num_ts; i++) {
      for (int j = 0; j < 10; j++)
        lsets->push_back("label" + std::to_string(j) + "$label" +
                         std::to_string(j) + "_" + std::to_string(i));
    }
  }
};

TEST_F(ARTOptLockTest, Test1) {
  art_tree t;
  art_tree_init(&t);
  std::cout << art_mem_size(&t) << std::endl;

  // one leaf.
  art_insert(&t, reinterpret_cast<const unsigned char*>("hello"), 5,
             (void*)((uintptr_t)(0)));
  std::cout << art_mem_size(&t) << std::endl;

  // two leaves, one node4.
  art_insert(&t, reinterpret_cast<const unsigned char*>("helly"), 5,
             (void*)((uintptr_t)(0)));
  std::cout << art_mem_size(&t) << std::endl;

  art_delete(&t, reinterpret_cast<const unsigned char*>("hello"), 5);
  std::cout << art_mem_size(&t) << std::endl;

  art_delete(&t, reinterpret_cast<const unsigned char*>("helly"), 5);
  std::cout << art_mem_size(&t) << std::endl;

  art_tree_destroy(&t);
}

void test6(art_tree* tree, std::vector<std::string>* lsets,
           tsdb::base::RWMutexLock* mutex_, int begin, int end) {
  for (int i = begin; i < end; i++) {
    tsdb::base::RWLockGuard mutex(*mutex_, 1);
    art_insert(tree,
               reinterpret_cast<const unsigned char*>(lsets->at(i).data()),
               lsets->at(i).size(), (void*)((uintptr_t)(1)));
  }
}

TEST_F(ARTOptLockTest, Test6) {
  art_tree tree;
  art_tree_init(&tree);
  std::vector<std::string> lsets;
  int num = 100000;
  load_simple_labels2(&lsets, num);
  tsdb::base::RWMutexLock mutex_;

  Timer t;
  t.start();
  std::thread t1(test6, &tree, &lsets, &mutex_, 0, lsets.size() / 4);
  std::thread t2(test6, &tree, &lsets, &mutex_, lsets.size() / 4,
                 lsets.size() / 2);
  std::thread t3(test6, &tree, &lsets, &mutex_, lsets.size() / 2,
                 lsets.size() / 4 * 3);
  std::thread t4(test6, &tree, &lsets, &mutex_, lsets.size() / 4 * 3,
                 lsets.size());
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  std::cout << t.since_start_nano() << std::endl;
}

TEST_F(ARTOptLockTest, Test7) {
  art_tree tree;
  art_tree_init(&tree);
  std::vector<std::string> lsets;
  int num = 100000;
  load_simple_labels2(&lsets, num);
  tsdb::base::RWMutexLock mutex_;

  Timer t;
  t.start();
  for (int i = 0; i < lsets.size(); i++) {
    tsdb::base::RWLockGuard mutex(mutex_, 1);
    art_insert(&tree, reinterpret_cast<const unsigned char*>(lsets[i].data()),
               lsets[i].size(), (void*)((uintptr_t)(1)));
  }
  std::cout << t.since_start_nano() << std::endl;
}

TEST_F(ARTOptLockTest, Test8) {
  art_tree tree;
  art_tree_init(&tree);
  std::vector<std::string> lsets;
  int num = 1000000;
  leveldb::Random rnd(1996);
  for (int i = 0; i < num; i++) {
    std::string s;
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &s);
    lsets.push_back(s);
  }
  tsdb::base::RWMutexLock mutex_;

  Timer t;
  t.start();
  for (int i = 0; i < lsets.size(); i++) {
    tsdb::base::RWLockGuard mutex(mutex_, 1);
    art_insert(&tree, reinterpret_cast<const unsigned char*>(lsets[i].data()),
               lsets[i].size(), (void*)((uintptr_t)(1)));
  }
  for (int i = 0; i < lsets.size(); i++) {
    tsdb::base::RWLockGuard mutex(mutex_, 0);
    art_search(&tree, reinterpret_cast<const unsigned char*>(lsets[i].data()),
               lsets[i].size());
  }
  std::cout << t.since_start_nano() << std::endl;
}

void test9(art_tree* tree, std::vector<std::string>* lsets,
           tsdb::base::RWMutexLock* mutex_, int begin, int end) {
  for (int i = begin; i < end; i++) {
    tsdb::base::RWLockGuard mutex(*mutex_, 0);
    art_search(tree,
               reinterpret_cast<const unsigned char*>(lsets->at(i).data()),
               lsets->at(i).size());
  }
}

TEST_F(ARTOptLockTest, Test9) {
  art_tree tree;
  art_tree_init(&tree);
  std::vector<std::string> lsets;
  int num = 1000000;
  leveldb::Random rnd(1996);
  for (int i = 0; i < num; i++) {
    std::string s;
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &s);
    lsets.push_back(s);
  }
  tsdb::base::RWMutexLock mutex_;

  Timer t;
  t.start();
  std::thread t1(test6, &tree, &lsets, &mutex_, 0, lsets.size() / 4);
  std::thread t2(test6, &tree, &lsets, &mutex_, lsets.size() / 4,
                 lsets.size() / 2);
  std::thread t3(test6, &tree, &lsets, &mutex_, lsets.size() / 2,
                 lsets.size() / 4 * 3);
  std::thread t4(test6, &tree, &lsets, &mutex_, lsets.size() / 4 * 3,
                 lsets.size());
  std::thread t5(test9, &tree, &lsets, &mutex_, 0, lsets.size() / 4);
  std::thread t6(test9, &tree, &lsets, &mutex_, lsets.size() / 4,
                 lsets.size() / 2);
  std::thread t7(test9, &tree, &lsets, &mutex_, lsets.size() / 2,
                 lsets.size() / 4 * 3);
  std::thread t8(test9, &tree, &lsets, &mutex_, lsets.size() / 4 * 3,
                 lsets.size());
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  t6.join();
  t7.join();
  t8.join();
  std::cout << t.since_start_nano() << std::endl;
}

}  // namespace art

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}