#include "mem/go_art.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/WaitGroup.hpp"
#include "gtest/gtest.h"
#include "label/Label.hpp"
#include "leveldb/env.h"
#include "third_party/thread_pool.h"
#include "util/random.h"
#include "util/testutil.h"

namespace tsdb {
namespace mem {

std::vector<std::vector<std::string>> devops(
    {{"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
      "usage_irq", "usage_softirq", "usage_steal", "usage_guest",
      "usage_guest_nice"},
     {"reads", "writes", "read_bytes", "write_bytes", "read_time", "write_time",
      "io_time"},
     {"total", "free", "used", "used_percent", "inodes_total", "inodes_free",
      "inodes_used"},
     {"boot_time", "interrupts", "context_switches", "processes_forked",
      "disk_pages_in", "disk_pages_out"},
     {"total", "available", "used", "free", "cached", "buffered",
      "used_percent", "available_percent", "buffered_percent"},
     {"bytes_sent", "bytes_recv", "packets_sent", "packets_recv", "err_in",
      "err_out", "drop_in", "drop_out"},
     {"accepts", "active", "handled", "reading", "requests", "waiting",
      "writing"},
     {"numbackends", "xact_commit", "xact_rollback", "blks_read", "blks_hit",
      "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
      "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks",
      "blk_read_time", "blk_write_time"},
     {"uptime_in_seconds",
      "total_connections_received",
      "expired_keys",
      "evicted_keys",
      "keyspace_hits",
      "keyspace_misses",
      "instantaneous_ops_per_sec",
      "instantaneous_input_kbps",
      "instantaneous_output_kbps",
      "connected_clients",
      "used_memory",
      "used_memory_rss",
      "used_memory_peak",
      "used_memory_lua",
      "rdb_changes_since_last_save",
      "sync_full",
      "sync_partial_ok",
      "sync_partial_err",
      "pubsub_channels",
      "pubsub_patterns",
      "latest_fork_usec",
      "connected_slaves",
      "master_repl_offset",
      "repl_backlog_active",
      "repl_backlog_size",
      "repl_backlog_histlen",
      "mem_fragmentation_ratio",
      "used_cpu_sys",
      "used_cpu_user",
      "used_cpu_sys_children",
      "used_cpu_user_children"}});
std::vector<std::string> devops_names({"cpu_", "diskio_", "disk_", "kernel_",
                                       "mem_", "net_", "nginx_", "postgres_",
                                       "redis_"});

std::unordered_map<std::string, bool> query_types({{"1-1-1", true},
                                                   {"1-1-12", true},
                                                   {"1-8-1", true},
                                                   {"5-1-1", true},
                                                   {"5-1-12", true},
                                                   {"5-8-1", true},
                                                   {"double-groupby-1", false},
                                                   {"high-cpu-1", false},
                                                   {"high-cpu-all", false},
                                                   {"lastpoint", true}});

class GoArtTest : public testing::Test {
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
  void load_devops_labels1(std::vector<tsdb::label::Labels>* lsets,
                           int num_ts) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 1;

    std::vector<std::string> items, names, values;
    for (size_t round = 0; round < devops_names.size(); round++) {
      while (cur_line < num_lines) {
        getline(file, line);

        size_t pos_start = 0, pos_end, delim_len = 1;
        std::string token;
        items.clear();
        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
          token = line.substr(pos_start, pos_end - pos_start);
          pos_start = pos_end + delim_len;
          items.push_back(token);
        }
        items.push_back(line.substr(pos_start));

        names.clear();
        values.clear();
        for (size_t i = 1; i < items.size(); i++) {
          pos_end = items[i].find("=");
          names.push_back(items[i].substr(0, pos_end));
          values.push_back(items[i].substr(pos_end + 1));
        }

        for (size_t i = 0; i < devops[round].size(); i++) {
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          lsets->push_back(std::move(lset));

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }
};

TEST_F(GoArtTest, Test1) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
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

TEST_F(GoArtTest, Test2) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
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

TEST_F(GoArtTest, Test3) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
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

TEST_F(GoArtTest, Test4) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
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

TEST_F(GoArtTest, Test5) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
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

TEST_F(GoArtTest, Test6) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
  art_tree tree;
  art_tree_init(&tree);

  int num = 20;
  leveldb::Random rnd(1996);
  std::set<std::string> dataset;
  while (dataset.size() < num) {
    std::string s;
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &s);
    dataset.insert(s);
  }

  std::vector<std::string> vec({"hellofkworld1", "hellofkworld9",
                                "hellofkworldgdb", "hellofkworldgcc",
                                "hellofkworldg++", "hellofkworldrng",
                                "hellofkworldabc", "hello", "helly", "kkp"});
  for (auto& s : vec) dataset.insert(s);

  for (auto& s : dataset) {
    art_insert(&tree, reinterpret_cast<const unsigned char*>(s.data()),
               s.size(), (void*)((uintptr_t)(1)));
    // std::cout << "\"" << s << "\"" << "," << std::endl;
  }

  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    // reinterpret_cast<std::atomic<int>*>(data)->fetch_add(1);
    reinterpret_cast<std::vector<std::string>*>(data)->emplace_back(
        reinterpret_cast<const char*>(key), key_len);
    return false;
  };

  std::vector<std::string> results;
  art_prefix(&tree, reinterpret_cast<const unsigned char*>("hellofkworld"), 12,
             cb, (void*)(&results));
  ASSERT_EQ(results, std::vector<std::string>({
                         "hellofkworld1",
                         "hellofkworld9",
                         "hellofkworldabc",
                         "hellofkworldg++",
                         "hellofkworldgcc",
                         "hellofkworldgdb",
                         "hellofkworldrng",
                     }));

  results.clear();
  art_range(&tree, reinterpret_cast<const unsigned char*>("hellofkworldg"), 13,
            reinterpret_cast<const unsigned char*>("hellofkworldh"), 13, false,
            false, cb, (void*)(&results));
  ASSERT_EQ(results, std::vector<std::string>({
                         "hellofkworldg++",
                         "hellofkworldgcc",
                         "hellofkworldgdb",
                     }));

  results.clear();
  art_range(&tree, reinterpret_cast<const unsigned char*>("hellofkworldg++"),
            15, reinterpret_cast<const unsigned char*>("hellofkworldgdb"), 15,
            false, true, cb, (void*)(&results));
  ASSERT_EQ(results, std::vector<std::string>({
                         "hellofkworldgcc",
                         "hellofkworldgdb",
                     }));

  results.clear();
  art_prefix(&tree, reinterpret_cast<const unsigned char*>("kkp"), 3, cb,
             (void*)(&results));
  ASSERT_EQ(results, std::vector<std::string>({
                         "kkp",
                     }));
}

TEST_F(GoArtTest, Test7) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
  art_tree tree;
  art_tree_init(&tree);

  std::vector<std::string> vec({
      "$qs2qh}$l'ip+_U+CjW>zi",
      "/@dQ-s2Ue9UD8uoetW&wot",
      "9F4QV^zF}&?'4xGp6Vuzx({DZoa",
      ":tkD]xF)<3",
      ">j",
      "CtF)",
      "hello",
      "hellofkworld1",
      "hellofkworld9",
      "hellofkworldabc",
      "hellofkworldg++",
      "hellofkworldgcc",
      "hellofkworldgdb",
      "hellofkworldrng",
  });
  for (auto& s : vec) {
    art_insert(&tree, reinterpret_cast<const unsigned char*>(s.data()),
               s.size(), (void*)((uintptr_t)(1)));
    // std::cout << "\"" << s << "\"" << "," << std::endl;
  }

  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    // reinterpret_cast<std::atomic<int>*>(data)->fetch_add(1);
    reinterpret_cast<std::vector<std::string>*>(data)->emplace_back(
        reinterpret_cast<const char*>(key), key_len);
    return false;
  };

  std::vector<std::string> results;
  art_prefix(&tree, reinterpret_cast<const unsigned char*>("hellofkworld"), 12,
             cb, (void*)(&results));
  ASSERT_EQ(results, std::vector<std::string>({
                         "hellofkworld1",
                         "hellofkworld9",
                         "hellofkworldabc",
                         "hellofkworldg++",
                         "hellofkworldgcc",
                         "hellofkworldgdb",
                         "hellofkworldrng",
                     }));

  results.clear();
  unsigned char l[1] = {0};
  unsigned char r[1] = {255};
  art_range(&tree, l, 1, r, 1, true, true, cb, (void*)(&results));
  ASSERT_EQ(results, vec);
}

void insert_and_prefix(art_tree* tree, int num, std::vector<std::string>* vec,
                       int seed) {
  leveldb::Random rnd(seed);
  for (int i = 0; i < num / 2; i++) {
    vec->emplace_back();
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &vec->back());
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    reinterpret_cast<std::vector<std::string>*>(data)->emplace_back(
        reinterpret_cast<const char*>(key), key_len);
    return false;
  };

  std::vector<std::string> results;
  art_prefix(tree, reinterpret_cast<const unsigned char*>("hellofkworld"), 12,
             cb, (void*)(&results));
  ASSERT_EQ(results, std::vector<std::string>({
                         "hellofkworld1",
                         "hellofkworld9",
                         "hellofkworldabc",
                         "hellofkworldg++",
                         "hellofkworldgcc",
                         "hellofkworldgdb",
                         "hellofkworldrng",
                     }));
  for (int i = 0; i < num / 2; i++) {
    vec->emplace_back();
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &vec->back());
    art_insert(tree, reinterpret_cast<const unsigned char*>(vec->back().data()),
               vec->back().size(), (void*)((uintptr_t)(1)));
  }
}

TEST_F(GoArtTest, Test8) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
  art_tree tree;
  art_tree_init(&tree);

  std::vector<std::string> vec({
      "$qs2qh}$l'ip+_U+CjW>zi",
      "/@dQ-s2Ue9UD8uoetW&wot",
      "9F4QV^zF}&?'4xGp6Vuzx({DZoa",
      ":tkD]xF)<3",
      ">j",
      "CtF)",
      "hello",
      "hellofkworld1",
      "hellofkworld9",
      "hellofkworldabc",
      "hellofkworldg++",
      "hellofkworldgcc",
      "hellofkworldgdb",
      "hellofkworldrng",
  });
  for (auto& s : vec)
    art_insert(&tree, reinterpret_cast<const unsigned char*>(s.data()),
               s.size(), (void*)((uintptr_t)(1)));

  std::vector<std::string> v1, v2, v3, v4;
  std::thread t1(insert_and_prefix, &tree, 100000, &v1, 1);
  std::thread t2(insert_and_prefix, &tree, 100000, &v2, 2);
  std::thread t3(insert_and_prefix, &tree, 100000, &v3, 3);
  std::thread t4(insert_and_prefix, &tree, 100000, &v4, 4);
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

TEST_F(GoArtTest, TestSerilization1) {
// #ifdef MMAP_ART_NODE
//   reset_mmap_art_nodes();
// #endif
  boost::filesystem::remove("/tmp/snapshot");
  int num = 100000;
  leveldb::Random rnd(1996);
  std::unordered_set<std::string> dataset;
  while (dataset.size() < num) {
    std::string s;
    leveldb::test::RandomString(&rnd, rnd.Uniform(30), &s);
    dataset.insert(s);
  }

  {
    art_tree tree;
    art_tree_init(&tree);
    for (auto& s : dataset)
      art_insert(&tree, reinterpret_cast<const unsigned char*>(s.data()),
                 s.size(), (void*)((uintptr_t)(1)));

    leveldb::WritableFile* f;
    leveldb::RandomRWFile* rwf;
    leveldb::Env::Default()->NewWritableFile("/tmp/snapshot", &f);
    leveldb::Env::Default()->NewRandomRWFile("/tmp/snapshot", &rwf);
    art_serialization(&tree, f, rwf);
  }

  {
    art_tree tree;
    leveldb::RandomAccessFile* f;
    leveldb::Env::Default()->NewRandomAccessFile("/tmp/snapshot", &f);
    art_deserialization(&tree, f);

    for (auto& s : dataset) {
      void* value = art_search(
          &tree, reinterpret_cast<const unsigned char*>(s.data()), s.size());
      ASSERT_EQ((void*)((uintptr_t)(1)), value);
    }
  }
}

}  // namespace mem
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}