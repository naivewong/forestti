#include "mem/mem_postings.h"

#include <sys/resource.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <fstream>
#include <unordered_set>

#include "base/WaitGroup.hpp"
#include "gtest/gtest.h"
#include "index/VectorPostings.hpp"
#include "index/EmptyPostings.hpp"
#include "leveldb/third_party/thread_pool.h"
#include "third_party/art.h"
#include "third_party/cedarpp.h"
#include "util/coding.h"

#include "util/testutil.h"

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

void mem_usage(double& vm_usage, double& resident_set) {
  vm_usage = 0.0;
  resident_set = 0.0;
  std::ifstream stat_stream("/proc/self/stat",
                            std::ios_base::in);  // get info from proc directory
  // create some variables to get info
  std::string pid, comm, state, ppid, pgrp, session, tty_nr;
  std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  std::string utime, stime, cutime, cstime, priority, nice;
  std::string O, itrealvalue, starttime;
  unsigned long vsize;
  long rss;
  stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
      tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
      stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
      starttime >> vsize >> rss;  // don't care about the rest
  stat_stream.close();
  long page_size_kb = sysconf(_SC_PAGE_SIZE) /
                      1024;  // for x86-64 is configured to use 2MB pages
  vm_usage = vsize / 1024.0;
  resident_set = rss * page_size_kb;
}

namespace tsdb {
namespace mem {

class MemPostingsTest : public testing::Test {
 public:
  void load_simple_labels1(std::vector<tsdb::label::Labels>* lsets,
                           int num_labels = 10) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lsets->push_back(lset);
    }
  }

  void load_devops_labels1(std::vector<tsdb::label::Labels>* lsets) {
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

  void insert_labels(const std::vector<tsdb::label::Labels>& lsets) {
    for (size_t i = 0; i < lsets.size(); i++) mp.add(i + 1, lsets[i]);
  }

  void delete_labels(const std::vector<tsdb::label::Labels>& lsets) {
    for (size_t i = 0; i < lsets.size(); i++) {
      for (size_t j = 0; j < lsets[i].size(); j++) mp.del(i + 1, lsets[i][j]);
      mp.del(i + 1, label::ALL_POSTINGS_KEYS);
    }
  }

  int num_ts;
  MemPostings mp;
};

// Flat.
TEST_F(MemPostingsTest, Test1) {
  ASSERT_EQ(sizeof(art_node4), mp.mem_size());
  mp.add(1, label::Label("key1", "value1"));
  mp.add(2, label::Label("key1", "value1"));
  std::vector<uint64_t> pids;
  mp.get(label::Label("key1", "value1"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1, 2}), pids);

  uint64_t keys_size = mp.tag_key_size();
  mp.del(1, label::Label("key1", "value1"));
  mp.del(2, label::Label("key1", "value1"));
  printf("keys_size:%lu\n", mp.tag_key_size());
  ASSERT_EQ(keys_size + sizeof(tag_values_node_flat), mp.mem_size());
  ASSERT_EQ(0, mp.postings_size());
}

// Art.
TEST_F(MemPostingsTest, Test2) {
  mp.add(1, label::Label("key1", "value1"));
  mp.add(1, label::Label("key1", "value2"));
  mp.add(1, label::Label("key1", "value3"));
  mp.add(1, label::Label("key1", "value4"));
  mp.add(1, label::Label("key1", "value5"));
  mp.add(1, label::Label("key1", "value6"));
  mp.add(1, label::Label("key1", "value7"));
  mp.add(1, label::Label("key1", "value8"));
  mp.add(1, label::Label("key1", "value9"));
  mp.add(1, label::Label("key1", "value10"));
  mp.add(1, label::Label("key1", "value11"));
  std::vector<uint64_t> pids;
  mp.get(label::Label("key1", "value1"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value2"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value3"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value4"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value5"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value6"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value7"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value8"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value9"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value10"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value11"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);

  uint64_t keys_size = mp.tag_key_size();
  mp.del(1, label::Label("key1", "value1"));
  mp.del(1, label::Label("key1", "value2"));
  mp.del(1, label::Label("key1", "value3"));
  mp.del(1, label::Label("key1", "value4"));
  mp.del(1, label::Label("key1", "value5"));
  mp.del(1, label::Label("key1", "value6"));
  mp.del(1, label::Label("key1", "value7"));
  mp.del(1, label::Label("key1", "value8"));
  mp.del(1, label::Label("key1", "value9"));
  mp.del(1, label::Label("key1", "value10"));
  mp.del(1, label::Label("key1", "value11"));
  ASSERT_EQ(keys_size, mp.mem_size());
  ASSERT_EQ(0, mp.postings_size());
}

TEST_F(MemPostingsTest, Test3) {
  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets);
  insert_labels(lsets);
  std::vector<uint64_t> pids, all_ts;
  mp.get(label::Label("label0", "label0_1192"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1193}), pids);
  pids.clear();
  mp.get(label::ALL_POSTINGS_KEYS, &pids);
  for (int i = 1; i <= num_ts; i++) all_ts.push_back(i);
  ASSERT_EQ(all_ts, pids);

  uint64_t keys_size = mp.tag_key_size();
  delete_labels(lsets);
  ASSERT_EQ(keys_size + sizeof(tag_values_node_flat), mp.mem_size());
  ASSERT_EQ(0, mp.postings_size());
}

TEST_F(MemPostingsTest, Test4) {
  num_ts = 100;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);
  ASSERT_EQ(sizeof(art_node4), mp.mem_size());
  insert_labels(lsets);

  uint64_t keys_size = mp.tag_key_size();
  std::cout << "num_tag_values_nodes: " << mp.num_tag_values_nodes()
            << std::endl;
  std::cout << "mem_size: " << mp.mem_size() << std::endl;
  delete_labels(lsets);
  std::cout << "num_tag_values_nodes: " << mp.num_tag_values_nodes()
            << " key_size:" << mp.tag_key_size() << std::endl;
  // ASSERT_EQ(keys_size, mp.mem_size());
  ASSERT_EQ(0, mp.postings_size());
}

// multi-threads 1.
TEST_F(MemPostingsTest, TestMultiInsert) {
  num_ts = 10000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets);
  ThreadPool pool(4);

  int num_thread = 4;
  for (int i = 0; i < num_thread; i++) {
    pool.enqueue(
        [](MemPostings* p, std::vector<tsdb::label::Labels>* ls, int start,
           int end) {
          for (int j = start; j < end; j++) {
            p->add(j + 1, ls->at(j));
          }
        },
        &mp, &lsets, num_ts * i / num_thread, num_ts * (i + 1) / num_thread);
  }
  pool.wait_barrier();

  uint64_t id = 1;
  std::vector<uint64_t> pids;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      pids.clear();
      mp.get(l, &pids);
      ASSERT_EQ(std::vector<uint64_t>({id}), pids);
    }
    ++id;
  }
}

TEST_F(MemPostingsTest, TestMultiInsertRead1) {
  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets);
  int num_thread = 4;
  ThreadPool pool(num_thread);

  for (int i = 0; i < num_thread; i++) {
    pool.enqueue(
        [](MemPostings* p, std::vector<tsdb::label::Labels>* ls, int start,
           int end) {
          std::vector<uint64_t> pids;
          for (int j = start; j < end; j++) {
            p->add(j + 1, ls->at(j));
            for (const auto& l : ls->at(j)) {
              pids.clear();
              p->get(l, &pids);
              ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(j + 1)}), pids);
            }
          }
        },
        &mp, &lsets, num_ts * i / num_thread, num_ts * (i + 1) / num_thread);
  }
  pool.wait_barrier();

  uint64_t id = 1;
  std::vector<uint64_t> pids;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      pids.clear();
      mp.get(l, &pids);
      ASSERT_EQ(std::vector<uint64_t>({id}), pids);
    }
    ++id;
  }
}

TEST_F(MemPostingsTest, TestMultiInsertRead2) {
  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets, 20);
  ThreadPool pool(4);

  int num_thread = 4;
  for (int i = 0; i < num_thread; i++) {
    pool.enqueue(
        [](MemPostings* p, std::vector<tsdb::label::Labels>* ls, int start,
           int end) {
          std::vector<uint64_t> pids;
          for (int j = start; j < end; j++) {
            p->add(j + 1, ls->at(j));
            for (const auto& l : ls->at(j)) {
              pids.clear();
              p->get(l, &pids);
              ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(j + 1)}), pids);
            }
          }
        },
        &mp, &lsets, num_ts * i / num_thread, num_ts * (i + 1) / num_thread);
  }
  pool.wait_barrier();

  uint64_t id = 1;
  std::vector<uint64_t> pids;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      pids.clear();
      mp.get(l, &pids);
      ASSERT_EQ(std::vector<uint64_t>({id}), pids);
    }
    ++id;
  }
}

TEST_F(MemPostingsTest, TestMultiInsertReadDelete) {
  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets, 20);
  int num_thread = 4;
  ThreadPool pool(num_thread);

  for (int i = 0; i < num_thread; i++) {
    pool.enqueue(
        [](MemPostings* p, std::vector<tsdb::label::Labels>* ls, int start,
           int end) {
          std::vector<uint64_t> pids;
          for (int j = start; j < end; j++) {
            p->add(j + 1, ls->at(j));
            for (const auto& l : ls->at(j)) {
              pids.clear();
              p->get(l, &pids);
              ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(j + 1)}), pids);
            }
            if ((j + 1) % 2 == 0) {
              for (const auto& l : ls->at(j)) p->del(j + 1, l);
            }
          }
          for (int j = start; j < end; j++) {
            for (const auto& l : ls->at(j)) {
              pids.clear();
              p->get(l, &pids);
              if ((j + 1) % 2 == 0)
                ASSERT_TRUE(pids.empty());
              else
                ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(j + 1)}), pids);
            }
          }
        },
        &mp, &lsets, num_ts * i / num_thread, num_ts * (i + 1) / num_thread);
  }
  pool.wait_barrier();

  uint64_t id = 1;
  std::vector<uint64_t> pids;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      pids.clear();
      mp.get(l, &pids);
      if (id % 2 == 0)
        ASSERT_TRUE(pids.empty());
      else
        ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(id)}), pids);
    }
    ++id;
  }
}

TEST_F(MemPostingsTest, GCTest1) {
  boost::filesystem::remove_all("/tmp/gctest");
  mp.add(1, label::Label("key1", "value1"));
  mp.add(2, label::Label("key1", "value1"));
  std::vector<uint64_t> pids;
  mp.get(label::Label("key1", "value1"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1, 2}), pids);

  mp.set_log("/tmp/gctest");

  std::vector<std::string> tn, tv;
  mp.select_postings_for_gc(&tn, &tv, 10);
  ASSERT_EQ(std::vector<std::string>({"key1"}), tn);
  ASSERT_EQ(std::vector<std::string>({"value1"}), tv);

  mp.postings_gc(&tn, &tv);
  pids.clear();
  mp.get(label::Label("key1", "value1"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1, 2}), pids);
}

TEST_F(MemPostingsTest, GCTest2) {
  boost::filesystem::remove_all("/tmp/gctest");
  boost::filesystem::remove_all("/tmp/mergeset");
  mp.add(1, label::Label("key1", "value1"));
  mp.add(1, label::Label("key1", "value2"));
  mp.add(1, label::Label("key1", "value3"));
  mp.add(1, label::Label("key1", "value4"));
  mp.add(1, label::Label("key1", "value5"));
  mp.add(1, label::Label("key1", "value6"));
  mp.add(1, label::Label("key1", "value7"));
  mp.add(1, label::Label("key1", "value8"));
  mp.add(1, label::Label("key1", "value9"));
  mp.add(1, label::Label("key1", "value10"));
  mp.add(1, label::Label("key1", "value11"));

  mp.set_log("/tmp/gctest");
  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.max_file_size = 1024 * 1024;
  mp.set_mergeset_manager("/tmp/mergeset", opts);

  ASSERT_EQ(1, mp.art_gc(10));

  std::vector<uint64_t> pids;
  mp.get(label::Label("key1", "value1"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value2"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value3"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value4"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value5"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value6"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value7"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value8"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value9"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value10"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  pids.clear();
  mp.get(label::Label("key1", "value11"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1}), pids);
}

// GC postings.
TEST_F(MemPostingsTest, TestMultiInsertGC1) {
  boost::filesystem::remove_all("/tmp/gctest");
  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets);
  int num_thread = 4;
  ThreadPool pool(num_thread);

  mp.set_log("/tmp/gctest");

  for (int i = 0; i < num_thread; i++) {
    pool.enqueue(
        [](MemPostings* p, std::vector<tsdb::label::Labels>* ls, int start,
           int end) {
          std::vector<uint64_t> pids;
          std::vector<std::string> tn, tv;
          for (int j = start; j < end; j++) {
            p->add(j + 1, ls->at(j));
            for (const auto& l : ls->at(j)) {
              pids.clear();
              p->get(l, &pids);
              ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(j + 1)}), pids);
            }
            if (start == 0 && (j + 1) % 100 == 0) {
              tn.clear();
              tv.clear();
              p->select_postings_for_gc(&tn, &tv, 10);
              p->postings_gc(&tn, &tv);
            }
          }
        },
        &mp, &lsets, num_ts * i / num_thread, num_ts * (i + 1) / num_thread);
  }
  pool.wait_barrier();

  uint64_t id = 1;
  std::vector<uint64_t> pids;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      pids.clear();
      mp.get(l, &pids);
      ASSERT_EQ(std::vector<uint64_t>({id}), pids);
    }
    ++id;
  }
}

// GC postings + art nodes.
TEST_F(MemPostingsTest, TestMultiInsertGC2) {
  boost::filesystem::remove_all("/tmp/gctest");
  boost::filesystem::remove_all("/tmp/mergeset");
  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets);
  int num_thread = 4;
  ThreadPool pool(num_thread);

  mp.set_log("/tmp/gctest");
  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.max_file_size = 1024 * 1024;
  mp.set_mergeset_manager("/tmp/mergeset", opts);

  for (int i = 0; i < num_thread; i++) {
    pool.enqueue(
        [](MemPostings* p, std::vector<tsdb::label::Labels>* ls, int start,
           int end) {
          std::vector<uint64_t> pids;
          std::vector<std::string> tn, tv;
          for (int j = start; j < end; j++) {
            p->add(j + 1, ls->at(j));
            for (const auto& l : ls->at(j)) {
              pids.clear();
              p->get(l, &pids);
              ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(j + 1)}), pids);
            }
            if (start == 0 && (j + 1) % 100 == 0) {
              tn.clear();
              tv.clear();
              p->select_postings_for_gc(&tn, &tv, 10);
              p->postings_gc(&tn, &tv);
              p->art_gc(10);
            }
          }
        },
        &mp, &lsets, num_ts * i / num_thread, num_ts * (i + 1) / num_thread);
  }
  pool.wait_barrier();

  uint64_t id = 1;
  std::vector<uint64_t> pids;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      pids.clear();
      mp.get(l, &pids);
      ASSERT_EQ(std::vector<uint64_t>({id}), pids);
    }
    ++id;
  }
}

TEST_F(MemPostingsTest, SnapshotTest1) {
  boost::filesystem::remove_all("/tmp/snapshot");
  boost::filesystem::remove_all("/tmp/mtest");

  std::vector<label::Label> lsets;
  for (int i = 1; i < 12; i++) {
    lsets.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
    mp.add(1, lsets.back());
  }
  mp.add(2, label::Label("key2", "value12"));

  mp.set_log("/tmp/mtest");
  std::vector<std::string> gc_names({"key1", "key2"});
  std::vector<std::string> gc_values({"value1", "value12"});
  mp.postings_gc(&gc_names, &gc_values);

  mp.snapshot_index("/tmp/snapshot");
  MemPostings another;
  another.recover_from_snapshot("/tmp/snapshot", "/tmp/snapshot");

  for (int i = 1; i < 12; i++) {
    std::vector<uint64_t> pids;
    another.get(
        label::Label("key" + std::to_string(i), "value" + std::to_string(i)),
        &pids);
    ASSERT_EQ(std::vector<uint64_t>({1}), pids);
  }
  std::vector<uint64_t> pids;
  another.get(label::Label("key2", "value12"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({2}), pids);
}

TEST_F(MemPostingsTest, SnapshotTest2) {
  boost::filesystem::remove_all("/tmp/snapshot");

  num_ts = 100000;
  std::vector<tsdb::label::Labels> lsets;
  load_simple_labels1(&lsets);
  insert_labels(lsets);
  std::vector<uint64_t> pids, all_ts;
  mp.get(label::Label("label0", "label0_1192"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1193}), pids);
  pids.clear();
  mp.get(label::ALL_POSTINGS_KEYS, &pids);
  for (int i = 1; i <= num_ts; i++) all_ts.push_back(i);
  ASSERT_EQ(all_ts, pids);

  mp.snapshot_index("/tmp/snapshot");
  MemPostings another;
  another.recover_from_snapshot("/tmp/snapshot", "/tmp/snapshot");

  pids.clear();
  another.get(label::Label("label0", "label0_1192"), &pids);
  ASSERT_EQ(std::vector<uint64_t>({1193}), pids);
  pids.clear();
  another.get(label::ALL_POSTINGS_KEYS, &pids);
  ASSERT_EQ(all_ts, pids);
}

class MockIndirection {
 public:
  std::vector<std::vector<std::atomic<uint64_t>>> blocks_;
  std::vector<std::atomic<uint64_t>> block_counters_;
  std::atomic<uint64_t> num_slots_;
  std::vector<int> available_blocks_;

  MockIndirection() : block_counters_(100000), num_slots_(0) {
    blocks_.reserve(100000);
    std::vector<std::atomic<uint64_t>> v(256);
    block_counters_[blocks_.size()] = 0;
    blocks_.push_back(std::move(v));
    available_blocks_.push_back(0);
  }

  uint64_t seq_alloc_slot() {
    if (block_counters_[blocks_.size() - 1] >= 256) {
      std::vector<std::atomic<uint64_t>> v(256);
      block_counters_[blocks_.size()] = 0;
      blocks_.push_back(std::move(v));
    }

    uint64_t idx = (((uint64_t)(blocks_.size() - 1)) << 8) |
                   (block_counters_[blocks_.size() - 1]++);
    ++num_slots_;
    return idx;
  }

  uint64_t random_alloc() {
    if (num_slots_ > 0.9 * 256 * blocks_.size()) {
      available_blocks_.push_back(blocks_.size());
      std::vector<std::atomic<uint64_t>> v(256);
      block_counters_[blocks_.size()] = 0;
      blocks_.push_back(std::move(v));
    }
    int block = rand() % available_blocks_.size();
    uint64_t idx = (((uint64_t)(available_blocks_[block])) << 8) |
                   block_counters_[available_blocks_[block]]++;
    if (block_counters_[available_blocks_[block]] == 256)
      available_blocks_.erase(available_blocks_.begin() + block);
    ++num_slots_;
    return idx;
  }

  uint64_t specific_alloc(int block) {
    if (block_counters_[block] >= 256) {
      return random_alloc();
    }
    if (num_slots_ > 0.9 * 256 * blocks_.size()) {
      available_blocks_.push_back(blocks_.size());
      std::vector<std::atomic<uint64_t>> v(256);
      block_counters_[blocks_.size()] = 0;
      blocks_.push_back(std::move(v));
    }
    uint64_t idx = (((uint64_t)(block)) << 8) | block_counters_[block]++;
    if (block_counters_[block] == 256)
      available_blocks_.erase(std::lower_bound(available_blocks_.begin(),
                                               available_blocks_.end(), block));

    ++num_slots_;
    return idx;
  }
};

TEST_F(MemPostingsTest, TestGroupID) {
  struct rusage r_usage;
  double vm, rss;
  num_ts = 100000;
  // std::vector<label::Labels> lsets;
  // load_devops_labels1(&lsets);

  // std::unordered_map<std::string, int> count;
  // for (auto& lset : lsets) {
  //   for (auto& l : lset) {
  //     if (count.count(l.label + "#" + l.value) == 0)
  //       count[l.label + "#" + l.value] = 1;
  //     else
  //       count[l.label + "#" + l.value] += 1;
  //   }
  // }

  // std::vector<std::pair<std::string, int>> count_vec(count.begin(),
  // count.end()); std::sort(count_vec.begin(), count_vec.end(), [](const
  // std::pair<std::string, int>& l, const std::pair<std::string, int>& r){
  //   return l.second > r.second;
  // });
  // for (int i = 0; i < 200; i++)
  //   printf("%s %d\n", count_vec[i].first.c_str(), count_vec[i].second);
  {
    std::vector<label::Labels> lsets;
    load_devops_labels1(&lsets);
    std::unordered_map<std::string, index::PrefixPostingsV3> pl;
    int id = 0;
    for (auto& lset : lsets) {
      for (auto& l : lset) {
        if (pl.count(l.label + "#" + l.value) == 0)
          pl[l.label + "#" + l.value] = index::PrefixPostingsV3();
        pl[l.label + "#" + l.value].insert(id);
      }
      id++;
    }
    int s = 0;
    for (auto& p : pl) s += p.second.mem_size();
    printf("size:%d\n", s);
  }

  {
    std::vector<label::Labels> lsets;
    load_devops_labels1(&lsets);
    std::unordered_map<std::string, index::PrefixPostingsV3> pl;
    int id = 0;
    while (!lsets.empty()) {
      int idx = rand() % lsets.size();
      for (auto& l : lsets[idx]) {
        if (pl.count(l.label + "#" + l.value) == 0)
          pl[l.label + "#" + l.value] = index::PrefixPostingsV3();
        pl[l.label + "#" + l.value].insert(id);
      }
      id++;
      lsets.erase(lsets.begin() + idx);
    }
    int s = 0;
    for (auto& p : pl) s += p.second.mem_size();
    printf("size:%d\n", s);
  }

  {
    std::vector<label::Labels> lsets;
    load_devops_labels1(&lsets);
    std::unordered_map<std::string, index::PrefixPostingsV3> pl;
    int id = 0;
    for (auto& lset : lsets) {
      bool f = false;
      for (auto& l : lset) {
        if (l.label == "arch" && l.value == "x86") {
          f = true;
          break;
        }
      }
      if (f) {
        for (auto& l : lset) {
          if (pl.count(l.label + "#" + l.value) == 0)
            pl[l.label + "#" + l.value] = index::PrefixPostingsV3();
          pl[l.label + "#" + l.value].insert(id);
        }
        id++;
        lsets.erase(lsets.begin() + id);
      }
    }
    for (auto& lset : lsets) {
      for (auto& l : lset) {
        if (pl.count(l.label + "#" + l.value) == 0)
          pl[l.label + "#" + l.value] = index::PrefixPostingsV3();
        pl[l.label + "#" + l.value].insert(id);
      }
      id++;
    }
    int s = 0;
    for (auto& p : pl) s += p.second.mem_size();
    printf("size:%d\n", s);
  }
}

std::string RandomString(leveldb::Random* rnd, int len) {
  std::string r;
  leveldb::test::RandomString(rnd, len, &r);
  return r;
}

void load_random_strings(std::vector<tsdb::label::Labels>* lsets, int num) {
  leveldb::Random rnd(110);
  for (int i = 0; i < num; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++) {
      lset.emplace_back("l", RandomString(&rnd, 10));
    }
    lsets->push_back(lset);
  }
}

void _test_flat_add(tag_values_node_flat* node, uint64_t addr, const std::string& s) {
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

TEST_F(MemPostingsTest, FlatTrieConversionPoint1) {
  num_ts = 10000;
  std::vector<label::Labels> lsets;
  load_simple_labels1(&lsets);
  std::unordered_set<std::string> values;
  for (auto& lset : lsets) {
    for (auto& l : lset) {
      values.insert(l.value);
    }
  }
  std::vector<std::string> v;
  for (auto& s : values)
    v.push_back(s);

  printf("num values: %d\n", v.size());

  for (int i = 1; i < 20; i++) {
    int num = v.size() / i;
    int64_t total_duration = 0;
    int total_size = 0;
    for (int j = 0; j < num * i; j += i) {
      tag_values_node_flat node;
      Timer timer;
      timer.start();
      for (int k = j; k < j + i; k++) {
        _test_flat_add(&node, k, v[k]);
      }
      total_duration += timer.since_start_nano();
      total_size += node.size();
    }
    printf("group size: %d, duration(ns): %d, size: %d\n", i, total_duration / num, total_size / num);
  }
}

bool _test_art_add(tag_values_node_art* node, uint64_t id,
                           const std::string& s) {
  uint64_t addr;
  uint32_t version;

  if (!read_lock_or_restart(&node->node_, &version)) return false;

  if (!upgrade_to_write_lock_or_restart(&node->node_, version)) return false;

  ART_NS::art_insert(
      &node->tree_, reinterpret_cast<const unsigned char*>(s.c_str()),
      s.size(), nullptr);

  write_unlock(&node->node_);
  return true;
}

TEST_F(MemPostingsTest, FlatTrieConversionPoint2) {
  num_ts = 10000;
  std::vector<label::Labels> lsets;
  load_simple_labels1(&lsets);
  std::unordered_set<std::string> values;
  for (auto& lset : lsets) {
    for (auto& l : lset) {
      values.insert(l.value);
    }
  }
  std::vector<std::string> v;
  for (auto& s : values)
    v.push_back(s);

  printf("num values: %d\n", v.size());

  for (int i = 1; i < 20; i++) {
    int num = v.size() / i;
    int64_t total_duration = 0;
    int total_size = 0;
    for (int j = 0; j < num * i; j += i) {
      tag_values_node_art node;
      art_tree_init(&node.tree_);
      Timer timer;
      timer.start();
      for (int k = j; k < j + i; k++) {
        _test_art_add(&node, k, v[k]);
      }
      total_duration += timer.since_start_nano();
      total_size += node.size();
      for (int k = j; k < j + i; k++)
        total_size -= v[k].size();
    }
    printf("group size: %d, duration(ns): %d, size: %d\n", i, total_duration / num, total_size / num);
  }
}

}  // namespace mem
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}