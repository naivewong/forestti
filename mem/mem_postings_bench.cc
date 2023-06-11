#include "mem/mem_postings.h"

#include <sys/resource.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <fstream>
#include <jemalloc/jemalloc.h>
#include <unordered_set>

#include "base/WaitGroup.hpp"
#include "gtest/gtest.h"
#include "index/VectorPostings.hpp"
#include "index/EmptyPostings.hpp"
#include "leveldb/third_party/thread_pool.h"
#include "mem/mergeset.h"
#include "third_party/art.h"
#include "third_party/cedarpp.h"
#include "util/coding.h"

#include "leveldb/filter_policy.h"

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

class CedarWrapper {
public:
  cedar::da<uint32_t> trie_;
  std::vector<std::vector<uint64_t>> lists_;
  char buf_[1024];

  void add_for(uint64_t id, const tsdb::label::Label& l) {
    memcpy(buf_, l.label.c_str(), l.label.size());
    buf_[l.label.size()] = '$';
    memcpy(buf_ + l.label.size() + 1, l.value.c_str(), l.value.size());
    buf_[l.label.size() + l.value.size() + 1] = '\0';

    uint32_t result = trie_.exactMatchSearch<uint32_t>(buf_);
    if (result != std::numeric_limits<uint32_t>::max())
      lists_[result].push_back(id);
    else {
      lists_.emplace_back();
      lists_.back().push_back(id);
      trie_.update(buf_, l.label.size() + l.value.size() + 1, lists_.size() - 1);
    }
  }

  void add(uint64_t id, const tsdb::label::Labels& lsets) {
    for (size_t i = 0; i < lsets.size(); i++)
      add_for(id, lsets[i]);
    add_for(id, tsdb::label::ALL_POSTINGS_KEYS);
  }

  std::unique_ptr<tsdb::index::PostingsInterface> get(const std::string& name, const std::string& value) {
    memcpy(buf_, name.c_str(), name.size());
    buf_[name.size()] = '$';
    memcpy(buf_ + name.size() + 1, value.c_str(), value.size());
    buf_[name.size() + value.size() + 1] = '\0';

    uint32_t result = trie_.exactMatchSearch<uint32_t>(buf_);
    if (result != std::numeric_limits<uint32_t>::max())
      return std::unique_ptr<tsdb::index::PostingsInterface>(
          new tsdb::index::VectorPtrPostings(&(lists_[result])));
    return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::EmptyPostings());
  }
};

class ArtWrapper {
public:
  art::art_tree tree_;
  std::vector<std::vector<uint64_t>> lists_;
  unsigned char buf_[1024];

  ArtWrapper() {
    art::art_tree_init(&tree_);
  }

  ~ArtWrapper() {
    art::art_tree_destroy(&tree_);
  }

  void add_for(uint64_t id, const tsdb::label::Label& l) {
    memcpy(buf_, l.label.c_str(), l.label.size());
    buf_[l.label.size()] = '$';
    memcpy(buf_ + l.label.size() + 1, l.value.c_str(), l.value.size());

    void* r = art::art_search(&tree_, buf_, l.label.size() + l.value.size() + 1);
    if (r) {
      uint64_t idx = (uint64_t)((uintptr_t)r);
      lists_[idx].push_back(id);
    }
    else {
      std::vector<uint64_t> pl(1);
      pl[0] = id;
      lists_.push_back(std::move(pl));
      art::art_insert(&tree_, buf_, l.label.size() + l.value.size() + 1, (void*)((uintptr_t)(lists_.size() - 1)));
    }
  }

  void add(uint64_t id, const tsdb::label::Labels& lsets) {
    for (size_t i = 0; i < lsets.size(); i++)
      add_for(id, lsets[i]);
    add_for(id, tsdb::label::ALL_POSTINGS_KEYS);
  }

  std::unique_ptr<tsdb::index::PostingsInterface> get(const std::string& name, const std::string& value) {
    memcpy(buf_, name.c_str(), name.size());
    buf_[name.size()] = '$';
    memcpy(buf_ + name.size() + 1, value.c_str(), value.size());

    void* r = art::art_search(&tree_, buf_, name.size() + value.size() + 1);
    if (r)
      return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::VectorPtrPostings(&lists_[(uint64_t)((uintptr_t)r)]));
    return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::EmptyPostings());
  }
};

class MapWrapper {
public:
  std::unordered_map<std::string, std::unordered_map<std::string, int>> m_;
  std::vector<std::vector<uint64_t>> lists_;

  void add_for(uint64_t id, const tsdb::label::Label& l) {
    auto it1 = m_.find(l.label);
    if (it1 != m_.end()) {
      auto it2 = it1->second.find(l.value);
      if (it2 != it1->second.end()) {
        lists_[it2->second].push_back(id);
        return;
      }
    }
    std::vector<uint64_t> pl(1);
    pl[0] = id;
    lists_.push_back(std::move(pl));
    m_[l.label][l.value] = lists_.size() - 1;
  }

  void add(uint64_t id, const tsdb::label::Labels& lsets) {
    for (size_t i = 0; i < lsets.size(); i++)
      add_for(id, lsets[i]);
    add_for(id, tsdb::label::ALL_POSTINGS_KEYS);
  }

  std::unique_ptr<tsdb::index::PostingsInterface> get(const std::string& name, const std::string& value) {
    auto it1 = m_.find(name);
    if (it1 != m_.end()) {
      auto it2 = it1->second.find(value);
      if (it2 != it1->second.end())
        return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::VectorPtrPostings(&lists_[it2->second]));
    }
    return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::EmptyPostings());
  }
};

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

  void load_simple_labels1(CedarWrapper* w,
                           int num_labels = 10) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      w->add(i + 1, lset);
    }
  }

  void load_simple_labels1(ArtWrapper* w,
                           int num_labels = 10) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      w->add(i + 1, lset);
    }
  }

  void load_simple_labels1(MapWrapper* w,
                           int num_labels = 10) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      w->add(i + 1, lset);
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

TEST_F(MemPostingsTest, TestSize1) {
  std::string buf;
  num_ts = 1000000;
  std::vector<label::Labels> lsets;
  load_devops_labels1(&lsets);
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  std::vector<art_tree> trees(1 << 14);
  for (size_t i = 0; i < trees.size(); i++) art_tree_init(&trees[i]);
  for (int i = 0; i < lsets.size(); i++) {
    buf.clear();
    uint64_t hash = label::lbs_hash(lsets[i]);
    leveldb::PutFixed64BE(&buf, i);
    int idx = hash % trees.size();
    art_insert(&trees[idx], reinterpret_cast<const unsigned char*>(buf.c_str()),
               buf.size(), (void*)(uintptr_t)hash);
  }

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(MemPostingsTest, TestSize2) {
  double vm, rss;
  num_ts = 1000000;
  std::vector<label::Labels> lsets;
  load_devops_labels1(&lsets);
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  std::vector<std::unordered_map<uint64_t, uint64_t>> trees(1 << 14);
  for (int i = 0; i < lsets.size(); i++) {
    uint64_t hash = label::lbs_hash(lsets[i]);
    int idx = hash % trees.size();
    trees[idx][i] = hash;
  }

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(MemPostingsTest, TestSize3) {
  double vm, rss;
  num_ts = 1000000;
  std::vector<label::Labels> lsets;
  load_devops_labels1(&lsets);
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  std::vector<std::unordered_map<uint64_t, std::vector<uint64_t>>> trees(1
                                                                         << 14);
  for (int i = 0; i < lsets.size(); i++) {
    uint64_t hash = label::lbs_hash(lsets[i]);
    int idx = hash % trees.size();
    auto p = trees[idx].find(hash);
    if (p == trees[idx].end())
      trees[idx][hash] = std::vector<uint64_t>({i});
    else
      p->second.push_back(i);
  }

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(MemPostingsTest, TestCedarWrapper) {
  CedarWrapper trie;
  std::vector<label::Labels> lsets({
    label::Labels({{"name", "val1"}, {"id", "1"}}),
    label::Labels({{"name", "val2"}, {"id", "1"}}),
    label::Labels({{"nam", "val1"}}),
  });

  for (size_t i = 0; i < lsets.size(); i++)
    trie.add(i + 1, lsets[i]);

  ASSERT_EQ(std::vector<uint64_t>({1, 2}), index::expand_postings(trie.get("id", "1")));
  ASSERT_EQ(std::vector<uint64_t>({3}), index::expand_postings(trie.get("nam", "val1")));
  ASSERT_EQ(std::vector<uint64_t>({1, 2, 3}), index::expand_postings(trie.get(label::ALL_POSTINGS_KEYS.label, label::ALL_POSTINGS_KEYS.value)));
}

TEST_F(MemPostingsTest, TestArtWrapper) {
  ArtWrapper trie;
  std::vector<label::Labels> lsets({
    label::Labels({{"name", "val1"}, {"id", "1"}}),
    label::Labels({{"name", "val2"}, {"id", "1"}}),
    label::Labels({{"nam", "val1"}}),
  });

  for (size_t i = 0; i < lsets.size(); i++)
    trie.add(i + 1, lsets[i]);

  ASSERT_EQ(std::vector<uint64_t>({1, 2}), index::expand_postings(trie.get("id", "1")));
  ASSERT_EQ(std::vector<uint64_t>({3}), index::expand_postings(trie.get("nam", "val1")));
  ASSERT_EQ(std::vector<uint64_t>({1, 2, 3}), index::expand_postings(trie.get(label::ALL_POSTINGS_KEYS.label, label::ALL_POSTINGS_KEYS.value)));
}

TEST_F(MemPostingsTest, TestMapWrapper) {
  MapWrapper m;
  std::vector<label::Labels> lsets({
    label::Labels({{"name", "val1"}, {"id", "1"}}),
    label::Labels({{"name", "val2"}, {"id", "1"}}),
    label::Labels({{"nam", "val1"}}),
  });

  for (size_t i = 0; i < lsets.size(); i++)
    m.add(i + 1, lsets[i]);

  ASSERT_EQ(std::vector<uint64_t>({1, 2}), index::expand_postings(m.get("id", "1")));
  ASSERT_EQ(std::vector<uint64_t>({3}), index::expand_postings(m.get("nam", "val1")));
  ASSERT_EQ(std::vector<uint64_t>({1, 2, 3}), index::expand_postings(m.get(label::ALL_POSTINGS_KEYS.label, label::ALL_POSTINGS_KEYS.value)));
}

TEST_F(MemPostingsTest, BenchCedarWrapper) {
  num_ts = 1000000;

  Timer timer;
  timer.start();

  CedarWrapper trie;
  load_simple_labels1(&trie, 20);

  int64_t d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (us)]:" << (d / 1000) << std::endl;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  sleep(60);

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;

  timer.start();
  for (int i = 0; i < num_ts; i += 1000) {
    {
      auto p = trie.get("label0", "label0_" + std::to_string(i));
      while (p->next()) {}
    }
    {
      auto p = trie.get("label19", "label19_" + std::to_string(i));
      while (p->next()) {}
    }
  }
  d = timer.since_start_nano();
  std::cout << "[Total Query duration (us)]:" << (d / 1000) << std::endl;
}

TEST_F(MemPostingsTest, BenchArtWrapper) {
  // bool bg = true;
  // size_t sz = sizeof(bg);
  // mallctl("opt.background_thread", NULL, 0, &bg, sz);
  // ssize_t t = 0;
  // sz = sizeof(t);
  // mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
  // mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

  num_ts = 1000000;

  Timer timer;
  timer.start();

  ArtWrapper art;
  load_simple_labels1(&art, 20);

  int64_t d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (us)]:" << (d / 1000) << std::endl;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  sleep(60);

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;

  timer.start();
  for (int i = 0; i < num_ts; i += 1000) {
    {
      auto p = art.get("label0", "label0_" + std::to_string(i));
      while (p->next()) {}
    }
    {
      auto p = art.get("label19", "label19_" + std::to_string(i));
      while (p->next()) {}
    }
  }
  d = timer.since_start_nano();
  std::cout << "[Total Query duration (us)]:" << (d / 1000) << std::endl;
}

TEST_F(MemPostingsTest, BenchMapWrapper) {
  num_ts = 1000000;

  Timer timer;
  timer.start();

  MapWrapper m;
  load_simple_labels1(&m, 20);

  int64_t d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (us)]:" << (d / 1000) << std::endl;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  sleep(60);

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;

  timer.start();
  for (int i = 0; i < num_ts; i += 1000) {
    {
      auto p = m.get("label0", "label0_" + std::to_string(i));
      while (p->next()) {}
    }
    {
      auto p = m.get("label19", "label19_" + std::to_string(i));
      while (p->next()) {}
    }
  }
  d = timer.since_start_nano();
  std::cout << "[Total Query duration (us)]:" << (d / 1000) << std::endl;
}

class GoArtWrapper {
public:
  art_tree tree_;
  std::vector<std::vector<uint64_t>> lists_;
  unsigned char buf_[1024];
  std::string s_;

  GoArtWrapper() {
    art_tree_init(&tree_);
  }

  ~GoArtWrapper() {
    art_tree_destroy(&tree_);
  }

  void add_for(uint64_t id, const tsdb::label::Label& l) {
    memcpy(buf_, l.label.c_str(), l.label.size());
    buf_[l.label.size()] = '$';
    memcpy(buf_ + l.label.size() + 1, l.value.c_str(), l.value.size());

    void* r = art_search(&tree_, buf_, l.label.size() + l.value.size() + 1);
    if (r) {
      uint64_t idx = (uint64_t)((uintptr_t)r);
      lists_[idx].push_back(id);
    }
    else {
      std::vector<uint64_t> pl(1);
      pl[0] = id;
      lists_.push_back(std::move(pl));
      art_insert(&tree_, buf_, l.label.size() + l.value.size() + 1, (void*)((uintptr_t)(lists_.size() - 1)));
    }
  }

  void add(uint64_t id, const tsdb::label::Labels& lsets) {
    for (size_t i = 0; i < lsets.size(); i++)
      add_for(id, lsets[i]);
    add_for(id, tsdb::label::ALL_POSTINGS_KEYS);
  }

  std::unique_ptr<tsdb::index::PostingsInterface> get(const std::string& name, const std::string& value) {
    memcpy(buf_, name.c_str(), name.size());
    buf_[name.size()] = '$';
    memcpy(buf_ + name.size() + 1, value.c_str(), value.size());

    void* r = art_search(&tree_, buf_, name.size() + value.size() + 1);
    if (r)
      return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::VectorPtrPostings(&lists_[(uint64_t)((uintptr_t)r)]));
    return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::EmptyPostings());
  }

  std::unique_ptr<tsdb::index::PostingsInterface> get(MergeSet* ms, const std::string& name, const std::string& value) {
    memcpy(buf_, name.c_str(), name.size());
    buf_[name.size()] = '$';
    memcpy(buf_ + name.size() + 1, value.c_str(), value.size());

    s_.clear();
    leveldb::Status s = ms->Get(leveldb::ReadOptions(), leveldb::Slice(reinterpret_cast<const char*>(buf_), name.size() + value.size() + 1), &s_);
    if (s.ok())
      return std::unique_ptr<tsdb::index::PostingsInterface>(new tsdb::index::VectorPtrPostings(&lists_[leveldb::DecodeFixed64(s_.data())]));
    abort();
  }
};

TEST_F(MemPostingsTest, TestMergeSet) {
  num_ts = 1000000;
  std::vector<label::Labels> lsets;
  load_simple_labels1(&lsets, 20);
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  GoArtWrapper art;
  for (size_t i = 0; i < lsets.size(); i++)
    art.add(i + 1, lsets[i]);

  MergeSet* m;
  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
  opts.max_file_size = 32 * 1024 * 1024;
  opts.use_log = false;
  leveldb::DestroyDB("/tmp/mergeset", opts);
  leveldb::Status s = MergeSet::MergeSetOpen(opts, "/tmp/mergeset", &m);
  ASSERT_TRUE(s.ok());

  Timer timer;
  timer.start();

  m->TrieToL1SSTs(&art.tree_, true);

  int64_t d = timer.since_start_nano();
  std::cout << "[Total Conversion duration (us)]:" << (d / 1000) << std::endl;

  sleep(20);

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;

  timer.start();
  for (int i = 0; i < num_ts; i += 1000) {
    {
      auto p = art.get(m, lsets[i][0].label, lsets[i][0].value);
      while (p->next()) {}
    }
    {
      auto p = art.get(m, lsets[i][lsets[i].size() - 1].label, lsets[i][lsets[i].size() - 1].value);
      while (p->next()) {}
    }
  }
  d = timer.since_start_nano();
  std::cout << "[Total Query duration (us)]:" << (d / 1000) << std::endl;

  delete m;
  delete opts.filter_policy;
}

TEST_F(MemPostingsTest, BenchMulti) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);

  int num_threads = 4;
  ThreadPool pool(num_threads);
  base::WaitGroup wg;

  auto func = [](MemPostings* _p, std::vector<tsdb::label::Labels>* _lsets,
                 int left, int right, base::WaitGroup* _wg) {
    printf("left:%d right:%d\n", left, right);
    for (int i = left; i < right; i++) _p->add(i + 1, _lsets->at(i));
    _wg->done();
  };

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  // ProfilerStart("cpu.prof");
  for (int i = 0; i < num_threads; i++) {
    wg.add(1);
    pool.enqueue(std::bind(func, &mp, &lsets, i * lsets.size() / num_threads,
                           (i + 1) * lsets.size() / num_threads, &wg));
  }
  wg.wait();
  // ProfilerStop();
  int64_t d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (us)]:" << (d / 1000) << std::endl;
  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;

  std::cout << "mem_size: " << mp.mem_size() << std::endl;
  std::cout << "key_size:" << mp.tag_key_size() << std::endl;
  std::cout << "postings_num:" << mp.postings_num() << std::endl;
  std::cout << "sizeof(postings_list):" << sizeof(postings_list)
            << " postings_size:" << mp.postings_size() << std::endl;
}

TEST_F(MemPostingsTest, TestSize) {
  struct rusage r_usage;
  double vm, rss;
  num_ts = 100000;
  std::vector<label::Labels> lsets;
  load_simple_labels1(&lsets);
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  int ret = getrusage(RUSAGE_SELF, &r_usage);
  printf("Memory usage: %ldMB\n", r_usage.ru_maxrss / 1024);

  // MemPostings p;
  // for (size_t i = 0; i < lsets.size(); i++)
  //   p.add(i, lsets[i]);
  lsets.clear();
  lsets.shrink_to_fit();
  sleep(120);

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "Virtual Memory: " << (vm2 / 1024)
            << "MB\nResident set size: " << (rss2 / 1024) << "MB\n"
            << std::endl;
  std::cout << "Virtual Memory(diff): " << ((vm2 - vm) / 1024)
            << "MB\nResident set size(diff): " << ((rss2 - rss) / 1024)
            << "MB\n"
            << std::endl;

  ret = getrusage(RUSAGE_SELF, &r_usage);
  printf("Memory usage: %ldMB\n", r_usage.ru_maxrss / 1024);
}

}  // namespace mem
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}