#include "mem/inverted_index.h"

#include <gperftools/profiler.h>

#include <boost/filesystem.hpp>

#include "base/ThreadPool.hpp"
#include "db/DBUtils.hpp"
#include "gtest/gtest.h"
#include "head/Head.hpp"
#include "label/EqualMatcher.hpp"
#include "third_party/thread_pool.h"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"

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

class InvertedIndexTest : public testing::Test {
 public:
  void load_simple_labels1(std::vector<tsdb::label::Labels>* lsets) {
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
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

  void load_devops_labels2(std::vector<tsdb::label::Labels>* lsets) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 0;

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
          if (items[i].substr(0, pos_end) == "hostname")
            values.push_back(items[i].substr(pos_end + 1));
          else
            values.push_back(items[i].substr(pos_end + 1) + "_" +
                             std::to_string(ts_counter));
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

  int num_ts;
};

// SEDA.
TEST_F(InvertedIndexTest, Test0) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 100);

  tdb.start_all_stages();

  tdb.stop_all_stages();
}

TEST_F(InvertedIndexTest, Test1) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 100);

  label::Labels lset;
  for (int i = 0; i < 10; i++)
    lset.push_back(label::Label("key" + std::to_string(i + 1),
                                "value" + std::to_string(i + 1)));
  for (int i = 0; i < 10; i++) tdb.add(i + 1, lset);

  ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  ASSERT_EQ(11, tdb.mem_postings()->postings_list_num());
  ASSERT_EQ(11, tdb.mem_postings()->postings_num());

  std::vector<std::vector<uint64_t>> lists(10);
  std::vector<label::MatcherInterface*> matchers;
  for (int i = 0; i < 10; i++)
    matchers.push_back(new label::EqualMatcher(
        "key" + std::to_string(i + 1), "value" + std::to_string(i + 1)));
  tdb.select(matchers, &lists);
  std::vector<uint64_t> results;
  for (int i = 0; i < 10; i++) results.push_back(i + 1);
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(results, lists[i]);
  }

  tdb.try_migrate();
  ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  ASSERT_EQ(10, tdb.mem_postings()->postings_list_num());
  ASSERT_EQ(10, tdb.mem_postings()->postings_num());

  lists.clear();
  lists.resize(10);
  tdb.select(matchers, &lists);
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(results, lists[i]);
  }
}

void finish_callback(void* arg) { *((bool*)(arg)) = true; }

TEST_F(InvertedIndexTest, Test1_2) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 100);

  tdb.start_all_stages();

  label::Labels lset;
  for (int i = 0; i < 10; i++)
    lset.push_back(label::Label("key" + std::to_string(i + 1),
                                "value" + std::to_string(i + 1)));
  std::vector<std::promise<bool>> add_promises(10);
  std::vector<std::future<bool>> add_futures;
  add_futures.reserve(10);
  for (size_t i = 0; i < add_promises.size(); i++)
    add_futures.push_back(add_promises[i].get_future());
  for (int i = 0; i < 10; i++)
    tdb.async_add(
        i + 1, &lset,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); },
        &add_promises[i]);

  for (size_t i = 0; i < add_futures.size(); i++) add_futures[i].wait();

  // ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  // ASSERT_EQ(11, tdb.mem_postings()->postings_list_num());
  sleep(1);

  std::cout << "num_tag_values_nodes:"
            << tdb.mem_postings()->num_tag_values_nodes() << std::endl;
  std::cout << "postings_list_num:" << tdb.mem_postings()->postings_list_num()
            << std::endl;
  std::cout << "mem_size:" << tdb.mem_postings()->mem_size() << std::endl;

  std::vector<std::vector<uint64_t>> lists(10);
  std::vector<label::MatcherInterface*> matchers;
  for (int i = 0; i < 10; i++)
    matchers.push_back(new label::EqualMatcher(
        "key" + std::to_string(i + 1), "value" + std::to_string(i + 1)));
  {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    tdb.async_select(
        &matchers, &lists,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  }
  std::vector<uint64_t> results;
  for (int i = 0; i < 10; i++) results.push_back(i + 1);
  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(results, lists[i]);
  }

  tdb.stop_all_stages();
  sleep(2);
}

TEST_F(InvertedIndexTest, Test2) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 100);

  label::Labels lset;
  for (int i = 0; i < 10; i++)
    for (int j = 0; j < 11; j++)
      lset.push_back(label::Label("key" + std::to_string(i + 1),
                                  "value" + std::to_string(j + 1)));
  for (int i = 0; i < 10; i++) tdb.add(i + 1, lset);

  ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  ASSERT_EQ(111, tdb.mem_postings()->postings_list_num());
  ASSERT_EQ(111, tdb.mem_postings()->postings_num());

  std::vector<std::vector<uint64_t>> lists(110);
  std::vector<label::MatcherInterface*> matchers;
  for (int i = 0; i < 10; i++)
    for (int j = 0; j < 11; j++)
      matchers.push_back(new label::EqualMatcher(
          "key" + std::to_string(i + 1), "value" + std::to_string(j + 1)));
  tdb.select(matchers, &lists);
  std::vector<uint64_t> results;
  for (int i = 0; i < 10; i++) results.push_back(i + 1);
  for (size_t i = 0; i < 110; i++) {
    ASSERT_EQ(results, lists[i]);
  }

  tdb.try_migrate();
  ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  ASSERT_EQ(100, tdb.mem_postings()->postings_list_num());
  ASSERT_EQ(100, tdb.mem_postings()->postings_num());

  lists.clear();
  lists.resize(110);
  tdb.select(matchers, &lists);
  for (size_t i = 0; i < 110; i++) {
    ASSERT_EQ(results, lists[i]);
  }
}

TEST_F(InvertedIndexTest, Test2_2) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 100);

  tdb.start_all_stages();

  label::Labels lset;
  for (int i = 0; i < 10; i++)
    for (int j = 0; j < 11; j++)
      lset.push_back(label::Label("key" + std::to_string(i + 1),
                                  "value" + std::to_string(j + 1)));

  std::vector<std::promise<bool>> add_promises(10);
  std::vector<std::future<bool>> add_futures;
  add_futures.reserve(10);
  for (size_t i = 0; i < add_promises.size(); i++)
    add_futures.push_back(add_promises[i].get_future());
  for (int i = 0; i < 10; i++)
    tdb.async_add(
        i + 1, &lset,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); },
        &add_promises[i]);

  for (size_t i = 0; i < add_futures.size(); i++) add_futures[i].wait();

  // ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  // ASSERT_EQ(111, tdb.mem_postings()->postings_list_num());
  sleep(1);
  std::cout << "num_tag_values_nodes:"
            << tdb.mem_postings()->num_tag_values_nodes() << std::endl;
  std::cout << "postings_list_num:" << tdb.mem_postings()->postings_list_num()
            << std::endl;
  std::cout << "mem_size:" << tdb.mem_postings()->mem_size() << std::endl;

  std::vector<std::vector<uint64_t>> lists(110);
  std::vector<label::MatcherInterface*> matchers;
  for (int i = 0; i < 10; i++)
    for (int j = 0; j < 11; j++)
      matchers.push_back(new label::EqualMatcher(
          "key" + std::to_string(i + 1), "value" + std::to_string(j + 1)));
  {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    tdb.async_select(
        &matchers, &lists,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  }
  std::vector<uint64_t> results;
  for (int i = 0; i < 10; i++) results.push_back(i + 1);
  for (size_t i = 0; i < 110; i++) {
    ASSERT_EQ(results, lists[i]);
  }

  tdb.stop_all_stages();
}

TEST_F(InvertedIndexTest, Test3) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 100);

  num_ts = 10000;

  std::vector<label::Labels> lsets;
  load_simple_labels1(&lsets);
  for (size_t i = 0; i < lsets.size(); i++) tdb.add(i + 1, lsets[i]);

  ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  ASSERT_EQ(num_ts * 10 + 1, tdb.mem_postings()->postings_list_num());
  ASSERT_EQ(num_ts * 10 + 1, tdb.mem_postings()->postings_num());

  std::vector<std::vector<uint64_t>> lists(1);
  std::vector<label::MatcherInterface*> matchers(
      {new label::EqualMatcher("label0", "label0_0")});
  tdb.select(matchers, &lists);
  ASSERT_EQ(std::vector<uint64_t>({1}), lists[0]);

  tdb.try_migrate();
  ASSERT_EQ(11, tdb.mem_postings()->num_tag_values_nodes());
  ASSERT_EQ(num_ts * 9 + 1, tdb.mem_postings()->postings_list_num());
  ASSERT_EQ(num_ts * 9 + 1, tdb.mem_postings()->postings_num());

  lists.clear();
  lists.resize(1);
  tdb.select(matchers, &lists);
  ASSERT_EQ(std::vector<uint64_t>({1}), lists[0]);

  lists.clear();
  lists.resize(2);
  matchers.clear();
  matchers.push_back(new label::EqualMatcher("label9", "label9_5000"));
  matchers.push_back(new label::EqualMatcher("label0", "label0_223"));
  tdb.select(matchers, &lists);
  ASSERT_EQ(std::vector<uint64_t>({5001}), lists[0]);
  ASSERT_EQ(std::vector<uint64_t>({224}), lists[1]);
}

TEST_F(InvertedIndexTest, Test3_2) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);

  InvertedIndex tdb(dbpath, "", 6411622);

  tdb.start_all_stages();

  num_ts = 10000;

  std::vector<label::Labels> lsets;
  load_simple_labels1(&lsets);
  std::vector<std::promise<bool>> add_promises(lsets.size());
  std::vector<std::future<bool>> add_futures;
  add_futures.reserve(lsets.size());
  for (size_t i = 0; i < add_promises.size(); i++)
    add_futures.push_back(add_promises[i].get_future());
  for (size_t i = 0; i < lsets.size(); i++)
    tdb.async_add(
        i + 1, &lsets[i],
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); },
        &add_promises[i]);

  for (size_t i = 0; i < add_futures.size(); i++) add_futures[i].wait();

  std::cout << "num_tag_values_nodes:"
            << tdb.mem_postings()->num_tag_values_nodes() << std::endl;
  std::cout << "original postings_list_num:" << num_ts * 10 + 1 << std::endl;
  std::cout << "postings_list_num:" << tdb.mem_postings()->postings_list_num()
            << std::endl;
  std::cout << "mem_size:" << tdb.mem_postings()->mem_size() << std::endl;

  std::vector<std::vector<uint64_t>> lists(1);
  std::vector<label::MatcherInterface*> matchers(
      {new label::EqualMatcher("label0", "label0_0")});
  {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    tdb.async_select(
        &matchers, &lists,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  }
  ASSERT_EQ(std::vector<uint64_t>({1}), lists[0]);

  std::cout << "after select1" << std::endl;

  lists.clear();
  lists.resize(2);
  delete matchers[0];
  matchers.clear();
  matchers.push_back(new label::EqualMatcher("label9", "label9_5000"));
  matchers.push_back(new label::EqualMatcher("label0", "label0_223"));
  {
    std::promise<bool> p;
    std::future<bool> f = p.get_future();
    tdb.async_select(
        &matchers, &lists,
        [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
    f.wait();
  }
  ASSERT_EQ(std::vector<uint64_t>({5001}), lists[0]);
  ASSERT_EQ(std::vector<uint64_t>({224}), lists[1]);

  std::cout << "after select2" << std::endl;

  lists.clear();
  lists.resize(1);
  delete matchers[0];
  delete matchers[1];
  matchers.clear();
  for (int i = 0; i < num_ts; i++) {
    matchers.push_back(
        new label::EqualMatcher("label5", "label5_" + std::to_string(i)));
    {
      std::promise<bool> p;
      std::future<bool> f = p.get_future();
      tdb.async_select(
          &matchers, &lists,
          [](void* arg) { ((std::promise<bool>*)(arg))->set_value(true); }, &p);
      f.wait();
    }
    ASSERT_EQ(std::vector<uint64_t>({(uint64_t)(i + 1)}), lists[0]);
    lists[0].clear();
    delete matchers[0];
    matchers.clear();
  }

  std::cout << "after select3" << std::endl;

  tdb.stop_all_stages();
  std::cout << "after stop_all_stages" << std::endl;
}

}  // namespace mem
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}