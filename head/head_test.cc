#include "head/Head.hpp"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include <boost/filesystem.hpp>

#include "base/Logging.hpp"
#include "base/ThreadPool.hpp"
#include "db/DBUtils.hpp"
#include "db/db_impl.h"
#include "gtest/gtest.h"
#include "head/HeadSeriesSet.hpp"
#include "label/EqualMatcher.hpp"
#include "querier/ChunkSeriesIterator.hpp"
#include "querier/Querier.hpp"
#include "querier/tsdb_querier.h"
#include "third_party/thread_pool.h"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"

namespace tsdb {
namespace head {

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

class HeadTest : public testing::Test {
 public:
  std::unique_ptr<Head> head;
  leveldb::DB* db;
  bool release_labels;

  HeadTest() : db(nullptr), release_labels(false) {}

  void clean(const std::string& path) { boost::filesystem::remove_all(path); }

  leveldb::Status setup(bool need_clean = true,
                        const std::string& snapshot_dir = "",
                        bool sync_api = false) {
    if (db) delete db;

    std::string path = "/tmp/head_test";
    head.reset();
    if (need_clean) clean(path);

    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 10 << 15;
    options.max_file_size = 10 << 15;
    options.use_log = false;
    leveldb::Status st = leveldb::DB::Open(options, path, &db);
    if (!st.ok()) return st;

    head.reset(new Head(path, snapshot_dir, db, sync_api));
    db->SetHead(head.get());
    return st;
  }

  void load_devops_labels1(int num_ts,
                           std::vector<tsdb::label::Labels>* lsets) {
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

  void insert_simple_data(int num_ts, int num_labels, int num_samples,
                          std::vector<std::vector<int64_t>>* times,
                          std::vector<std::vector<double>>* values) {
    auto app = head->appender();
    for (int i = 0; i < num_ts; i++) {
      tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label_" + std::to_string(j),
                          "value_" + std::to_string(i));

      auto r = app->add(lset, 0, 0);
      ASSERT_EQ(i, r.first);
      std::vector<int64_t> tmp_times;
      std::vector<double> tmp_values;
      tmp_times.push_back(0);
      tmp_values.push_back(0);
      for (int j = 0; j < num_samples; j++) {
        int64_t t = (j + 1) * 1000 + rand() % 100;
        double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
        tmp_times.push_back(t);
        tmp_values.push_back(v);
        leveldb::Status st = app->add_fast(i, t, v);
        ASSERT_TRUE(st.ok());
      }
      times->push_back(tmp_times);
      values->push_back(tmp_values);
      ASSERT_TRUE(app->commit(release_labels).ok());

      if ((i + 1) % (num_ts / 10) == 0)
        printf("insert_simple_data:%d\n", i + 1);
    }
  }

  void insert_simple_data_parallel(int num_ts, int num_labels, int num_samples,
                                   int num_thread) {
    auto func = [](head::Head* _h, int left, int right, int num_labels,
                   int num_samples, base::WaitGroup* _wg, bool release_labels) {
      auto app = _h->appender();
      for (int i = left; i < right; i++) {
        tsdb::label::Labels lset;
        for (int j = 0; j < num_labels; j++)
          lset.emplace_back("label_" + std::to_string(j),
                            "value_" + std::to_string(i));
        auto r = app->add(lset, 0, 0);
        for (int j = 0; j < num_samples; j++)
          app->add_fast(r.first, (j + 1) * 1000, (j + 1) * 1000);
        app->commit(release_labels);
      }
      _wg->done();
    };

    ThreadPool pool(num_thread);
    base::WaitGroup wg;
    for (int i = 0; i < num_thread; i++) {
      wg.add(1);
      pool.enqueue(std::bind(func, head.get(), i * num_ts / num_thread,
                             (i + 1) * num_ts / num_thread, num_labels,
                             num_samples, &wg, release_labels));
    }
    wg.wait();
  }

  ~HeadTest() {
    if (db) delete db;
  }
};

TEST_F(HeadTest, Test1) {
  release_labels = true;
  ASSERT_TRUE(setup().ok());
  auto app = head->appender();
  int num_ts = 10000;
  int num_labels = 10;
  std::set<std::string> syms;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));
      syms.insert("label_" + std::to_string(j));
      syms.insert("value_" + std::to_string(i));
    }
    auto r = app->add(std::move(lset), 0, 0);
    ASSERT_EQ(i, r.first);
    ASSERT_TRUE(app->commit(release_labels).ok());
  }

  // Symbols.
  ASSERT_EQ(syms, head->symbols());

  // Label names.
  std::vector<std::string> lnames;
  lnames.push_back(label::ALL_POSTINGS_KEYS.label);
  for (int j = 0; j < num_labels; j++)
    lnames.push_back("label_" + std::to_string(j));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, head->label_names());

  // Label values.
  std::vector<std::string> lvalues;
  for (int i = 0; i < num_ts; i++)
    lvalues.push_back("value_" + std::to_string(i));
  std::sort(lvalues.begin(), lvalues.end());
  for (int i = 0; i < num_labels; i++)
    ASSERT_EQ(lvalues, head->label_values("label_" + std::to_string(i)));

  // Postings.
  auto f = [&]() {
    for (int i = 0; i < num_ts; i++) {
      for (int j = 0; j < num_labels; j++) {
        auto p = head->postings("label_" + std::to_string(j),
                                "value_" + std::to_string(i));
        ASSERT_TRUE(p.second);
        int num = 0;
        while (p.first->next()) {
          ASSERT_EQ(i, p.first->at());
          num++;
        }
        ASSERT_EQ(1, num);
      }
    }
    auto p = head->postings(label::ALL_POSTINGS_KEYS.label,
                            label::ALL_POSTINGS_KEYS.value);
    ASSERT_TRUE(p.second);
    int num = 0;
    while (p.first->next()) {
      ASSERT_EQ(num, p.first->at());
      num++;
    }
    ASSERT_EQ(num_ts, num);
    printf("postings checked\n");
  };
  f();

  // Test after tags gc.
  head->set_inverted_index_gc_threshold(1000);
  printf("tags gc count:%d\n", head->inverted_index_gc());
  f();
}

TEST_F(HeadTest, Test2) {
  release_labels = true;
  ASSERT_TRUE(setup().ok());

  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 12;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  insert_simple_data(num_ts, num_labels, num_samples, &times, &values);

  auto f = [&]() {
    for (int i = 0; i < num_ts; i++) {
      tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label_" + std::to_string(j),
                          "value_" + std::to_string(i));

      tsdb::label::Labels result_lset;
      std::string chunk_contents;

      ASSERT_TRUE(head->series(i, result_lset, &chunk_contents));
      ASSERT_EQ(lset, result_lset);

      HeadIterator it(&chunk_contents, 0, 10000000);
      int count = 0;
      while (it.next()) {
        ASSERT_EQ(times[i][count], it.at().first);
        ASSERT_EQ(values[i][count], it.at().second);
        count++;
      }
      ASSERT_EQ(num_samples + 1, count);
    }
    printf("checked\n");
  };
  f();

  auto st = setup(false);
  printf("setup:%s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());
  f();
}

// Querier with memory data only.
TEST_F(HeadTest, Test3) {
  release_labels = true;
  ASSERT_TRUE(setup().ok());

  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 12;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  insert_simple_data(num_ts, num_labels, num_samples, &times, &values);

  auto f = [&]() {
    querier::TSDBQuerier q(db, head.get(), 0, 10000000);
    label::EqualMatcher m1("label_1", "value_1");
    label::EqualMatcher m2("label_0", "value_1");
    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
        q.select({&m1, &m2});
    ASSERT_TRUE(ss->next());
    std::unique_ptr<::tsdb::querier::SeriesInterface> s = ss->at();
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "value_1");
    ASSERT_EQ(lset, s->labels());
    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it1 =
        s->iterator();
    int count = 0;
    while (it1->next()) {
      ASSERT_EQ(times[1][count], it1->at().first);
      ASSERT_EQ(values[1][count], it1->at().second);
      count++;
    }
    ASSERT_EQ(num_samples + 1, count);
    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it2 =
        s->chain_iterator();
    count = 0;
    while (it2->next()) {
      ASSERT_EQ(times[1][count], it2->at().first);
      ASSERT_EQ(values[1][count], it2->at().second);
      count++;
    }
    ASSERT_EQ(num_samples + 1, count);
    ASSERT_FALSE(ss->next());
    printf("checked\n");
  };
  f();

  auto st = setup(false);
  printf("setup:%s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());
  f();
}

// Querier with leveldb data.
TEST_F(HeadTest, Test4) {
  release_labels = true;
  MAX_HEAD_SAMPLES_LOG_SIZE = 1024 * 1024;
  ASSERT_TRUE(setup().ok());

  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 120;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  insert_simple_data(num_ts, num_labels, num_samples, &times, &values);

  auto f = [&]() {
    querier::TSDBQuerier q(db, head.get(), 0, 10000000);
    label::EqualMatcher m1("label_1", "value_1");
    label::EqualMatcher m2("label_0", "value_1");
    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
        q.select({&m1, &m2});
    ASSERT_TRUE(ss->next());
    std::unique_ptr<::tsdb::querier::SeriesInterface> s = ss->at();
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "value_1");
    ASSERT_EQ(lset, s->labels());
    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it1 =
        s->iterator();
    int count = 0;
    while (it1->next()) {
      ASSERT_EQ(times[1][count], it1->at().first);
      ASSERT_EQ(values[1][count], it1->at().second);
      count++;
    }
    ASSERT_EQ(num_samples + 1, count);
    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it2 =
        s->chain_iterator();
    count = 0;
    while (it2->next()) {
      ASSERT_EQ(times[1][count], it2->at().first);
      ASSERT_EQ(values[1][count], it2->at().second);
      count++;
    }
    ASSERT_EQ(num_samples + 1, count);
    ASSERT_FALSE(ss->next());
    printf("checked\n");
  };
  f();

  head->clean_samples_logs();
  leveldb::Status st = head->snapshot_index();
  printf("snapshot_index: %s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());

  st = setup(false, "/tmp/head_test/snapshot");
  printf("setup:%s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());
  f();

  head->set_inverted_index_gc_threshold(1000);
  printf("tags gc count:%d\n", head->inverted_index_gc());
  printf("memseries_gc count:%d\n", head->memseries_gc(50));
  f();
}

// Parallel insertion.
TEST_F(HeadTest, Test4_2) {
  release_labels = true;
  MAX_HEAD_SAMPLES_LOG_SIZE = 1024 * 1024;
  ASSERT_TRUE(setup().ok());

  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 120;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  insert_simple_data_parallel(num_ts, num_labels, num_samples, 8);

  auto f = [&]() {
    querier::TSDBQuerier q(db, head.get(), 0, 10000000);
    label::EqualMatcher m1("label_1", "value_1");
    label::EqualMatcher m2("label_0", "value_1");
    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
        q.select({&m1, &m2});
    ASSERT_TRUE(ss->next());
    std::unique_ptr<::tsdb::querier::SeriesInterface> s = ss->at();
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "value_1");
    ASSERT_EQ(lset, s->labels());
    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it1 =
        s->iterator();
    int count = 0;
    while (it1->next()) {
      ASSERT_EQ(count * 1000, it1->at().first);
      ASSERT_EQ(count * 1000, it1->at().second);
      count++;
    }
    ASSERT_EQ(num_samples + 1, count);
    ASSERT_FALSE(ss->next());
    printf("checked\n");
  };
  f();

  auto st = setup(false);
  printf("setup:%s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());
  f();
}

// Test global gc.
TEST_F(HeadTest, Test5) {
  release_labels = true;
  MAX_HEAD_SAMPLES_LOG_SIZE = 1024 * 1024;
  ASSERT_TRUE(setup(true, "", true).ok());

  int num_ts = 100;
  int num_labels = 10;
  int num_samples = 120;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(num_ts, &lsets);

  head->enable_concurrency();
  int tid;
  head->register_thread(&tid);
  printf("tid:%d\n", tid);
  uint64_t epoch = 0;
  for (size_t i = 0; i < lsets.size(); i++) {
    usleep(1200);
    auto app = head->appender();
    head->update_local_epoch(tid);
    epoch = head->get_epoch(tid);
    auto p = app->add(lsets[i], 0, 0, epoch);
    ASSERT_TRUE(p.second.ok());
    for (int j = 1; j < num_samples; j++) {
      head->update_local_epoch(tid);
      app->add_fast(p.first, j, j);
    }
    app->commit(release_labels);
  }
  head->deregister_thread(tid);

  printf("Garbage counter: %lu\n", _garbage_counter);

  auto f = [&]() {
    querier::TSDBQuerier q(db, head.get(), 0, 10000000);
    label::EqualMatcher m(label::ALL_POSTINGS_KEYS.label,
                          label::ALL_POSTINGS_KEYS.value);
    std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q.select({&m});
    for (size_t i = 0; i < num_ts / 100 * 101; i++) {
      ASSERT_TRUE(ss->next());
      std::unique_ptr<::tsdb::querier::SeriesInterface> s = ss->at();
      ASSERT_EQ(lsets[i], s->labels());
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it1 =
          s->iterator();
      int count = 0;
      while (it1->next()) {
        ASSERT_EQ(count, it1->at().first);
        ASSERT_EQ(count, it1->at().second);
        count++;
      }
      ASSERT_EQ(num_samples, count);
    }
    ASSERT_FALSE(ss->next());
    printf("checked\n");
  };
  f();
}

TEST_F(HeadTest, Bench1) {
  ASSERT_TRUE(setup(true, "", true).ok());

  int num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(num_ts, &lsets);
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  auto app = head->appender();
  Timer timer;
  timer.start();
  // ProfilerStart("head.prof");
  // HeapProfilerStart("headheap.prof");
  for (size_t i = 0; i < lsets.size(); i++) {
    app->add(std::move(lsets[i]), 0, 0);
    // if ((i + 1) % (num_ts / 10) == 0) {
    //   // app->rollback();
    //   mem_usage(vm, rss);
    //   std::cout << "Virtual Memory: " << (vm / 1024) << "MB\nResident set
    //   size: " << (rss / 1024) << "MB\n" << std::endl;
    // }
  }
  // ProfilerStop();
  // HeapProfilerStop();
  int64_t d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (us)]:" << (d / 1000) << std::endl;
  app.reset();
  sleep(5);
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

TEST_F(HeadTest, Bench2) {
  ASSERT_TRUE(setup(true, "", true).ok());

  int num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(num_ts, &lsets);

  int num_threads = 8;
  std::vector<std::unique_ptr<db::AppenderInterface>> apps;
  apps.reserve(num_threads);
  ThreadPool pool(num_threads);
  base::WaitGroup wg;

  auto func = [](db::AppenderInterface* _p,
                 std::vector<tsdb::label::Labels>* _lsets, int left, int right,
                 base::WaitGroup* _wg) {
    for (int i = left; i < right; i++) _p->add(std::move(_lsets->at(i)), 0, 0);
    _wg->done();
  };

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();

  for (int i = 0; i < num_threads; i++) {
    wg.add(1);
    apps.push_back(std::move(head->appender()));
    pool.enqueue(std::bind(func, apps.back().get(), &lsets,
                           i * lsets.size() / num_threads,
                           (i + 1) * lsets.size() / num_threads, &wg));
  }
  wg.wait();

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
}

}  // namespace head
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}