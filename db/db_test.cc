#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <signal.h>

#include <boost/filesystem.hpp>
#include <jemalloc/jemalloc.h>

#include "chunk/XORChunk.hpp"
#include "db/version_set.h"
#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/MemSeries.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"
#include "leveldb/cache.h"
#include "port/port.h"
#include "querier/tsdb_querier.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/thread_pool.h"

namespace tsdb {
namespace db {

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
                                                   {"1-1-12", false},
                                                   {"1-1-24", true},
                                                   {"1-1-all", false},
                                                   {"1-8-1", true},
                                                   {"5-1-1", true},
                                                   {"5-1-12", false},
                                                   {"5-1-24", true},
                                                   {"5-1-all", false},
                                                   {"5-8-1", true},
                                                   {"cpu-max-all-8", true},
                                                   {"double-groupby-5", true},
                                                   {"double-groupby-all", true},
                                                   {"high-cpu-1", false},
                                                   {"high-cpu-all", false},
                                                   {"lastpoint", true}});

class TSDBTest {
 public:
  TSDBTest() {
    for (int i = 0; i < 50; i++)
      matchers1.emplace_back("hostname", "host_" + std::to_string(i));
    matchers2 = std::vector<tsdb::label::EqualMatcher>(
        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
         tsdb::label::EqualMatcher("__name__", "diskio_reads"),
         tsdb::label::EqualMatcher("__name__", "kernel_boot_time"),
         tsdb::label::EqualMatcher("__name__", "mem_total"),
         tsdb::label::EqualMatcher("__name__", "net_bytes_sent")});
    matchers3 = std::vector<tsdb::label::EqualMatcher>(
        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_system"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_idle"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_nice"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_iowait"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_irq"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_softirq"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_steal"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_guest"),
         tsdb::label::EqualMatcher("__name__", "cpu_usage_guest_nice")});
  }

  void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
    num_ts = num_ts_;
    tuple_size = tuple_size_;
    num_tuple = num_tuple_;
    head::MEM_TUPLE_SIZE = tuple_size_;
    for (int i = 50; i < num_ts/100; i++)
      matchers1.emplace_back("hostname", "host_" + std::to_string(i));
  }

  // Simple labels.
  void head_add1(int64_t st, int64_t interval, int num_labels) {
    // auto app = head_->TEST_appender();
    for (int i = 0; i < num_ts; i++) {
      auto app = head_->appender();
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      // lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0);
      if (r.first != ((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                         (uint64_t)(i % PRESERVED_BLOCKS))
        std::cout << "TSDBTest::head_add1 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      st + k * interval * 1000, st + k * interval * 1000);
      app->commit();
    }
    // app->TEST_commit();
  }

  // Node exporter labels.
  void head_add2(int64_t st, int64_t interval) {
    char instance[64];
    int current_instance = 0;
    std::ifstream file("../test/timeseries.json");
    std::string line;

    for (int i = 0; i < num_ts; i++) {
      auto app = head_->appender();

      if (!getline(file, line)) {
        file = std::ifstream("../test/timeseries.json");
        ++current_instance;
        getline(file, line);
      }
      rapidjson::Document d;
      d.Parse(line.c_str());
      if (d.HasParseError()) std::cout << "Cannot parse: " << line << std::endl;
      tsdb::label::Labels lset;
      for (auto& m : d.GetObject()) {
        if (memcmp(m.name.GetString(), "instance", 8) == 0) {
          sprintf(instance, "pc9%06d:9100", current_instance);
          lset.emplace_back(m.name.GetString(), instance);
        }
        lset.emplace_back(m.name.GetString(), m.value.GetString());
      }
      std::sort(lset.begin(), lset.end());

      auto r = app->add(lset, 0, 0);
      if (r.first != ((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                         (uint64_t)(i % PRESERVED_BLOCKS))
        std::cout << "TSDBTest::head_add2 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      st + k * interval * 1000, st + k * interval * 1000);
      app->commit();
    }
  }

  int load_devops(int num_lines, const std::string& name, int64_t st,
                  int64_t interval, int ts_counter) {
    std::ifstream file(name);
    std::string line;
    int cur_line = 0;

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
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
          auto app = head_->appender();
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          auto r = app->add(lset, tfluc[0], tfluc[0]);
          if (r.first != ((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                             (uint64_t)(ts_counter % PRESERVED_BLOCKS)) {
            std::cout << tsdb::label::lbs_string(lset) << std::endl;
            std::cout << "TSDBTest::load_devops wrong id exp:" << ts_counter
                      << " got:" << r.first << std::endl;
          }

          for (int k = 1; k < tuple_size; k++)
            app->add_fast(((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                              (uint64_t)(ts_counter % PRESERVED_BLOCKS),
                          st + k * interval * 1000 + tfluc[k], tfluc[k]);
          app->commit();

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
    return ts_counter;
  }

  // Devops labels.
  void head_add3(int64_t st, int64_t interval) {
    int num_lines = num_ts / 100;
    int tscounter;
    if (num_lines > 100000) {
      tscounter =
          load_devops(100000, "../test/devops100000.txt", st, interval, 0);
      tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                              st, interval, tscounter);
    } else
      tscounter =
          load_devops(num_lines, "../test/devops100000.txt", st, interval, 0);
    std::cout << "head_add3: " << tscounter << std::endl;
  }

  int load_devops(int num_lines, const std::string& name,
                  std::vector<label::Labels>* lsets, int ts_counter) {
    std::ifstream file(name);
    std::string line;
    int cur_line = 0;

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
    return ts_counter;
  }

  void get_devops_labels(std::vector<label::Labels>* lsets) {
    int num_lines = num_ts / 100;
    int tscounter;
    if (num_lines > 100000) {
      tscounter = load_devops(100000, "../test/devops100000.txt", lsets, 0);
      tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                              lsets, tscounter);
    } else
      tscounter = load_devops(num_lines, "../test/devops100000.txt", lsets, 0);
    std::cout << "get_devops_labels: " << tscounter << std::endl;
  }

  void head_add4(int64_t st, int64_t interval) {
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100;
    int cur_line = 0;
    int ts_counter = 0;

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
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
          auto app = head_->appender();
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          auto r = app->add(lset, tfluc[0], tfluc[0]);
          if (r.first != ((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                             (uint64_t)(ts_counter % PRESERVED_BLOCKS)) {
            std::cout << tsdb::label::lbs_string(lset) << std::endl;
            std::cout << "TSDBTest::head_add4 wrong id exp:" << ts_counter
                      << " got:" << r.first << std::endl;
          }

          for (int k = 1; k < tuple_size; k++)
            app->add_fast(((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                              (uint64_t)(ts_counter % PRESERVED_BLOCKS),
                          st + k * interval * 1000 + tfluc[k], tfluc[k]);
          app->commit();

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }
  }

  // Mixed labels
  void head_add5(int64_t st, int64_t interval) {
    std::ifstream file("../test/devops100000.txt");
    std::string line;
    int num_lines = num_ts / 100 / 2;
    int cur_line = 0;
    int ts_counter = 0;

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
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
          auto app = head_->appender();
          tsdb::label::Labels lset;
          for (size_t j = 0; j < names.size(); j++)
            lset.emplace_back(names[j], values[j]);
          lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
          std::sort(lset.begin(), lset.end());

          auto r = app->add(lset, tfluc[0], tfluc[0]);
          if (r.first != ((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                             (uint64_t)(ts_counter % PRESERVED_BLOCKS)) {
            std::cout << tsdb::label::lbs_string(lset) << std::endl;
            std::cout << "TSDBTest::head_add5 wrong id exp:" << ts_counter
                      << " got:" << r.first << std::endl;
          }

          for (int k = 1; k < tuple_size; k++)
            app->add_fast(((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                              (uint64_t)(ts_counter % PRESERVED_BLOCKS),
                          st + k * interval * 1000 + tfluc[k], tfluc[k]);
          app->commit();

          ts_counter++;
        }
        cur_line++;
      }
      for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
      cur_line = 0;
    }

    for (int i = 0; i < num_ts / 2; i++) {
      auto app = head_->appender();
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 20; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));

      auto r = app->add(lset, tfluc[0], tfluc[0]);
      if (r.first != ((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                         (uint64_t)(ts_counter % PRESERVED_BLOCKS))
        std::cout << "TSDBTest::head_add5 wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(ts_counter % PRESERVED_BLOCKS),
                      st + k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
      ts_counter++;
    }
  }

  void head_add_fast1(int64_t st, int64_t interval) {
    // auto app = head_->TEST_appender();
    Timer t;
    // int count = 0;
    int64_t d, last_t;
    t.start();
    auto app = head_->appender();
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    for (int i = 0; i < num_ts; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      st + k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
      // count += tuple_size;
      // if (count > 500000) {
      //   d = t.since_start_nano();
      //   LOG_INFO << "Time(ms):" <<
      //   std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()
      //   << " throughput:" << (double)(count)  / (double)(d - last_t) *
      //   1000000000; last_t = d; count = 0;
      // }
    }
    // app->TEST_commit();
  }

  void head_add_fast_partial(int64_t st, int step, int64_t interval) {
    auto app = head_->appender();
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    for (int i = 0; i < num_ts; i += step) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      st + k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
    }
  }

  void head_add_random(int64_t st, int64_t interval) {
    auto app = head_->appender();
    int64_t fluc;
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 20; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0);
      if (!r.second.ok())
        std::cout << "TSDBTest::insert_tuple_with_labels failed" << std::endl;
      else if (r.first != ((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                              (uint64_t)(i % PRESERVED_BLOCKS))
        std::cout << "TSDBTest::insert_tuple_with_labels wrong id exp:" << i + 1
                  << " got:" << r.first << std::endl;
      // app->rollback(); // Clear the fake sample.

      fluc = rand() % 200;
      for (int k = 1; k < tuple_size; k++)
        app->add_fast(i, st + k * interval * 1000 + fluc,
                      st + k * interval * 1000 + fluc);
    }
    app->commit();
  }

  void head_add_fast_random(int64_t st, int64_t interval) {
    int64_t fluc;
    auto app = head_->appender();
    for (int i = 0; i < num_ts; i++) {
      fluc = rand() % 200;
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(i, st + k * interval * 1000 + fluc,
                      st + k * interval * 1000 + fluc);
    }
    app->commit();
  }

  void queryDevOps1(leveldb::DB* db, int64_t endtime,
                    leveldb::Cache* cache = nullptr) {
    int iteration = 1000;

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
    // hours.
    if (query_types["1-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[round % 50], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      std::cout << "[1-1-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
    // hours.
    if (query_types["1-1-12"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < (iteration / 2); round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 43200000, endtime,
                               cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[round % 50], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      std::cout << "[1-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / (iteration / 2)
                << "us "
                << "samples:" << total_samples / (iteration / 2) << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["1-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      int count = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[count % 50], &matchers2[0]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
          count++;
        }
        duration += t.since_start_nano();
      }
      std::cout << "[1-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
    if (query_types["5-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[round % 50], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      std::cout << "[5-1-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
    // hour.
    if (query_types["5-1-12"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < (iteration / 2); round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 43200000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[round % 50], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      std::cout << "[5-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / (iteration / 2)
                << "us "
                << "samples:" << total_samples / (iteration / 2) << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["5-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      int count = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[count % 50], &matchers2[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q.select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
          ++count;
        }
        duration += t.since_start_nano();
      }
      std::cout << "[5-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Last reading of a metric of a host.
    if (query_types["lastpoint"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime, endtime + 1, cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      std::cout << "[lastpoint] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }
  }

  void queryDevOps2(leveldb::DB* db, int64_t endtime,
                    leveldb::Cache* cache = nullptr, int iteration = 1000) {
    int big_iteration = 3;
    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
    // hours.
    if (query_types["1-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[1-1-1] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << ":"
                  << total_samples << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
    // hours.
    if (query_types["1-1-12"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 43200000, endtime,
                               cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[1-1-12] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    if (query_types["1-1-24"] && endtime - 86400000 > -120000) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 86400000, endtime,
                               cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[1-1-24] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    if (query_types["1-1-all"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), 0, endtime, cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[1-1-all] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["1-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[host], &matchers2[0]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[1-8-1] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
    if (query_types["5-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[5-1-1] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
    // hour.
    if (query_types["5-1-12"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 43200000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[5-1-12] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    if (query_types["5-1-24"] && endtime - 86400000 > -120000) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 86400000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[5-1-24] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    if (query_types["5-1-all"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), 0, endtime, cache);
        Timer t;
        t.start();

        for (int j = 0; j < 5; j++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1[0], &matchers2[j]});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q.select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              ++total_samples;
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[5-1-all] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["5-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[host], &matchers2[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q.select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[5-8-1] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    // Last reading of a metric of a host.
    if (query_types["lastpoint"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime, endtime + 1000, cache);
        Timer t;
        t.start();

        std::vector<tsdb::label::MatcherInterface*> matchers(
            {&matchers1[0], &matchers2[0]});
        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            q.select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

          std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
              series->chain_iterator();
          while (it->next()) {
            ++total_samples;
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[lastpoint] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << ":"
                  << total_samples << std::endl;
    }

    // Aggregate across all CPU metrics per hour over 1 hour for eight hosts
    if (query_types["cpu-max-all-8"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), endtime - 3600000, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < 8; host++) {
          for (int j = 0; j < matchers3.size(); j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[host], &matchers3[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q.select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[cpu-max-all-8] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / iteration << "us "
                  << "samples:" << total_samples / iteration << std::endl;
    }

    // Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours
    if (query_types["double-groupby-5"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < big_iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), 0, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < matchers1.size(); host++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[host], &matchers3[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q.select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[double-groupby-5] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / big_iteration << "us "
                  << "samples:" << total_samples / big_iteration << std::endl;
    }

    // Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours
    if (query_types["double-groupby-all"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      for (int round = 0; round < big_iteration; round++) {
        querier::TSDBQuerier q(db, head_.get(), 0, endtime,
                               cache);
        Timer t;
        t.start();

        for (int host = 0; host < matchers1.size(); host++) {
          for (int j = 0; j < matchers3.size(); j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1[host], &matchers3[j]});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q.select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                ++total_samples;
              }
            }
          }
        }
        duration += t.since_start_nano();
      }
      if (iteration > 10)
        std::cout << "[double-groupby-all] duration(total):" << duration / 1000 << "us "
                  << "duration(avg):" << duration / 1000 / big_iteration << "us "
                  << "samples:" << total_samples / big_iteration << std::endl;
    }
  }

  void queryDevOps2Parallel(ThreadPool* pool, int thread_num, leveldb::DB* db,
                            int64_t endtime, leveldb::Cache* cache = nullptr) {
    int iteration = 100000;
    int big_iteration = 64;
    base::WaitGroup wg;

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
    // hours.
    if (query_types["1-1-1"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1->at(0), &matchers2->at(0)});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              total_samples->fetch_add(1);
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 3600000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[1-1-1] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << ":"
                << total_samples.load() << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12
    // hours.
    if (query_types["1-1-12"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1->at(0), &matchers2->at(0)});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              total_samples->fetch_add(1);
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 43200000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[1-1-12] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    if (query_types["1-1-24"] && endtime - 86400000 > -120000) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1->at(0), &matchers2->at(0)});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              total_samples->fetch_add(1);
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 86400000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[1-1-24] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    if (query_types["1-1-all"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1->at(0), &matchers2->at(0)});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              total_samples->fetch_add(1);
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), 0, endtime, cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[1-1-all] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["1-8-1"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int host = 0; host < 8; host++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1->at(host), &matchers2->at(0)});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                total_samples->fetch_add(1);
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 3600000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[1-8-1] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
    if (query_types["5-1-1"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1->at(0), &matchers2->at(j)});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                total_samples->fetch_add(1);
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 3600000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[5-1-1] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12
    // hour.
    if (query_types["5-1-12"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1->at(0), &matchers2->at(j)});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                total_samples->fetch_add(1);
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 43200000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[5-1-12] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    if (query_types["5-1-24"] && endtime - 86400000 > -120000) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1->at(0), &matchers2->at(j)});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                total_samples->fetch_add(1);
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 86400000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[5-1-24] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    if (query_types["5-1-all"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int j = 0; j < 5; j++) {
            std::vector<tsdb::label::MatcherInterface*> matchers(
                {&matchers1->at(0), &matchers2->at(j)});
            std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                q->select(matchers);
            while (ss->next()) {
              std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                  ss->at();

              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->chain_iterator();
              while (it->next()) {
                total_samples->fetch_add(1);
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), 0, endtime, cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[5-1-all] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["5-8-1"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int host = 0; host < 8; host++) {
            for (int j = 0; j < 5; j++) {
              std::vector<tsdb::label::MatcherInterface*> matchers(
                  {&matchers1->at(host), &matchers2->at(j)});
              std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                  q->select(matchers);
              while (ss->next()) {
                std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                    ss->at();

                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  total_samples->fetch_add(1);
                }
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 3600000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[5-8-1] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    // Last reading of a metric of a host.
    if (query_types["lastpoint"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          std::vector<tsdb::label::MatcherInterface*> matchers(
              {&matchers1->at(0), &matchers2->at(0)});
          std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
              q->select(matchers);
          while (ss->next()) {
            std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->chain_iterator();
            while (it->next()) {
              total_samples->fetch_add(1);
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime, endtime + 1000, cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers2,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[lastpoint] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << ":"
                << total_samples.load() << std::endl;
    }

    // Aggregate across all CPU metrics per hour over 1 hour for eight hosts
    if (query_types["cpu-max-all-8"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int host = 0; host < 8; host++) {
            for (int j = 0; j < matchers2->size(); j++) {
              std::vector<tsdb::label::MatcherInterface*> matchers(
                  {&matchers1->at(host), &matchers2->at(j)});
              std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                  q->select(matchers);
              while (ss->next()) {
                std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                    ss->at();

                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  total_samples->fetch_add(1);
                }
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), endtime - 3600000, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers3,
                                iteration / thread_num, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[cpu-max-all-8] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / iteration
                << "us "
                << "samples:" << total_samples.load() / iteration << std::endl;
    }

    // Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours
    if (query_types["double-groupby-5"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int host = 0; host < matchers1->size(); host++) {
            for (int j = 0; j < 5; j++) {
              std::vector<tsdb::label::MatcherInterface*> matchers(
                  {&matchers1->at(host), &matchers2->at(j)});
              std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                  q->select(matchers);
              while (ss->next()) {
                std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                    ss->at();

                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  total_samples->fetch_add(1);
                }
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), 0, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers3,
                                big_iteration / thread_num / 500, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[double-groupby-5] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / big_iteration
                << "us "
                << "samples:" << total_samples.load() / big_iteration << std::endl;
    }

    // Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours
    if (query_types["double-groupby-all"]) {
      std::atomic<int64_t> total_samples(0);
      Timer t;
      t.start();
      auto func = [](std::vector<tsdb::label::EqualMatcher>* matchers1,
                     std::vector<tsdb::label::EqualMatcher>* matchers2,
                     int iteration, std::atomic<int64_t>* total_samples,
                     base::WaitGroup* _wg, querier::TSDBQuerier* q) {
        for (int round = 0; round < iteration; round++) {
          for (int host = 0; host < matchers1->size(); host++) {
            for (int j = 0; j < matchers2->size(); j++) {
              std::vector<tsdb::label::MatcherInterface*> matchers(
                  {&matchers1->at(host), &matchers2->at(j)});
              std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
                  q->select(matchers);
              while (ss->next()) {
                std::unique_ptr<::tsdb::querier::SeriesInterface> series =
                    ss->at();

                std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                    series->chain_iterator();
                while (it->next()) {
                  total_samples->fetch_add(1);
                }
              }
            }
          }
        }
        _wg->done();
      };
      std::vector<querier::TSDBQuerier> queriers;
      queriers.reserve(thread_num);
      for (int i = 0; i < thread_num; i++) {
        wg.add(1);
        queriers.emplace_back(db, head_.get(), 0, endtime,
                              cache);
        pool->enqueue(std::bind(func, &matchers1, &matchers3,
                                big_iteration / thread_num / 500, &total_samples, &wg,
                                &queriers[i]));
      }
      wg.wait();
      std::cout << "[double-groupby-all] duration(total):" << t.since_start_nano() / 1000
                << "us "
                << "duration(avg):" << t.since_start_nano() / 1000 / big_iteration
                << "us "
                << "samples:" << total_samples.load() / big_iteration << std::endl;
    }
  }

  void setup(const std::string& dir, const std::string& snapshot_dir = "",
             bool sync_api = true, leveldb::DB* db = nullptr) {
    ::tsdb::head::Head* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::Head(dir, snapshot_dir, db, sync_api));
    db->SetHead(head_.get());
  }

  int num_ts;
  int tuple_size;
  int num_tuple;
  std::unique_ptr<::tsdb::head::Head> head_;

  std::vector<tsdb::label::EqualMatcher> matchers1;
  std::vector<tsdb::label::EqualMatcher> matchers2;
  std::vector<tsdb::label::EqualMatcher> matchers3;
};

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

void TestInsertAndQuery1(int num, int interval = 30, int numtuple = 90) {
  printf("TestInsertAndQuery1\n");

  bool bg = true;
  size_t sz = sizeof(bg);
  mallctl("opt.background_thread", NULL, 0, &bg, sz);
  ssize_t t = 0;
  sz = sizeof(t);
  mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
  mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 3;
  options.write_buffer_size = 64 * 1024 * 1024;
  options.block_cache = leveldb::NewLRUCache(1024 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  leveldb::DB* ldb;
  if (!leveldb::DB::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, "", true, ldb);

  // tsdbtest.head_->enable_concurrency();
  tsdbtest.head_->bg_clean_samples_logs();
  tsdbtest.head_->set_no_log(true);

  leveldb::Options ms_opts;
  ms_opts.create_if_missing = true;
  ms_opts.use_log = false;
  tsdbtest.head_->set_mergeset_manager(dbpath + "/mergeset", ms_opts);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();

  // HeapProfilerStart("headheap.prof");
  tsdbtest.head_add3(0, interval);
  // HeapProfilerStop();
  d = timer.since_start_nano();
  std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.tuple_size) /
                (double)(d)*1000000000)
            << std::endl;
  // std::cout << "[Labels Insertion duration (ms)]:" << ((d) / 1000000) << "
  // [throughput]:" << ((double)(tsdbtest.num_ts) / (double)(d) * 1000000000) <<
  // std::endl;
  mem_usage(vm, rss);
  std::cout << "[After Labels Insertion] VM:" << (vm / 1024)
            << "MB RSS:" << (rss / 1024) << "MB" << std::endl;

  int sep_period = 5;
  for (int tuple = 1; tuple < tsdbtest.num_tuple; tuple++) {
    tsdbtest.head_add_fast1(tuple * tsdbtest.tuple_size * 1000 * interval,
                            interval);
    if ((tuple + 1) % sep_period == 0) {
      d = timer.since_start_nano();
      std::cout << "[#tuples] " << (tuple + 1) << " [st] "
                << (tuple * tsdbtest.tuple_size * 1000) << std::endl;
      std::cout << "[Insertion duration (ms)]:" << ((d - last_t) / 1000000)
                << " [throughput]:"
                << ((double)(tsdbtest.num_ts) * (double)(sep_period) *
                    (double)(tsdbtest.tuple_size) / (double)(d - last_t) *
                    1000000000)
                << std::endl;
      last_t = d;
    }
  }

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  std::cout << "-------- sleep for 30 seconds before querying ----------" << std::endl;
  sleep(30);
  ldb->PrintLevel();

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;

  uint64_t epoch = 1;
  sz = sizeof(epoch);
  mallctl("epoch", &epoch, &sz, &epoch, sz);

  size_t allocated, active, mapped;
  sz = sizeof(size_t);
  mallctl("stats.allocated", &allocated, &sz, NULL, 0);
  mallctl("stats.active", &active, &sz, NULL, 0);
  mallctl("stats.mapped", &mapped, &sz, NULL, 0);
  printf("allocated/active/mapped (MB): %zu/%zu/%zu\n", allocated/1024/1024, active/1024/1024, mapped/1024/1024);

  // leveldb::Cache* cache = leveldb::NewLRUCache(1024 * 1024 * 1024);
  leveldb::Cache* cache = nullptr;
  ProfilerStart("dbtest.prof");
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  ProfilerStop();
  // std::cout << "TotalCharge:" << cache->TotalCharge() << std::endl;
  delete cache;
  delete ldb;
  // options.ccache->print_summary(true);
}

void TestParallelInsertAndQuery1(int thread_num, int num, int interval = 30,
                                 int numtuple = 90, int query_thread_num = 0) {
  printf("TestParallelInsertAndQuery1\n");

  bool bg = true;
  size_t sz = sizeof(bg);
  mallctl("opt.background_thread", NULL, 0, &bg, sz);
  ssize_t t = 0;
  sz = sizeof(t);
  mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
  mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

  // leveldb::L0_MERGE_FACTOR = 4;
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 3;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.max_file_size = 256 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  leveldb::DB* ldb;
  if (!leveldb::DB::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, "", true, ldb);

  leveldb::Options ms_opts;
  ms_opts.create_if_missing = true;
  ms_opts.use_log = false;
  tsdbtest.head_->set_mergeset_manager(dbpath + "/mergeset", ms_opts);

  // head::MEM_TUPLE_SIZE = 300;
  // leveldb::PARTITION_LENGTH = 4 * 3600 * 1000;

  ThreadPool pool(thread_num >= query_thread_num ? thread_num
                                                 : query_thread_num);
  base::WaitGroup wg;
  std::vector<std::unique_ptr<db::AppenderInterface>> apps;
  apps.reserve(thread_num);

  std::vector<label::Labels> lsets;
  lsets.reserve(num);
  tsdbtest.get_devops_labels(&lsets);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();

  auto func = [](db::AppenderInterface* app,
                 std::vector<tsdb::label::Labels>* _lsets, int left, int right,
                 base::WaitGroup* _wg, int num_tuple, int tuple_size,
                 int interval, Timer* timer, int tid) {
    std::vector<uint64_t> tsids;
    tsids.reserve(right - left);

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    for (int i = left; i < right; i++) {
      auto r = app->add(std::move(_lsets->at(i)), tfluc[0], tfluc[0]);
      for (int k = 1; k < tuple_size; k++)
        app->add_fast(r.first, k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
      tsids.push_back(r.first);
    }

    int64_t d = timer->since_start_nano(), last_t = 0;
    printf(
        "[thread id]:%d [Labels Insertion duration (ms)]:%ld [throughput]:%f\n",
        tid, d / 1000000,
        (double)(right - left) * (double)(tuple_size) / (double)(d)*1000000000);

    for (int tuple = 1; tuple < num_tuple; tuple++) {
      tfluc.clear();
      for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
      int64_t st = tuple * tuple_size * 1000 * interval;
      for (int i = 0; i < right - left; i++) {
        for (int k = 0; k < tuple_size; k++)
          app->add_fast(tsids[i], st + k * interval * 1000 + tfluc[k],
                        tfluc[k]);
        app->commit();
      }

      if ((tuple + 1) % 15 == 0) {
        d = timer->since_start_nano();
        printf(
            "[thread id]:%d [#tuples]:%d [st]:%d [Insertion duration (ms)]:%ld "
            "[throughput]:%f\n",
            tid, tuple + 1, tuple * tuple_size * 1000, (d - last_t) / 1000000,
            (double)(right - left) * (double)(15) * (double)(tuple_size) /
                (double)(d - last_t) * 1000000000);
        last_t = d;
      }
    }

    _wg->done();
  };

  // ProfilerStart("dbtest.prof");
  for (int i = 0; i < thread_num; i++) {
    wg.add(1);
    apps.push_back(std::move(tsdbtest.head_->appender()));
    pool.enqueue(std::bind(
        func, apps.back().get(), &lsets, i * lsets.size() / thread_num,
        (i + 1) * lsets.size() / thread_num, &wg, tsdbtest.num_tuple,
        tsdbtest.tuple_size, interval, &timer, i));
  }
  wg.wait();
  // ProfilerStop();

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(60);
  ldb->PrintLevel();

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
  std::cout << "inverted index size:"
            << tsdbtest.head_->inverted_index()->mem_postings()->mem_size()
            << std::endl;

  uint64_t epoch = 1;
  sz = sizeof(epoch);
  mallctl("epoch", &epoch, &sz, &epoch, sz);

  size_t allocated, active, mapped;
  sz = sizeof(size_t);
  mallctl("stats.allocated", &allocated, &sz, NULL, 0);
  mallctl("stats.active", &active, &sz, NULL, 0);
  mallctl("stats.mapped", &mapped, &sz, NULL, 0);
  printf("allocated/active/mapped (MB): %zu/%zu/%zu\n", allocated/1024/1024, active/1024/1024, mapped/1024/1024);

  leveldb::Cache* cache = leveldb::NewLRUCache(64 * 1024 * 1024);
  // ProfilerStart("dbtest.prof");
  if (query_thread_num == 0)
    tsdbtest.queryDevOps2(
        ldb,
        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
            (tsdbtest.tuple_size - 1) * 1000 * interval,
        cache);
  else
    tsdbtest.queryDevOps2Parallel(
        &pool, query_thread_num, ldb,
        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
            (tsdbtest.tuple_size - 1) * 1000 * interval,
        cache);
  sleep(3);
  if (query_thread_num == 0)
    tsdbtest.queryDevOps2(
        ldb,
        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
            (tsdbtest.tuple_size - 1) * 1000 * interval,
        cache);
  else
    tsdbtest.queryDevOps2Parallel(
        &pool, query_thread_num, ldb,
        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
            (tsdbtest.tuple_size - 1) * 1000 * interval,
        cache);
  sleep(3);
  if (query_thread_num == 0)
    tsdbtest.queryDevOps2(
        ldb,
        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
            (tsdbtest.tuple_size - 1) * 1000 * interval,
        cache);
  else
    tsdbtest.queryDevOps2Parallel(
        &pool, query_thread_num, ldb,
        (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
            (tsdbtest.tuple_size - 1) * 1000 * interval,
        cache);
  // ProfilerStop();
  std::cout << "TotalCharge:" << cache->TotalCharge() << std::endl;
  delete cache;
  delete ldb;
  // options.ccache->print_summary(true);
}

void TestParallelInsertAndQuery1_2(int thread_num, int num, int interval = 30,
                                   int numtuple = 90) {
  printf("TestParallelInsertAndQuery1_2\n");

  bool bg = true;
  size_t sz = sizeof(bg);
  mallctl("opt.background_thread", NULL, 0, &bg, sz);
  ssize_t t = 0;
  sz = sizeof(t);
  mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
  mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

  // leveldb::L0_MERGE_FACTOR = 4;
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 3;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.max_file_size = 256 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  leveldb::DB* ldb;
  if (!leveldb::DB::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, "", true, ldb);

  leveldb::Options ms_opts;
  ms_opts.create_if_missing = true;
  ms_opts.use_log = false;
  tsdbtest.head_->set_mergeset_manager(dbpath + "/mergeset", ms_opts);

  // head::MEM_TUPLE_SIZE = 300;
  // leveldb::PARTITION_LENGTH = 4 * 3600 * 1000;

  ThreadPool pool(thread_num);
  base::WaitGroup wg;
  std::vector<std::unique_ptr<db::AppenderInterface>> apps;
  apps.reserve(thread_num);

  std::vector<label::Labels> lsets;
  lsets.reserve(num);
  tsdbtest.get_devops_labels(&lsets);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  {
    auto app = tsdbtest.head_->appender();
    for (int i = 0; i < lsets.size(); i++) {
      app->add(std::move(lsets[i]), 0, 0);
      app->commit();
    }
  }

  Timer timer;
  timer.start();

  auto func = [](db::AppenderInterface* app, int left, int right,
                 base::WaitGroup* _wg, int num_tuple, int tuple_size,
                 int interval, Timer* timer, int tid) {
    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
    for (int i = left; i < right; i++) {
      for (int k = 1; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
    }

    int64_t d = timer->since_start_nano(), last_t = 0;
    printf(
        "[thread id]:%d [Labels Insertion duration (ms)]:%ld [throughput]:%f\n",
        tid, d / 1000000,
        (double)(right - left) * (double)(tuple_size) / (double)(d)*1000000000);

    for (int tuple = 1; tuple < num_tuple; tuple++) {
      tfluc.clear();
      for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
      int64_t st = tuple * tuple_size * 1000 * interval;
      for (int i = left; i < right; i++) {
        for (int k = 0; k < tuple_size; k++)
          app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                            (uint64_t)(i % PRESERVED_BLOCKS),
                        st + k * interval * 1000 + tfluc[k], tfluc[k]);
        app->commit();
      }

      if ((tuple + 1) % 15 == 0) {
        d = timer->since_start_nano();
        printf(
            "[thread id]:%d [#tuples]:%d [st]:%d [Insertion duration (ms)]:%ld "
            "[throughput]:%f\n",
            tid, tuple + 1, tuple * tuple_size * 1000, (d - last_t) / 1000000,
            (double)(right - left) * (double)(15) * (double)(tuple_size) /
                (double)(d - last_t) * 1000000000);
        last_t = d;
      }
    }

    _wg->done();
  };

  for (int i = 0; i < thread_num; i++) {
    wg.add(1);
    apps.push_back(std::move(tsdbtest.head_->appender()));
    pool.enqueue(
        std::bind(func, apps.back().get(), i * lsets.size() / thread_num,
                  (i + 1) * lsets.size() / thread_num, &wg, tsdbtest.num_tuple,
                  tsdbtest.tuple_size, interval, &timer, i));
  }
  wg.wait();

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(60);
  ldb->PrintLevel();

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
  std::cout << "inverted index size:"
            << tsdbtest.head_->inverted_index()->mem_postings()->mem_size()
            << std::endl;

  uint64_t epoch = 1;
  sz = sizeof(epoch);
  mallctl("epoch", &epoch, &sz, &epoch, sz);

  size_t allocated, active, mapped;
  sz = sizeof(size_t);
  mallctl("stats.allocated", &allocated, &sz, NULL, 0);
  mallctl("stats.active", &active, &sz, NULL, 0);
  mallctl("stats.mapped", &mapped, &sz, NULL, 0);
  printf("allocated/active/mapped (MB): %zu/%zu/%zu\n", allocated/1024/1024, active/1024/1024, mapped/1024/1024);

  leveldb::Cache* cache = leveldb::NewLRUCache(64 * 1024 * 1024);
  // ProfilerStart("dbtest.prof");
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  // ProfilerStop();
  std::cout << "TotalCharge:" << cache->TotalCharge() << std::endl;
  delete cache;
  delete ldb;
  // options.ccache->print_summary(true);
}

// open background jobs.
void TestParallelInsertAndQuery2(int thread_num, int num, int interval = 30,
                                 int numtuple = 90) {
  printf("TestParallelInsertAndQuery2\n");

  bool bg = true;
  size_t sz = sizeof(bg);
  mallctl("opt.background_thread", NULL, 0, &bg, sz);
  ssize_t t = 0;
  sz = sizeof(t);
  mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
  mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

  // leveldb::L0_MERGE_FACTOR = 4;
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 3;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.max_file_size = 256 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  leveldb::DB* ldb;
  if (!leveldb::DB::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, "", true, ldb);

  ThreadPool pool(thread_num);
  base::WaitGroup wg;
  std::vector<std::unique_ptr<db::AppenderInterface>> apps;
  apps.reserve(thread_num);

  std::vector<label::Labels> lsets;
  lsets.reserve(num);
  tsdbtest.get_devops_labels(&lsets);

  tsdbtest.head_->enable_concurrency();
  tsdbtest.head_->bg_clean_samples_logs();
  // tsdbtest.head_->set_mem_to_disk_migration_threshold(0);
  // tsdbtest.head_->enable_migration();
  // tsdbtest.head_->set_no_log(true);

  leveldb::Options ms_opts;
  ms_opts.create_if_missing = true;
  ms_opts.use_log = false;
  tsdbtest.head_->set_mergeset_manager(dbpath + "/mergeset", ms_opts);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();

  auto func = [](db::AppenderInterface* app,
                 std::vector<tsdb::label::Labels>* _lsets, int left, int right,
                 base::WaitGroup* _wg, int num_tuple, int tuple_size,
                 int interval, Timer* timer, int tid, head::Head* head) {
    std::vector<uint64_t> tsids;
    tsids.reserve(right - left);

    int gcid;
    head->register_thread(&gcid);
    printf("register_thread:%d\n", gcid);

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);

    head->update_local_epoch(gcid);
    uint64_t epoch = head->get_epoch(gcid);
    printf("epoch:%lu\n", epoch);

    // HeapProfilerStart("dbtest");
    for (int i = left; i < right; i++) {
      auto r = app->add(std::move(_lsets->at(i)), tfluc[0], tfluc[0], epoch);
      for (int k = 1; k < tuple_size; k++)
        app->add_fast(r.first, k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
      tsids.push_back(r.first);
    }
    // HeapProfilerStop();

    head->update_local_epoch(gcid);
    head->deregister_thread(gcid);
    printf("deregister_thread:%d\n", gcid);

    int64_t d = timer->since_start_nano(), last_t = 0;
    printf(
        "[thread id]:%d [Labels Insertion duration (ms)]:%ld [throughput]:%f\n",
        tid, d / 1000000,
        (double)(right - left) * (double)(tuple_size) / (double)(d)*1000000000);

    for (int tuple = 1; tuple < num_tuple; tuple++) {
      tfluc.clear();
      for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
      int64_t st = tuple * tuple_size * 1000 * interval;
      for (int i = 0; i < right - left; i++) {
        for (int k = 0; k < tuple_size; k++)
          app->add_fast(tsids[i], st + k * interval * 1000 + tfluc[k],
                        tfluc[k]);
        app->commit();
      }

      if ((tuple + 1) % 15 == 0) {
        d = timer->since_start_nano();
        printf(
            "[thread id]:%d [#tuples]:%d [st]:%d [Insertion duration (ms)]:%ld "
            "[throughput]:%f\n",
            tid, tuple + 1, tuple * tuple_size * 1000, (d - last_t) / 1000000,
            (double)(right - left) * (double)(15) * (double)(tuple_size) /
                (double)(d - last_t) * 1000000000);
        last_t = d;
      }
    }

    _wg->done();
  };

  for (int i = 0; i < thread_num; i++) {
    wg.add(1);
    apps.push_back(std::move(tsdbtest.head_->appender()));
    pool.enqueue(std::bind(
        func, apps.back().get(), &lsets, i * lsets.size() / thread_num,
        (i + 1) * lsets.size() / thread_num, &wg, tsdbtest.num_tuple,
        tsdbtest.tuple_size, interval, &timer, i, tsdbtest.head_.get()));
  }
  wg.wait();

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;

  sleep(60);
  tsdbtest.head_->set_mem_to_disk_migration_threshold(2lu * 1024lu * 1024lu *
                                                      1024lu);
  tsdbtest.head_->enable_migration();
  sleep(60);
  tsdbtest.head_->disable_migration();
  // tsdbtest.head_->full_migrate();
  sleep(60);
  ldb->PrintLevel();

  printf("Garbage counter: %lu\n", head::_garbage_counter);
  printf("MS GC counter: %lu\n", head::_ms_gc_counter.load());
  printf("PL GC counter: %lu\n", mem::_pl_gc_counter.load());
  printf("ART GC counter: %lu\n", mem::_art_gc_counter.load());

  double vm2, rss2;
  mem_usage(vm2, rss2);
  std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
            << std::endl;
  std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
            << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
  std::cout << "inverted index size:"
            << tsdbtest.head_->inverted_index()->mem_postings()->mem_size()
            << std::endl;

  uint64_t epoch = 1;
  sz = sizeof(epoch);
  mallctl("epoch", &epoch, &sz, &epoch, sz);

  size_t allocated, active, mapped;
  sz = sizeof(size_t);
  mallctl("stats.allocated", &allocated, &sz, NULL, 0);
  mallctl("stats.active", &active, &sz, NULL, 0);
  mallctl("stats.mapped", &mapped, &sz, NULL, 0);
  printf("allocated/active/mapped (MB): %zu/%zu/%zu\n", allocated/1024/1024, active/1024/1024, mapped/1024/1024);

  leveldb::Cache* cache = leveldb::NewLRUCache(64 * 1024 * 1024);
  // ProfilerStart("dbtest.prof");
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  sleep(3);
  tsdbtest.queryDevOps2(
      ldb,
      (tsdbtest.num_tuple - 1) * tsdbtest.tuple_size * 1000 * interval +
          (tsdbtest.tuple_size - 1) * 1000 * interval,
      cache);
  // ProfilerStop();
  std::cout << "TotalCharge:" << cache->TotalCharge() << std::endl;
  delete cache;
  delete ldb;
  // options.ccache->print_summary(true);
}

// Mixed parallel write & read.
void TestParallelInsertAndQuery3(int thread_num, int num, int interval = 30,
                                 int numtuple = 90) {
  printf("TestParallelInsertAndQuery3\n");

  bool bg = true;
  size_t sz = sizeof(bg);
  mallctl("opt.background_thread", NULL, 0, &bg, sz);
  ssize_t t = 0;
  sz = sizeof(t);
  mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
  mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

  // leveldb::L0_MERGE_FACTOR = 4;
  std::string dbpath = "/tmp/tsdb_big";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 3;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.max_file_size = 256 * 1024 * 1024;
  // options.block_cache = NewLRUCache(64 * 1024 * 1024);
  // options.sample_cache = NewLRUCache(64 * 1024 * 1024);

  leveldb::DB* ldb;
  if (!leveldb::DB::Open(options, dbpath, &ldb).ok()) {
    std::cout << "Cannot open DB" << std::endl;
    exit(-1);
  }

  TSDBTest tsdbtest;
  tsdbtest.set_parameters(num, 32, numtuple);
  tsdbtest.setup(dbpath, "", true, ldb);

  ThreadPool pool(thread_num);
  base::WaitGroup wg;
  std::vector<std::unique_ptr<db::AppenderInterface>> apps;
  apps.reserve(thread_num);

  std::vector<label::Labels> lsets;
  lsets.reserve(num);
  tsdbtest.get_devops_labels(&lsets);

  tsdbtest.head_->enable_concurrency();
  tsdbtest.head_->bg_clean_samples_logs();
  // tsdbtest.head_->set_mem_to_disk_migration_threshold(0);
  // tsdbtest.head_->enable_migration();

  leveldb::Options ms_opts;
  ms_opts.create_if_missing = true;
  ms_opts.use_log = false;
  tsdbtest.head_->set_mergeset_manager(dbpath + "/mergeset", ms_opts);

  int64_t last_t = 0, d;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();

  leveldb::Cache* cache = leveldb::NewLRUCache(64 * 1024 * 1024);

  auto func = [](db::AppenderInterface* app,
                 std::vector<tsdb::label::Labels>* _lsets, int left, int right,
                 base::WaitGroup* _wg, int num_tuple, int tuple_size,
                 int interval, Timer* timer, int tid, head::Head* head,
                 TSDBTest* tsdbtest, leveldb::DB* ldb, leveldb::Cache* cache) {
    std::vector<uint64_t> tsids;
    tsids.reserve(right - left);

    int gcid;
    head->register_thread(&gcid);
    printf("register_thread:%d\n", gcid);

    std::vector<int64_t> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);

    head->update_local_epoch(gcid);
    uint64_t epoch = head->get_epoch(gcid);
    printf("epoch:%lu\n", epoch);

    // HeapProfilerStart("dbtest");
    for (int i = left; i < right; i++) {
      auto r = app->add(std::move(_lsets->at(i)), tfluc[0], tfluc[0], epoch);
      for (int k = 1; k < tuple_size; k++)
        app->add_fast(r.first, k * interval * 1000 + tfluc[k], tfluc[k]);
      app->commit();
      tsids.push_back(r.first);
    }
    tsdbtest->queryDevOps2(
      ldb,
      (tsdbtest->num_tuple - 1) * tsdbtest->tuple_size * 1000 * interval +
          (tsdbtest->tuple_size - 1) * 1000 * interval,
      cache, 1);
    // HeapProfilerStop();

    head->update_local_epoch(gcid);
    head->deregister_thread(gcid);
    printf("deregister_thread:%d\n", gcid);

    int64_t d = timer->since_start_nano(), last_t = 0;
    printf(
        "[thread id]:%d [Labels Insertion duration (ms)]:%ld [throughput]:%f\n",
        tid, d / 1000000,
        (double)(right - left) * (double)(tuple_size) / (double)(d)*1000000000);

    for (int tuple = 1; tuple < num_tuple; tuple++) {
      tfluc.clear();
      for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
      int64_t st = tuple * tuple_size * 1000 * interval;
      for (int i = 0; i < right - left; i++) {
        for (int k = 0; k < tuple_size; k++)
          app->add_fast(tsids[i], st + k * interval * 1000 + tfluc[k],
                        tfluc[k]);
        app->commit();
      }
      tsdbtest->queryDevOps2(
        ldb,
        (tsdbtest->num_tuple - 1) * tsdbtest->tuple_size * 1000 * interval +
            (tsdbtest->tuple_size - 1) * 1000 * interval,
        cache, 1);

      if ((tuple + 1) % 15 == 0) {
        d = timer->since_start_nano();
        printf(
            "[thread id]:%d [#tuples]:%d [st]:%d [Insertion duration (ms)]:%ld "
            "[throughput]:%f\n",
            tid, tuple + 1, tuple * tuple_size * 1000, (d - last_t) / 1000000,
            (double)(right - left) * (double)(15) * (double)(tuple_size) /
                (double)(d - last_t) * 1000000000);
        last_t = d;
      }
    }

    _wg->done();
  };

  for (int i = 0; i < thread_num; i++) {
    wg.add(1);
    apps.push_back(std::move(tsdbtest.head_->appender()));
    pool.enqueue(std::bind(
        func, apps.back().get(), &lsets, i * lsets.size() / thread_num,
        (i + 1) * lsets.size() / thread_num, &wg, tsdbtest.num_tuple,
        tsdbtest.tuple_size, interval, &timer, i, tsdbtest.head_.get(),
        &tsdbtest, ldb, cache));
  }
  wg.wait();

  auto im = tsdbtest.head_->indirection_manager();
  auto v = im->get_ids();
  for (auto p : v) {
    assert((p.second >> 63) == 0);
    if (((head::MemSeries*)(p.second))->ref == 0)
      std::cout << p.first << " " << ((head::MemSeries*)(p.second))->logical_id << std::endl;
  }

  d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
            << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                   (double)(tsdbtest.tuple_size)
            << " [throughput]:"
            << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
            << std::endl;
  delete cache;
  delete ldb;
  // options.ccache->print_summary(true);
}


}  // namespace db
}  // namespace tsdb

int main(int argc, char* argv[]) {
  if (argc == 4)
    tsdb::db::TestInsertAndQuery1(std::stoi(argv[1]), std::stoi(argv[2]),
                                  std::stoi(argv[3]));
  else if (argc == 5)
    tsdb::db::TestParallelInsertAndQuery1(
        std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]),
        std::stoi(argv[4]));
  else if (argc == 6)
    tsdb::db::TestParallelInsertAndQuery1(
        std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]),
        std::stoi(argv[4]), std::stoi(argv[5]));
  else if (argc == 3)
    tsdb::db::TestParallelInsertAndQuery3(std::stoi(argv[1]),
                                          std::stoi(argv[2]));
  else
    tsdb::db::TestInsertAndQuery1(std::stoi(argv[1]));
}