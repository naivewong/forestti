#include <gperftools/profiler.h>
#include <snappy.h>

#include "db/DB.hpp"
#include "db/HttpParser.hpp"
#include "label/EqualMatcher.hpp"
#include "leveldb/db.h"
#include "leveldb/third_party/thread_pool.h"
#include "third_party/httplib.h"
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
                                                   {"1-1-12", false},
                                                   {"1-1-24", true},
                                                   {"1-8-1", true},
                                                   {"5-1-1", true},
                                                   {"5-1-12", false},
                                                   {"5-1-24", true},
                                                   {"5-8-1", true},
                                                   {"double-groupby-1", false},
                                                   {"high-cpu-1", false},
                                                   {"high-cpu-all", false},
                                                   {"lastpoint", true}});

namespace tsdb {
namespace db {

int _test_num_ts = 10000;

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

class DBTest : public testing::Test {
 public:
  void head_add_proto(httplib::Client* cli, int64_t st, int64_t off = 0) {
    for (int i = 0; i < num_ts; i++) {
      InsertSamples samples;
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      Add(&samples, lset, 0, 0 + off);

      for (int k = 1; k < tuple_size; k++)
        Add(&samples, i, st + k * 1000, st + k * 1000 + off);

      std::string data, compressed_data;
      samples.SerializeToString(&data);
      snappy::Compress(data.data(), data.size(), &compressed_data);
      cli->Post("/insert", compressed_data, "text/plain");
    }
  }

  void head_add_fast_proto(httplib::Client* cli, int64_t st, int64_t off = 0) {
    for (int i = 0; i < num_ts; i++) {
      InsertSamples samples;
      for (int k = 0; k < tuple_size; k++)
        Add(&samples, i, st + k * 1000, st + k * 1000 + off);

      std::string data, compressed_data;
      samples.SerializeToString(&data);
      snappy::Compress(data.data(), data.size(), &compressed_data);
      cli->Post("/insert", compressed_data, "text/plain");
    }
  }

  void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
    num_ts = num_ts_;
    tuple_size = tuple_size_;
    num_tuple = num_tuple_;
    head::MEM_TUPLE_SIZE = tuple_size_;
  }

  void queryDevOpsProto(DB* db, int64_t endtime) {
    tsdb::label::Labels matchers1;
    for (int i = 0; i < 50; i++)
      matchers1.emplace_back("hostname", "host_" + std::to_string(i));
    tsdb::label::Labels matchers2({{"__name__", "cpu_usage_user"},
                                   {"__name__", "diskio_reads"},
                                   {"__name__", "kernel_boot_time"},
                                   {"__name__", "mem_total"},
                                   {"__name__", "net_bytes_sent"}});

    httplib::Client cli("127.0.0.1", 9966);
    std::string request_data, data;
    int iteration = 1000;

    // Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1
    // hours.
    if (query_types["1-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        QueryRequest req;
        Add(&req, true, {matchers1[0], matchers2[0]}, endtime - 3600000 + 1,
            endtime);
        request_data.clear();
        req.SerializeToString(&request_data);
        int64_t tmp = t.since_start_nano();
        auto res = cli.Post("/query", request_data, "text/plain");
        duration += t.since_start_nano() - tmp;

        data.clear();
        snappy::Uncompress(res->body.data(), res->body.size(), &data);
        QueryResults results;
        results.ParseFromString(data);
        for (int i = 0; i < results.results_size(); i++) {
          if (results.results(i).values_size() > 0)
            total_samples += results.results(i).values_size();
        }
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
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        QueryRequest req;
        Add(&req, true, {matchers1[0], matchers2[0]}, endtime - 43200000 + 1,
            endtime);
        request_data.clear();
        req.SerializeToString(&request_data);
        int64_t tmp = t.since_start_nano();
        auto res = cli.Post("/query", request_data, "text/plain");
        duration += t.since_start_nano() - tmp;

        data.clear();
        snappy::Uncompress(res->body.data(), res->body.size(), &data);
        QueryResults results;
        results.ParseFromString(data);
        for (int i = 0; i < results.results_size(); i++) {
          if (results.results(i).values_size() > 0)
            total_samples += results.results(i).values_size();
        }
      }
      std::cout << "[1-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    if (query_types["1-1-24"] && endtime - 86400000 > -120000) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        QueryRequest req;
        Add(&req, true, {matchers1[0], matchers2[0]}, endtime - 86400000 + 1,
            endtime);
        request_data.clear();
        req.SerializeToString(&request_data);
        int64_t tmp = t.since_start_nano();
        auto res = cli.Post("/query", request_data, "text/plain");
        duration += t.since_start_nano() - tmp;

        data.clear();
        snappy::Uncompress(res->body.data(), res->body.size(), &data);
        QueryResults results;
        results.ParseFromString(data);
        for (int i = 0; i < results.results_size(); i++) {
          if (results.results(i).values_size() > 0)
            total_samples += results.results(i).values_size();
        }
      }
      std::cout << "[1-1-24] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["1-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        for (int host = 0; host < 8; host++) {
          QueryRequest req;
          Add(&req, true, {matchers1[host], matchers2[0]},
              endtime - 3600000 + 1, endtime);
          request_data.clear();
          req.SerializeToString(&request_data);
          int64_t tmp = t.since_start_nano();
          auto res = cli.Post("/query", request_data, "text/plain");
          duration += t.since_start_nano() - tmp;

          data.clear();
          snappy::Uncompress(res->body.data(), res->body.size(), &data);
          QueryResults results;
          results.ParseFromString(data);
          for (int i = 0; i < results.results_size(); i++) {
            if (results.results(i).values_size() > 0)
              total_samples += results.results(i).values_size();
          }
        }
      }
      std::cout << "[1-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
    if (query_types["5-1-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        for (int j = 0; j < 5; j++) {
          QueryRequest req;
          Add(&req, true, {matchers1[0], matchers2[j]}, endtime - 3600000 + 1,
              endtime);
          request_data.clear();
          req.SerializeToString(&request_data);
          int64_t tmp = t.since_start_nano();
          auto res = cli.Post("/query", request_data, "text/plain");
          duration += t.since_start_nano() - tmp;

          data.clear();
          snappy::Uncompress(res->body.data(), res->body.size(), &data);
          QueryResults results;
          results.ParseFromString(data);
          for (int i = 0; i < results.results_size(); i++) {
            if (results.results(i).values_size() > 0)
              total_samples += results.results(i).values_size();
          }
        }
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
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        for (int j = 0; j < 5; j++) {
          QueryRequest req;
          Add(&req, true, {matchers1[0], matchers2[j]}, endtime - 43200000 + 1,
              endtime);
          request_data.clear();
          req.SerializeToString(&request_data);
          int64_t tmp = t.since_start_nano();
          auto res = cli.Post("/query", request_data, "text/plain");
          duration += t.since_start_nano() - tmp;

          data.clear();
          snappy::Uncompress(res->body.data(), res->body.size(), &data);
          QueryResults results;
          results.ParseFromString(data);
          for (int i = 0; i < results.results_size(); i++) {
            if (results.results(i).values_size() > 0)
              total_samples += results.results(i).values_size();
          }
        }
      }
      std::cout << "[5-1-12] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    if (query_types["5-1-24"] && endtime - 86400000 > -120000) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        for (int j = 0; j < 5; j++) {
          QueryRequest req;
          Add(&req, true, {matchers1[0], matchers2[j]}, endtime - 86400000 + 1,
              endtime);
          request_data.clear();
          req.SerializeToString(&request_data);
          int64_t tmp = t.since_start_nano();
          auto res = cli.Post("/query", request_data, "text/plain");
          duration += t.since_start_nano() - tmp;

          data.clear();
          snappy::Uncompress(res->body.data(), res->body.size(), &data);
          QueryResults results;
          results.ParseFromString(data);
          for (int i = 0; i < results.results_size(); i++) {
            if (results.results(i).values_size() > 0)
              total_samples += results.results(i).values_size();
          }
        }
      }
      std::cout << "[5-1-24] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1
    // hour.
    if (query_types["5-8-1"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        for (int host = 0; host < 8; host++) {
          for (int j = 0; j < 5; j++) {
            QueryRequest req;
            Add(&req, true, {matchers1[host], matchers2[j]},
                endtime - 3600000 + 1, endtime);
            request_data.clear();
            req.SerializeToString(&request_data);
            int64_t tmp = t.since_start_nano();
            auto res = cli.Post("/query", request_data, "text/plain");
            duration += t.since_start_nano() - tmp;

            data.clear();
            snappy::Uncompress(res->body.data(), res->body.size(), &data);
            QueryResults results;
            results.ParseFromString(data);
            for (int i = 0; i < results.results_size(); i++) {
              if (results.results(i).values_size() > 0)
                total_samples += results.results(i).values_size();
            }
          }
        }
      }
      std::cout << "[5-8-1] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }

    // Last reading of a metric of a host.
    if (query_types["lastpoint"]) {
      int64_t total_samples = 0;
      int64_t duration = 0;
      Timer t;
      t.start();
      for (int round = 0; round < iteration; round++) {
        QueryRequest req;
        Add(&req, true, {matchers1[0], matchers2[0]}, endtime, endtime + 1);
        request_data.clear();
        req.SerializeToString(&request_data);
        int64_t tmp = t.since_start_nano();
        auto res = cli.Post("/query", request_data, "text/plain");
        duration += t.since_start_nano() - tmp;

        data.clear();
        snappy::Uncompress(res->body.data(), res->body.size(), &data);
        QueryResults results;
        results.ParseFromString(data);
        for (int i = 0; i < results.results_size(); i++) {
          if (results.results(i).values_size() > 0)
            total_samples += results.results(i).values_size();
        }
      }
      std::cout << "[lastpoint] duration(total):" << duration / 1000 << "us "
                << "duration(avg):" << duration / 1000 / iteration << "us "
                << "samples:" << total_samples / iteration << std::endl;
    }
  }

  void load_devops_labels(std::vector<label::Labels>* lsets) {
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

  void load_devops_labels2(std::vector<label::Labels>* lsets) {
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
  int tuple_size;
  int num_tuple;
};

TEST_F(DBTest, TestProto1) {
  std::string dbpath = "/tmp/tsdb";
  boost::filesystem::remove_all(dbpath);
  leveldb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  leveldb::DB* ldb;
  ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());
  set_parameters(10, 16, 300);

  DB db(dbpath, ldb);
  sleep(1);

  httplib::Client cli("127.0.0.1", 9966);

  head_add_proto(&cli, 0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast_proto(&cli, tuple * tuple_size * 1000);
  }

  querier::TSDBQuerier* q =
      new querier::TSDBQuerier(ldb, db.head(), 1600000, 4200000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  uint64_t tsid = 0;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4201);

    tsid++;
  }
  ASSERT_EQ(num_ts, tsid);
  delete q;
}

TEST_F(DBTest, TestProto2) {
  std::string dbpath = "/tmp/tsdb";
  boost::filesystem::remove_all(dbpath);
  leveldb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.

  leveldb::DB* ldb;
  ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());
  set_parameters(10, 16, 300);

  DB db(dbpath, ldb);
  sleep(1);

  httplib::Client cli("127.0.0.1", 9966);

  head_add_proto(&cli, 0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast_proto(&cli, tuple * tuple_size * 1000);
  }

  QueryRequest request;
  Add(&request, true, label::Labels({{"label_all", "label_all"}}), 1600000,
      4200000);
  std::string request_data;
  request.SerializeToString(&request_data);
  auto res = cli.Post("/query", request_data, "text/plain");

  std::string data;
  snappy::Uncompress(res->body.data(), res->body.size(), &data);
  QueryResults results;
  results.ParseFromString(data);

  ASSERT_EQ(num_ts, results.results_size());

  std::vector<int64_t> timestamps;
  std::vector<double> values;
  for (int i = 1600; i < 4201; i++) {
    timestamps.push_back(i * 1000);
    values.push_back(i * 1000);
  }

  uint64_t tsid = 0;
  for (int i = 0; i < results.results_size(); i++) {
    QueryResult* result = results.mutable_results(i);
    uint64_t id = result->id();

    ASSERT_EQ(tsid, id);
    label::Labels lset1;
    std::vector<int64_t> t;
    std::vector<double> v;
    Parse(result, &lset1, &id, &t, &v);

    ::tsdb::label::Labels lset2;
    for (int j = 0; j < 10; j++)
      lset2.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid));
    lset2.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset2, lset1);
    ASSERT_EQ(timestamps, t);
    ASSERT_EQ(values, v);

    tsid++;
  }
}

TEST_F(DBTest, TestProtoRepeatedAdd) {
  std::string dbpath = "/tmp/tsdb";
  boost::filesystem::remove_all(dbpath);
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::DB* ldb;
  ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());
  set_parameters(10, 16, 300);
  int num_labels = 20;

  DB db(dbpath, ldb);
  sleep(1);

  httplib::Client cli("127.0.0.1", 9966);

  std::vector<::tsdb::label::Labels> lsets;
  lsets.reserve(num_ts);
  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
    lsets.push_back(std::move(lset));
  }

  InsertSamples samples;
  for (int i = 0; i < num_ts; i++) Add(&samples, lsets[i], 0, 0);

  std::string data, compressed_data;
  samples.SerializeToString(&data);
  snappy::Compress(data.data(), data.size(), &compressed_data);
  auto res = cli.Post("/insert", compressed_data, "text/plain");
  InsertResults results;
  results.ParseFromString(res->body);
  ASSERT_EQ(num_ts, results.results_size());
  for (int i = 0; i < num_ts; i++) ASSERT_EQ(i, results.results(i).id());

  samples.Clear();
  for (int i = 0; i < num_ts; i++) Add(&samples, lsets[i], 0, 0);
  data.clear();
  compressed_data.clear();
  samples.SerializeToString(&data);
  snappy::Compress(data.data(), data.size(), &compressed_data);
  res = cli.Post("/insert", compressed_data, "text/plain");
  results.Clear();
  results.ParseFromString(res->body);
  ASSERT_EQ(num_ts, results.results_size());
  for (int i = 0; i < num_ts; i++) ASSERT_EQ(i, results.results(i).id());
}

TEST_F(DBTest, ProtoBigData1) {
  std::string dbpath = "/tmp/tsdb";
  boost::filesystem::remove_all(dbpath);
  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;

  leveldb::DB* ldb;
  ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());
  set_parameters(_test_num_ts, 16, 300);
  int batch = 10000;
  int num_labels = 20;
  int num_samples = 720;
  int interval = 20;

  DB db(dbpath, ldb);
  sleep(1);

  httplib::Client cli("127.0.0.1", 9966);

  std::vector<::tsdb::label::Labels> lsets;
  lsets.reserve(num_ts);
  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
    lsets.push_back(std::move(lset));
  }

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024) << "MB"
            << std::endl;

  // ProfilerStart("db_test.prof");
  InsertSamples samples;
  int count = 0;
  Timer t;
  t.start();
  int64_t last = 0;
  std::string data, compressed_data;
  std::vector<uint64_t> ids;
  for (int s = 0; s < num_samples; s++) {
    for (int i = 0; i < num_ts; i++) {
      // ::tsdb::label::Labels lset;
      // for (int j = 0; j < num_labels; j++)
      //   lset.emplace_back("label" + std::to_string(j), "label" +
      //   std::to_string(j) + "_" + std::to_string(i));

      Add(&samples, lsets[i], s * interval * 1000, s * interval * 1000);
      ids.push_back(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                    (uint64_t)(i % PRESERVED_BLOCKS));

      ++count;
      if (count >= batch) {
        data.clear();
        compressed_data.clear();
        samples.SerializeToString(&data);
        snappy::Compress(data.data(), data.size(), &compressed_data);
        auto res = cli.Post("/insert", compressed_data, "text/plain");
        InsertResults results;
        results.ParseFromString(res->body);
        if (results.results_size() != ids.size()) {
          printf("#exp:%lu #got:%lu\n", ids.size(), results.results_size());
          return;
        }
        for (int k = 0; k < ids.size(); k++) {
          if (results.results(k).id() != ids[k]) {
            printf("exp:%lu got:%lu\n", ids[k], results.results(k).id());
            return;
          }
        }
        ids.clear();

        count = 0;
        samples.Clear();
      }
    }

    if ((s + 1) % 10 == 0) {
      int64_t d = t.since_start_nano() - last;
      std::cout << "Duration:" << (d / 1000) << "us throughput:"
                << (double)(num_ts) * (double)(10) / (double)(d)*1000000000
                << "samples/s" << std::endl;
      last = t.since_start_nano();
    }
  }
  if (count > 0) {
    data.clear();
    compressed_data.clear();
    samples.SerializeToString(&data);
    snappy::Compress(data.data(), data.size(), &compressed_data);
    cli.Post("/insert", compressed_data, "text/plain");
  }
  // ProfilerStop();

  mem_usage(vm, rss);
  std::cout << "VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024) << "MB"
            << std::endl;
}

TEST_F(DBTest, ProtoBigDataQuery1) {
  std::string dbpath = "/tmp/tsdb";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 1;
  options.write_buffer_size = 64 * 1024 * 1024;

  leveldb::DB* ldb;
  ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());

  set_parameters(_test_num_ts, 32, 300);
  int batch = 10000;
  int num_labels = 20;
  int num_samples = 1440;
  int interval = 60;

  DB db(dbpath, ldb);

  httplib::Client cli("127.0.0.1", 9966);

  std::vector<::tsdb::label::Labels> lsets;
  lsets.reserve(num_ts);
  load_devops_labels2(&lsets);

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024) << "MB"
            << std::endl;

  InsertSamples samples;
  int count = 0;
  Timer t;
  t.start();
  int64_t last = 0;
  std::string data, compressed_data;
  std::vector<uint64_t> ids;
  uint64_t post_time = 0, post_count = 0, total_post_time = 0;
  for (int s = 0; s < num_samples; s++) {
    int64_t dif = rand() % 100;
    for (int i = 0; i < num_ts; i++) {
      Add(&samples, lsets[i], s * interval * 1000, dif);
      ids.push_back(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                    (uint64_t)(i % PRESERVED_BLOCKS));

      ++count;
      if (count >= batch) {
        data.clear();
        compressed_data.clear();
        samples.SerializeToString(&data);
        snappy::Compress(data.data(), data.size(), &compressed_data);
        uint64_t tmp = t.since_start_nano();
        auto res = cli.Post("/insert", compressed_data, "text/plain");
        post_time += t.since_start_nano() - tmp;
        post_count++;
        InsertResults results;
        results.ParseFromString(res->body);
        if (results.results_size() != ids.size()) {
          printf("#exp:%lu #got:%lu\n", ids.size(), results.results_size());
          return;
        }
        for (int k = 0; k < ids.size(); k++) {
          if (results.results(k).id() != ids[k]) {
            printf("exp:%lu got:%lu\n", ids[k], results.results(k).id());
            return;
          }
        }
        ids.clear();

        count = 0;
        samples.Clear();
      }
    }

    if ((s + 1) % 10 == 0) {
      int64_t d = t.since_start_nano() - last;
      std::cout << "Duration:" << (d / 1000) << "us throughput:"
                << (double)(num_ts) * (double)(10) / (double)(d)*1000000000
                << "samples/s" << std::endl;
      last = t.since_start_nano();
      std::cout << "Post:" << post_time / post_count / 1000
                << "us count:" << post_count << " throughput:"
                << (double)(num_ts) * (double)(10) /
                       (double)(post_time)*1000000000
                << std::endl;
      total_post_time += post_time;
      post_time = 0;
      post_count = 0;
    }
  }
  if (count > 0) {
    data.clear();
    compressed_data.clear();
    samples.SerializeToString(&data);
    snappy::Compress(data.data(), data.size(), &compressed_data);
    uint64_t tmp = t.since_start_nano();
    cli.Post("/insert", compressed_data, "text/plain");
    total_post_time += t.since_start_nano() - tmp;
  }
  int64_t d = t.since_start_nano();
  std::cout << "Duration:" << (d / 1000) << "us throughput:"
            << (double)(num_ts) * (double)(num_samples) / (double)(d)*1000000000
            << "samples/s" << std::endl;
  std::cout << "Post:" << (total_post_time / 1000) << "us throughput:"
            << (double)(num_ts) * (double)(num_samples) /
                   (double)(total_post_time)*1000000000
            << "samples/s" << std::endl;

  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "VM:" << (vm_after / 1024) << "MB RSS:" << (rss_after / 1024)
            << "MB" << std::endl;
  std::cout << "VM(diff):" << ((vm_after - vm) / 1024)
            << "MB RSS(diff):" << ((rss_after - rss) / 1024) << "MB"
            << std::endl;

  queryDevOpsProto(&db, (num_samples - 1) * interval * 1000);
  sleep(3);
  queryDevOpsProto(&db, (num_samples - 1) * interval * 1000);
  sleep(3);
  queryDevOpsProto(&db, (num_samples - 1) * interval * 1000);
}

// Fast path.
TEST_F(DBTest, ProtoBigDataQuery2) {
  std::string dbpath = "/tmp/tsdb";
  boost::filesystem::remove_all(dbpath);

  leveldb::Options options;
  options.create_if_missing = true;
  options.use_log = false;
  options.max_imm_num = 1;
  options.write_buffer_size = 64 * 1024 * 1024;

  leveldb::DB* ldb;
  ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());

  set_parameters(_test_num_ts, 32, 300);
  int batch = 10000;
  int num_labels = 20;
  int num_samples = 1440;
  int interval = 60;

  DB db(dbpath, ldb);

  httplib::Client cli("127.0.0.1", 9966);

  std::vector<::tsdb::label::Labels> lsets;
  lsets.reserve(num_ts);
  load_devops_labels2(&lsets);

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "VM:" << (vm / 1024) << "MB RSS:" << (rss / 1024) << "MB"
            << std::endl;

  InsertSamples samples;
  int count = 0;
  Timer t;
  t.start();
  int64_t last = 0;
  std::string data, compressed_data;
  std::vector<uint64_t> ids, tmp_ids;
  uint64_t post_time = 0, post_count = 0, total_post_time = 0;
  for (int s = 0; s < num_samples; s++) {
    int64_t dif = rand() % 100;
    for (int i = 0; i < num_ts; i++) {
      if (ids.size() < num_ts) {
        Add(&samples, lsets[i], s * interval * 1000, dif);
        ids.push_back(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                      (uint64_t)(i % PRESERVED_BLOCKS));
      } else
        Add(&samples, ids[i], s * interval * 1000, dif);
      tmp_ids.push_back(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                        (uint64_t)(i % PRESERVED_BLOCKS));

      ++count;
      if (count >= batch) {
        data.clear();
        compressed_data.clear();
        samples.SerializeToString(&data);
        snappy::Compress(data.data(), data.size(), &compressed_data);
        uint64_t tmp = t.since_start_nano();
        auto res = cli.Post("/insert", compressed_data, "text/plain");
        post_time += t.since_start_nano() - tmp;
        post_count++;
        InsertResults results;
        results.ParseFromString(res->body);
        if (results.results_size() != tmp_ids.size()) {
          printf("#exp:%lu #got:%lu\n", tmp_ids.size(), results.results_size());
          return;
        }
        for (int k = 0; k < tmp_ids.size(); k++) {
          if (results.results(k).id() != tmp_ids[k]) {
            printf("exp:%lu got:%lu\n", tmp_ids[k], results.results(k).id());
            return;
          }
        }
        tmp_ids.clear();

        count = 0;
        samples.Clear();
      }
    }

    if ((s + 1) % 10 == 0) {
      int64_t d = t.since_start_nano() - last;
      std::cout << "Duration:" << (d / 1000) << "us throughput:"
                << (double)(num_ts) * (double)(10) / (double)(d)*1000000000
                << "samples/s" << std::endl;
      last = t.since_start_nano();
      std::cout << "Post:" << post_time / post_count / 1000
                << "us count:" << post_count << " throughput:"
                << (double)(num_ts) * (double)(10) /
                       (double)(post_time)*1000000000
                << std::endl;
      total_post_time += post_time;
      post_time = 0;
      post_count = 0;
    }
  }
  if (count > 0) {
    data.clear();
    compressed_data.clear();
    samples.SerializeToString(&data);
    snappy::Compress(data.data(), data.size(), &compressed_data);
    uint64_t tmp = t.since_start_nano();
    cli.Post("/insert", compressed_data, "text/plain");
    total_post_time += t.since_start_nano() - tmp;
  }
  int64_t d = t.since_start_nano();
  std::cout << "Duration:" << (d / 1000) << "us throughput:"
            << (double)(num_ts) * (double)(num_samples) / (double)(d)*1000000000
            << "samples/s" << std::endl;
  std::cout << "Post:" << (total_post_time / 1000) << "us throughput:"
            << (double)(num_ts) * (double)(num_samples) /
                   (double)(total_post_time)*1000000000
            << "samples/s" << std::endl;

  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "VM:" << (vm_after / 1024) << "MB RSS:" << (rss_after / 1024)
            << "MB" << std::endl;
  std::cout << "VM(diff):" << ((vm_after - vm) / 1024)
            << "MB RSS(diff):" << ((rss_after - rss) / 1024) << "MB"
            << std::endl;

  queryDevOpsProto(&db, (num_samples - 1) * interval * 1000);
  sleep(3);
  queryDevOpsProto(&db, (num_samples - 1) * interval * 1000);
  sleep(3);
  queryDevOpsProto(&db, (num_samples - 1) * interval * 1000);
}
}  // namespace db
}  // namespace tsdb

int main(int argc, char** argv) {
  // tsdb::db::_test_num_ts = std::stoi(argv[1]);
  // printf("--------------- _test_num_ts:%d --------------\n",
  // tsdb::db::_test_num_ts);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}