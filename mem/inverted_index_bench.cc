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

class PostingsWithArt {
 private:
  art::art_tree tree;
  std::vector<std::vector<uint64_t>> lists;
  base::RWMutexLock mutex;

 public:
  PostingsWithArt() { art::art_tree_init(&tree); }

  void add(uint64_t id, const label::Labels& ls) {
    for (auto& l : ls) {
      std::string tag = l.label + "$" + l.value;
      void* r = art::art_search(
          &tree, reinterpret_cast<const unsigned char*>(tag.c_str()),
          tag.size());
      if (r == NULL) {
        art::art_insert(&tree,
                        reinterpret_cast<const unsigned char*>(tag.c_str()),
                        tag.size(), (void*)((uintptr_t)(lists.size())));
        lists.emplace_back();
        lists.back().push_back(id);
      } else {
        int i = (int)((uintptr_t)(r));
        lists[i].push_back(id);
      }
    }
  }

  void add_with_lock(uint64_t id, const label::Labels& ls) {
    base::RWLockGuard lock(mutex, 1);
    add(id, ls);
  }

  void get(const label::Label& l, std::vector<uint64_t>* pids) {
    base::RWLockGuard lock(mutex, 0);
    std::string tag = l.label + "$" + l.value;
    void* r = art::art_search(
        &tree, reinterpret_cast<const unsigned char*>(tag.c_str()), tag.size());
    if (r == NULL)
      pids = nullptr;
    else
      pids = &(lists[(int)((uintptr_t)(r))]);
  }
};

class PostingsWithOptArt {
 public:
  PostingsWithOptArt() : postings_head_(nullptr), postings_tail_(nullptr) {
    art_tree_init(&tree);
  }

  struct postings_list {
    postings_list* prev_;
    postings_list* next_;
    uint64_t leaf_;  // the addr of leaf.
    std::atomic<uint64_t> read_epoch_;
    std::vector<uint64_t> list_;

    std::atomic<bool> lock_;
    uint32_t version_;

    void lock() {
      for (;;) {
        // Optimistically assume the lock is free on the first try
        if (!lock_.exchange(true, std::memory_order_acquire)) {
          return;
        }
        // Wait for lock to be released without generating cache misses
        while (lock_.load(std::memory_order_relaxed)) {
          // Issue X86 PAUSE or ARM YIELD instruction to reduce contention
          // between hyper-threads sched_yield();
          __builtin_ia32_pause();
        }
      }
    }

    void unlock() { lock_.store(false, std::memory_order_release); }

    void clear() {
      prev_ = nullptr;
      next_ = nullptr;
      leaf_ = 0;
      read_epoch_ = 0;
      list_.clear();
      lock_ = false;
      version_ = 0;
    }
  };

  void add(uint64_t id, const label::Label& l) {
    std::string tag = l.label + "$" + l.value;

  AGAIN:
    void* r = art_search(
        &tree, reinterpret_cast<const unsigned char*>(tag.c_str()), tag.size());
    if (r == NULL) {
      postings_list* list = new postings_list();
      list->list_.push_back(id);

      art_insert_no_update(&tree,
                           reinterpret_cast<const unsigned char*>(tag.c_str()),
                           tag.size(), (void*)(list), &r);
      if (r != (void*)(list)) {
        delete list;
        goto AGAIN;
      }
    } else {
      postings_list* l = (postings_list*)((uintptr_t)r);

      l->lock();
      // TODO(Alec): we may need binary search before inserting.
      l->list_.push_back(id);
      l->unlock();
    }
  }

  void add(uint64_t id, const label::Labels& ls) {
    for (auto& l : ls) add(id, l);
  }
  void naive_add(uint64_t id, const label::Labels& ls) {
    for (auto& l : ls) {
      std::string tag = l.label + "$" + l.value;
      art_insert(&tree, reinterpret_cast<const unsigned char*>(tag.c_str()),
                 tag.size(), (void*)((uintptr_t)(id)));
    }
  }

  void add_with_lock(uint64_t id, const label::Labels& ls) {
    base::RWLockGuard lock(mutex, 1);
    add(id, ls);
  }

  // void get(const label::Label &l, std::vector<uint64_t>* pids) {
  //   base::RWLockGuard lock(mutex, 0);
  //   std::string tag = l.label + "$" + l.value;
  //   void* r = art_search(&tree, reinterpret_cast<const unsigned
  //   char*>(tag.c_str()), tag.size()); if (r == NULL)
  //     pids = nullptr;
  //   else
  //     pids = &(lists[(int)((uintptr_t)(r))]);
  // }

 private:
  art_tree tree;
  postings_list* postings_head_;
  postings_list* postings_tail_;
  base::RWMutexLock mutex;
};

TEST_F(InvertedIndexTest, OriginalART1) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);
  PostingsWithArt p;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  for (size_t i = 0; i < lsets.size(); i++) p.add_with_lock(i + 1, lsets[i]);
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

TEST_F(InvertedIndexTest, OriginalART2) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);
  PostingsWithArt p;

  int num_threads = 4;
  ThreadPool pool(num_threads);
  base::WaitGroup wg;

  auto func = [](PostingsWithArt* _p, std::vector<tsdb::label::Labels>* _lsets,
                 int left, int right, base::WaitGroup* _wg) {
    for (int i = left; i < right; i++) _p->add_with_lock(i + 1, _lsets->at(i));
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
    pool.enqueue(std::bind(func, &p, &lsets, i * lsets.size() / num_threads,
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

TEST_F(InvertedIndexTest, OptART1) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);
  PostingsWithOptArt p;
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  // ProfilerStart("cpu.prof");
  for (size_t i = 0; i < lsets.size(); i++) p.add(i + 1, lsets[i]);
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
}

TEST_F(InvertedIndexTest, OptART2) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);
  PostingsWithOptArt p;

  int num_threads = 8;
  ThreadPool pool(num_threads);
  base::WaitGroup wg;

  auto func = [](PostingsWithOptArt* _p,
                 std::vector<tsdb::label::Labels>* _lsets, int left, int right,
                 base::WaitGroup* _wg) {
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
    pool.enqueue(std::bind(func, &p, &lsets, i * lsets.size() / num_threads,
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
}

TEST_F(InvertedIndexTest, MemPostings1) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);

  mem::MemPostings p;

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  for (size_t i = 0; i < lsets.size(); i++) p.add(i + 1, lsets[i]);
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

TEST_F(InvertedIndexTest, MemPostings2) {
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);

  mem::MemPostings p;

  int num_threads = 8;
  ThreadPool pool(num_threads);
  base::WaitGroup wg;

  auto func = [](mem::MemPostings* _p, std::vector<tsdb::label::Labels>* _lsets,
                 int left, int right, base::WaitGroup* _wg) {
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
  for (int i = 0; i < num_threads; i++) {
    wg.add(1);
    pool.enqueue(std::bind(func, &p, &lsets, i * lsets.size() / num_threads,
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

TEST_F(InvertedIndexTest, InvertedIndex1) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);

  InvertedIndex tdb(dbpath, "", 1024 * 1024 * 1024);

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  for (size_t i = 0; i < lsets.size(); i++) tdb.add(i + 1, lsets[i]);
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

// async
TEST_F(InvertedIndexTest, InvertedIndex2) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);

  InvertedIndex tdb(dbpath, "", 1024 * 1024 * 1024);
  tdb.start_all_stages();

  std::atomic<int> counter(0);
  auto func = [](void* arg) {
    reinterpret_cast<std::atomic<int>*>(arg)->fetch_add(1);
  };

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  for (size_t i = 0; i < lsets.size(); i++)
    tdb.async_add(i + 1, &lsets[i], func, (void*)(&counter));
  while (counter.load() != num_ts / 100 * 101) {
    // printf("%d\n", __sync_val_compare_and_swap(&counter, 0, 0));
    usleep(10);
  }
  std::cout << "num_running_workers:" << tdb.num_running_workers() << std::endl;
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
  tdb.stop_all_stages();
}

// sync
TEST_F(InvertedIndexTest, InvertedIndex3) {
  std::string dbpath = "/tmp/tsdb_test1";
  boost::filesystem::remove_all(dbpath);
  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);

  InvertedIndex tdb(dbpath, "", 1024 * 1024 * 1024);

  int num_threads = 8;
  ThreadPool pool(num_threads);
  base::WaitGroup wg;

  auto func = [](InvertedIndex* _p, std::vector<tsdb::label::Labels>* _lsets,
                 int left, int right, base::WaitGroup* _wg) {
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

  for (int i = 0; i < num_threads; i++) {
    wg.add(1);
    pool.enqueue(std::bind(func, &tdb, &lsets, i * lsets.size() / num_threads,
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

TEST_F(InvertedIndexTest, Head1) {
  std::string path = "/tmp/head_test";
  boost::filesystem::remove_all(path);

  head::Head head(path);

  num_ts = 1000000;
  std::vector<tsdb::label::Labels> lsets;
  load_devops_labels1(&lsets);
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  auto app = head.appender();
  Timer timer;
  timer.start();
  for (size_t i = 0; i < lsets.size(); i++) {
    app->add(lsets[i], 0, 0);
    if ((i + 1) % (num_ts / 20) == 0) {
      // app->rollback();
      mem_usage(vm, rss);
      std::cout << "Virtual Memory: " << (vm / 1024)
                << "MB\nResident set size: " << (rss / 1024) << "MB\n"
                << std::endl;
    }
  }
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

}  // namespace mem
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}