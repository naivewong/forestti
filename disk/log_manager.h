#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "leveldb/env.h"

#define LOG_MANAGER_RESERVED_SIZE 500
#define LOG_MAX_SIZE (512 * 1024 * 1024)
#define LOG_HEADER_SIZE 5

namespace tsdb {
namespace disk {

enum RecordState { Active, Inactive };

class SpinLock {
 private:
  std::atomic<bool> lock_;

 public:
  SpinLock() : lock_(false) {}

  void lock() {
    for (;;) {
      // Optimistically assume the lock is free on the first try
      if (!lock_.exchange(true, std::memory_order_acquire)) {
        return;
      }
      // Wait for lock to be released without generating cache misses
      while (lock_.load(std::memory_order_relaxed)) {
        // Issue X86 PAUSE or ARM YIELD instruction to reduce contention between
        // hyper-threads
        __builtin_ia32_pause();
      }
    }
  }

  void unlock() { lock_.store(false, std::memory_order_release); }
};

class LogManager {
 private:
  leveldb::Env* env_;
  std::string dir_;
  std::string prefix_;
  std::vector<std::unique_ptr<leveldb::RandomRWFile>> files_;
  std::vector<uint64_t> file_sizes_;
  std::vector<uint64_t> freed_sizes_;
  SpinLock lock_;

 public:
  LogManager(leveldb::Env* env, const std::string& dir,
             const std::string& prefix = "");

  uint64_t add_record(const std::string& data);
  std::pair<leveldb::Slice, char*> read_record(uint64_t pos);
  void free_record(uint64_t pos);
};

// Used during snapshot.
class SequentialLogManager {
 private:
  leveldb::Env* env_;
  std::string dir_;
  std::string prefix_;
  std::vector<leveldb::WritableFile*> files_;
  std::vector<uint64_t> file_sizes_;
  std::vector<uint64_t> freed_sizes_;
  SpinLock lock_;

 public:
  SequentialLogManager(leveldb::Env* env, const std::string& dir,
                       const std::string& prefix = "");
  ~SequentialLogManager();

  uint64_t add_record(const std::string& data);
};

}  // namespace disk
}  // namespace tsdb