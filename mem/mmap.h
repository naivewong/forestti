#pragma once

#include <atomic>
#include <vector>

#include "mem/inverted_index.h"
#include "third_party/art_optlock_epoch.h"
#include "third_party/moodycamel/concurrentqueue.h"
#include "tsdbutil/MMapLinuxManager.hpp"

#define ART_MMAP_FILE_SIZE (64 * 1024 * 1024)
#define ART_MMAP_FILE_ARRAY_PRESERVED_CAP 1024

// #define MMAP_ART_NODE 1

namespace tsdb {
namespace mem {

extern size_t ART_MMAP_FILE_NODE4_SLOTS;
extern size_t ART_MMAP_FILE_NODE16_SLOTS;
extern size_t ART_MMAP_FILE_NODE48_SLOTS;
extern size_t ART_MMAP_FILE_NODE256_SLOTS;

class MMapArtNode4 {
private:
  std::string dir_;
  std::atomic<uint64_t> cur_slot_;
  SpinLock lock_;

  std::vector<std::atomic<tsdbutil::MMapLinuxManager*>> files_;
  moodycamel::ConcurrentQueue<uint64_t> free_slots_;

public:
  MMapArtNode4(const std::string& dir);
  ~MMapArtNode4();
  art_node4* alloc();
  void reclaim(void* addr);
};

class MMapArtNode16 {
private:
  std::string dir_;
  std::atomic<uint64_t> cur_slot_;
  SpinLock lock_;

  std::vector<std::atomic<tsdbutil::MMapLinuxManager*>> files_;
  moodycamel::ConcurrentQueue<uint64_t> free_slots_;

public:
  MMapArtNode16(const std::string& dir);
  ~MMapArtNode16();
  art_node16* alloc();
  void reclaim(void* addr);
};

class MMapArtNode48 {
private:
  std::string dir_;
  std::atomic<uint64_t> cur_slot_;
  SpinLock lock_;

  std::vector<std::atomic<tsdbutil::MMapLinuxManager*>> files_;
  moodycamel::ConcurrentQueue<uint64_t> free_slots_;

public:
  MMapArtNode48(const std::string& dir);
  ~MMapArtNode48();
  art_node48* alloc();
  void reclaim(void* addr);
};

class MMapArtNode256 {
private:
  std::string dir_;
  std::atomic<uint64_t> cur_slot_;
  SpinLock lock_;

  std::vector<std::atomic<tsdbutil::MMapLinuxManager*>> files_;
  moodycamel::ConcurrentQueue<uint64_t> free_slots_;

public:
  MMapArtNode256(const std::string& dir);
  ~MMapArtNode256();
  art_node256* alloc();
  void reclaim(void* addr);
};

#ifdef MMAP_ART_NODE
extern std::unique_ptr<MMapArtNode4> mmap_node4;
extern std::unique_ptr<MMapArtNode16> mmap_node16;
extern std::unique_ptr<MMapArtNode48> mmap_node48;
extern std::unique_ptr<MMapArtNode256> mmap_node256;

extern void reset_mmap_art_nodes();
#endif

}
}