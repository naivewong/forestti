#include <boost/filesystem.hpp>

#include "mem/mmap.h"

namespace tsdb {
namespace mem {

size_t ART_MMAP_FILE_NODE4_SLOTS = ART_MMAP_FILE_SIZE / sizeof(art_node4);
size_t ART_MMAP_FILE_NODE16_SLOTS = ART_MMAP_FILE_SIZE / sizeof(art_node16);
size_t ART_MMAP_FILE_NODE48_SLOTS = ART_MMAP_FILE_SIZE / sizeof(art_node48);
size_t ART_MMAP_FILE_NODE256_SLOTS = ART_MMAP_FILE_SIZE / sizeof(art_node256);

/************************** MMapArtNode4 **************************/
MMapArtNode4::MMapArtNode4(const std::string& dir): dir_(dir), cur_slot_(0), files_(ART_MMAP_FILE_ARRAY_PRESERVED_CAP) {
  tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node4_0", ART_MMAP_FILE_SIZE);
  files_[0].store(f);
  for (size_t i = 1; i < ART_MMAP_FILE_ARRAY_PRESERVED_CAP; i++)
    files_[i].store(nullptr);
}

MMapArtNode4::~MMapArtNode4() {
  for (size_t i = 0; i < files_.size(); i++) {
    tsdbutil::MMapLinuxManager* f = files_[i].load();
    if (f) {
      delete f;
      boost::filesystem::remove(dir_ + "/node4_" + std::to_string(i));
    }
    else
      return;
  }
}

art_node4* MMapArtNode4::alloc() {
  uint64_t slot;
  if (free_slots_.try_dequeue(slot)) {
    uint64_t idx1 = slot / ART_MMAP_FILE_NODE4_SLOTS;
    uint64_t idx2 = slot % ART_MMAP_FILE_NODE4_SLOTS;
    return (art_node4*)(files_[idx1].load()->data() + idx2 * sizeof(art_node4));
  }

  slot = cur_slot_++;
  // printf("MMapArtNode4 %lu\n", slot);
  if (slot > 0 && slot % ART_MMAP_FILE_NODE4_SLOTS == 0) {
    // New file.
    int fid = slot / ART_MMAP_FILE_NODE4_SLOTS;
    tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node4_" + std::to_string(fid), ART_MMAP_FILE_SIZE);
    files_[fid].store(f, std::memory_order_release);
    return (art_node4*)(files_[fid].load()->data());
  }

  uint64_t idx1 = slot / ART_MMAP_FILE_NODE4_SLOTS;
  uint64_t idx2 = slot % ART_MMAP_FILE_NODE4_SLOTS;
  while (files_[idx1].load(std::memory_order_relaxed) == nullptr) {
    __builtin_ia32_pause();
  }
  return (art_node4*)(files_[idx1].load()->data() + idx2 * sizeof(art_node4));
}

void MMapArtNode4::reclaim(void* addr) {
  // NOTE(Alec): it's important to clear the space.
  memset(addr, 0, sizeof(art_node4));

  uint64_t slot = cur_slot_;
  uint64_t idx = slot / ART_MMAP_FILE_NODE4_SLOTS;
  uint64_t idx2;
  for (uint64_t i = 0; i <= idx; i++) {
    if ((uintptr_t)(addr) < (uintptr_t)(files_[i].load()->data()))
      continue;
    idx2 = ((uintptr_t)(addr) - (uintptr_t)(files_[i].load()->data())) / sizeof(art_node4);
    if (idx2 < ART_MMAP_FILE_NODE4_SLOTS) {
      free_slots_.enqueue(i * ART_MMAP_FILE_NODE4_SLOTS + idx2);
      return;
    }
  }
}

/************************** MMapArtNode16 **************************/
MMapArtNode16::MMapArtNode16(const std::string& dir): dir_(dir), cur_slot_(0), files_(ART_MMAP_FILE_ARRAY_PRESERVED_CAP) {
  tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node16_0", ART_MMAP_FILE_SIZE);
  files_[0].store(f);
  for (size_t i = 1; i < ART_MMAP_FILE_ARRAY_PRESERVED_CAP; i++)
    files_[i].store(nullptr);
}

MMapArtNode16::~MMapArtNode16() {
  for (size_t i = 0; i < files_.size(); i++) {
    tsdbutil::MMapLinuxManager* f = files_[i].load();
    if (f) {
      delete f;
      boost::filesystem::remove(dir_ + "/node16_" + std::to_string(i));
    }
    else
      return;
  }
}

art_node16* MMapArtNode16::alloc() {
  uint64_t slot;
  if (free_slots_.try_dequeue(slot)) {
    uint64_t idx1 = slot / ART_MMAP_FILE_NODE16_SLOTS;
    uint64_t idx2 = slot % ART_MMAP_FILE_NODE16_SLOTS;
    return (art_node16*)(files_[idx1].load()->data() + idx2 * sizeof(art_node16));
  }

  slot = cur_slot_++;
  // printf("MMapArtNode16 %lu\n", slot);
  if (slot > 0 && slot % ART_MMAP_FILE_NODE16_SLOTS == 0) {
    // New file.
    int fid = slot / ART_MMAP_FILE_NODE16_SLOTS;
    tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node16_" + std::to_string(fid), ART_MMAP_FILE_SIZE);
    files_[fid].store(f, std::memory_order_release);
    return (art_node16*)(files_[fid].load()->data());
  }

  uint64_t idx1 = slot / ART_MMAP_FILE_NODE16_SLOTS;
  uint64_t idx2 = slot % ART_MMAP_FILE_NODE16_SLOTS;
  while (files_[idx1].load(std::memory_order_relaxed) == nullptr) {
    __builtin_ia32_pause();
  }
  return (art_node16*)(files_[idx1].load()->data() + idx2 * sizeof(art_node16));
}

void MMapArtNode16::reclaim(void* addr) {
  // NOTE(Alec): it's important to clear the space.
  memset(addr, 0, sizeof(art_node16));

  uint64_t slot = cur_slot_;
  uint64_t idx = slot / ART_MMAP_FILE_NODE16_SLOTS;
  uint64_t idx2;
  for (uint64_t i = 0; i <= idx; i++) {
    if ((uintptr_t)(addr) < (uintptr_t)(files_[i].load()->data()))
      continue;
    idx2 = ((uintptr_t)(addr) - (uintptr_t)(files_[i].load()->data())) / sizeof(art_node16);
    if (idx2 < ART_MMAP_FILE_NODE16_SLOTS) {
      free_slots_.enqueue(i * ART_MMAP_FILE_NODE16_SLOTS + idx2);
      return;
    }
  }
}

/************************** MMapArtNode48 **************************/
MMapArtNode48::MMapArtNode48(const std::string& dir): dir_(dir), cur_slot_(0), files_(ART_MMAP_FILE_ARRAY_PRESERVED_CAP) {
  tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node48_0", ART_MMAP_FILE_SIZE);
  files_[0].store(f);
  for (size_t i = 1; i < ART_MMAP_FILE_ARRAY_PRESERVED_CAP; i++)
    files_[i].store(nullptr);
}

MMapArtNode48::~MMapArtNode48() {
  for (size_t i = 0; i < files_.size(); i++) {
    tsdbutil::MMapLinuxManager* f = files_[i].load();
    if (f) {
      delete f;
      boost::filesystem::remove(dir_ + "/node48_" + std::to_string(i));
    }
    else
      return;
  }
}

art_node48* MMapArtNode48::alloc() {
  uint64_t slot;
  if (free_slots_.try_dequeue(slot)) {
    uint64_t idx1 = slot / ART_MMAP_FILE_NODE48_SLOTS;
    uint64_t idx2 = slot % ART_MMAP_FILE_NODE48_SLOTS;
    return (art_node48*)(files_[idx1].load()->data() + idx2 * sizeof(art_node48));
  }

  slot = cur_slot_++;
  // printf("MMapArtNode48 %lu\n", slot);
  if (slot > 0 && slot % ART_MMAP_FILE_NODE48_SLOTS == 0) {
    // New file.
    int fid = slot / ART_MMAP_FILE_NODE48_SLOTS;
    tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node48_" + std::to_string(fid), ART_MMAP_FILE_SIZE);
    files_[fid].store(f, std::memory_order_release);
    return (art_node48*)(files_[fid].load()->data());
  }

  uint64_t idx1 = slot / ART_MMAP_FILE_NODE48_SLOTS;
  uint64_t idx2 = slot % ART_MMAP_FILE_NODE48_SLOTS;
  while (files_[idx1].load(std::memory_order_relaxed) == nullptr) {
    __builtin_ia32_pause();
  }
  return (art_node48*)(files_[idx1].load()->data() + idx2 * sizeof(art_node48));
}

void MMapArtNode48::reclaim(void* addr) {
  // NOTE(Alec): it's important to clear the space.
  memset(addr, 0, sizeof(art_node48));

  uint64_t slot = cur_slot_;
  uint64_t idx = slot / ART_MMAP_FILE_NODE48_SLOTS;
  uint64_t idx2;
  for (uint64_t i = 0; i <= idx; i++) {
    if ((uintptr_t)(addr) < (uintptr_t)(files_[i].load()->data()))
      continue;
    idx2 = ((uintptr_t)(addr) - (uintptr_t)(files_[i].load()->data())) / sizeof(art_node48);
    if (idx2 < ART_MMAP_FILE_NODE48_SLOTS) {
      free_slots_.enqueue(i * ART_MMAP_FILE_NODE48_SLOTS + idx2);
      return;
    }
  }
}

/************************** MMapArtNode256 **************************/
MMapArtNode256::MMapArtNode256(const std::string& dir): dir_(dir), cur_slot_(0), files_(ART_MMAP_FILE_ARRAY_PRESERVED_CAP) {
  tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node256_0", ART_MMAP_FILE_SIZE);
  files_[0].store(f);
  for (size_t i = 1; i < ART_MMAP_FILE_ARRAY_PRESERVED_CAP; i++)
    files_[i].store(nullptr);
}

MMapArtNode256::~MMapArtNode256() {
  for (size_t i = 0; i < files_.size(); i++) {
    tsdbutil::MMapLinuxManager* f = files_[i].load();
    if (f) {
      delete f;
      boost::filesystem::remove(dir_ + "/node256_" + std::to_string(i));
    }
    else
      return;
  }
}

art_node256* MMapArtNode256::alloc() {
  uint64_t slot;
  if (free_slots_.try_dequeue(slot)) {
    uint64_t idx1 = slot / ART_MMAP_FILE_NODE256_SLOTS;
    uint64_t idx2 = slot % ART_MMAP_FILE_NODE256_SLOTS;
    return (art_node256*)(files_[idx1].load()->data() + idx2 * sizeof(art_node256));
  }

  slot = cur_slot_++;
  // printf("MMapArtNode256 %lu\n", slot);
  if (slot > 0 && slot % ART_MMAP_FILE_NODE256_SLOTS == 0) {
    // New file.
    int fid = slot / ART_MMAP_FILE_NODE256_SLOTS;
    tsdbutil::MMapLinuxManager* f = new tsdb::tsdbutil::MMapLinuxManager(dir_ + "/node256_" + std::to_string(fid), ART_MMAP_FILE_SIZE);
    files_[fid].store(f, std::memory_order_release);
    return (art_node256*)(files_[fid].load()->data());
  }

  uint64_t idx1 = slot / ART_MMAP_FILE_NODE256_SLOTS;
  uint64_t idx2 = slot % ART_MMAP_FILE_NODE256_SLOTS;
  while (files_[idx1].load(std::memory_order_relaxed) == nullptr) {
    __builtin_ia32_pause();
  }
  return (art_node256*)(files_[idx1].load()->data() + idx2 * sizeof(art_node256));
}

void MMapArtNode256::reclaim(void* addr) {
  // NOTE(Alec): it's important to clear the space.
  memset(addr, 0, sizeof(art_node256));

  uint64_t slot = cur_slot_;
  uint64_t idx = slot / ART_MMAP_FILE_NODE256_SLOTS;
  uint64_t idx2;
  for (uint64_t i = 0; i <= idx; i++) {
    if ((uintptr_t)(addr) < (uintptr_t)(files_[i].load()->data()))
      continue;
    idx2 = ((uintptr_t)(addr) - (uintptr_t)(files_[i].load()->data())) / sizeof(art_node256);
    if (idx2 < ART_MMAP_FILE_NODE256_SLOTS) {
      free_slots_.enqueue(i * ART_MMAP_FILE_NODE256_SLOTS + idx2);
      return;
    }
  }
}

/********************* global allocators ***********************/
#ifdef MMAP_ART_NODE
std::unique_ptr<MMapArtNode4> mmap_node4 = std::unique_ptr<MMapArtNode4>(new MMapArtNode4("/tmp"));
std::unique_ptr<MMapArtNode16> mmap_node16 = std::unique_ptr<MMapArtNode16>(new MMapArtNode16("/tmp"));
std::unique_ptr<MMapArtNode48> mmap_node48 = std::unique_ptr<MMapArtNode48>(new MMapArtNode48("/tmp"));
std::unique_ptr<MMapArtNode256> mmap_node256 = std::unique_ptr<MMapArtNode256>(new MMapArtNode256("/tmp"));

void reset_mmap_art_nodes() {
  mmap_node4.reset(new MMapArtNode4("/tmp"));
  mmap_node16.reset(new MMapArtNode16("/tmp"));
  mmap_node48.reset(new MMapArtNode48("/tmp"));
  mmap_node256.reset(new MMapArtNode256("/tmp"));
}
#endif

}
}