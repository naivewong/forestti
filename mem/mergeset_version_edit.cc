#include "mem/mergeset_version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace tsdb {
namespace mem {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  compact_pointers_.clear();
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    leveldb::PutVarint32(dst, kComparator);
    leveldb::PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    leveldb::PutVarint32(dst, kLogNumber);
    leveldb::PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    leveldb::PutVarint32(dst, kPrevLogNumber);
    leveldb::PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    leveldb::PutVarint32(dst, kNextFileNumber);
    leveldb::PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    leveldb::PutVarint32(dst, kLastSequence);
    leveldb::PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    leveldb::PutVarint32(dst, kCompactPointer);
    leveldb::PutVarint32(dst, compact_pointers_[i].first);  // level
    leveldb::PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (const auto& deleted_file_kvp : deleted_files_) {
    leveldb::PutVarint32(dst, kDeletedFile);
    leveldb::PutVarint32(dst, deleted_file_kvp.first);   // level
    leveldb::PutVarint64(dst, deleted_file_kvp.second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const leveldb::FileMetaData& f = new_files_[i].second;
    leveldb::PutVarint32(dst, kNewFile);
    leveldb::PutVarint32(dst, new_files_[i].first);  // level
    leveldb::PutVarint64(dst, f.number);
    leveldb::PutVarint64(dst, f.file_size);
    leveldb::PutLengthPrefixedSlice(dst, f.smallest.Encode());
    leveldb::PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

static bool GetInternalKey(leveldb::Slice* input, leveldb::InternalKey* dst) {
  leveldb::Slice str;
  if (leveldb::GetLengthPrefixedSlice(input, &str)) {
    return dst->DecodeFrom(str);
  } else {
    return false;
  }
}

static bool GetLevel(leveldb::Slice* input, int* level) {
  uint32_t v;
  if (leveldb::GetVarint32(input, &v) && v < 2) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

leveldb::Status VersionEdit::DecodeFrom(const leveldb::Slice& src) {
  Clear();
  leveldb::Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  leveldb::FileMetaData f;
  leveldb::Slice str;
  leveldb::InternalKey key;

  while (msg == nullptr && leveldb::GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (leveldb::GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (leveldb::GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (leveldb::GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (leveldb::GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (leveldb::GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) && leveldb::GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level) && leveldb::GetVarint64(&input, &f.number) &&
            leveldb::GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  leveldb::Status result;
  if (msg != nullptr) {
    result = leveldb::Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    leveldb::AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    leveldb::AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    leveldb::AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    leveldb::AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    leveldb::AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (const auto& deleted_files_kvp : deleted_files_) {
    r.append("\n  RemoveFile: ");
    leveldb::AppendNumberTo(&r, deleted_files_kvp.first);
    r.append(" ");
    leveldb::AppendNumberTo(&r, deleted_files_kvp.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const leveldb::FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    leveldb::AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    leveldb::AppendNumberTo(&r, f.number);
    r.append(" ");
    leveldb::AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}
}