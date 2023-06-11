#pragma once

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;

}

namespace tsdb {
namespace mem {

struct art_tree;

class Version;
class VersionEdit;
class VersionSet;

class MergeSet : public leveldb::DB {
 public:
  MergeSet(const leveldb::Options& options, const std::string& dbname);

  MergeSet(const MergeSet&) = delete;
  MergeSet& operator=(const MergeSet&) = delete;

  ~MergeSet() override;

  // Implementations of the DB interface
  leveldb::Status Put(const leveldb::WriteOptions&, const leveldb::Slice& key,
             const leveldb::Slice& value) override;
  leveldb::Status Delete(const leveldb::WriteOptions&, const leveldb::Slice& key) override;
  leveldb::Status Write(const leveldb::WriteOptions& options, leveldb::WriteBatch* updates) override;
  leveldb::Status Get(const leveldb::ReadOptions& options, const leveldb::Slice& key,
             std::string* value) override;
  leveldb::Iterator* NewIterator(const leveldb::ReadOptions&) override;
  const leveldb::Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const leveldb::Snapshot* snapshot) override;
  bool GetProperty(const leveldb::Slice& property, std::string* value) override;
  void GetApproximateSizes(const leveldb::Range* range, int n, uint64_t* sizes) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Force current memtable contents to be compacted.
  leveldb::Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  leveldb::Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(leveldb::Slice key);

  void CompactRange(const leveldb::Slice* begin, const leveldb::Slice* end) {}

  void TrieToL1SSTs(struct art_tree* tree, bool sync=false);

  static leveldb::Status MergeSetOpen(const leveldb::Options& options, const std::string& dbname, MergeSet** dbptr);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const leveldb::InternalKey* begin;  // null means beginning of key range
    const leveldb::InternalKey* end;    // null means end of key range
    leveldb::InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  leveldb::Iterator* NewInternalIterator(const leveldb::ReadOptions&,
                                leveldb::SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  leveldb::Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  leveldb::Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(leveldb::Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  leveldb::Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, leveldb::SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  leveldb::Status WriteLevel0Table(leveldb::MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  leveldb::Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  leveldb::WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const leveldb::Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  leveldb::Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  leveldb::Status OpenCompactionOutputFile(CompactionState* compact);
  leveldb::Status FinishCompactionOutputFile(CompactionState* compact, leveldb::Iterator* input);
  leveldb::Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const leveldb::Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  static void BGTrieToSSTsWork(void* db);
  void BGTrieToSSTs(struct art_tree* tree);
  void BackgroundTrieToSSTs(struct art_tree* tree);
  leveldb::Status WriteLevel1Tables(struct art_tree* tree, VersionEdit* edit);

  // Constant after construction
  leveldb::Env* const env_;
  const leveldb::InternalKeyComparator internal_comparator_;
  const leveldb::InternalFilterPolicy internal_filter_policy_;
  const leveldb::Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  leveldb::TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  leveldb::FileLock* db_lock_;

  // State below is protected by mutex_
  leveldb::port::Mutex mutex_;
  std::atomic<bool> shutting_down_;
  leveldb::port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  leveldb::MemTable* mem_;
  leveldb::MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  leveldb::WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  leveldb::log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  leveldb::WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  leveldb::SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  leveldb::Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[2] GUARDED_BY(mutex_);
};

}
}