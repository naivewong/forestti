// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_format.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "util/coding.h"

#include "head/Head.hpp"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta,
                  tsdb::head::Head* head) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  char buf[4096];
  buf[0] = 0;
  Slice user_key;
  uint64_t physical_id, logical_id, txn;
  std::vector<log::RefFlush> flush_marks;

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();

      if (head && !options.use_log) {
        user_key = ExtractUserKey(key);
        decodeKey(user_key, &physical_id, nullptr);
        logical_id = DecodeFixed64BE(iter->value().data() + 1);
        txn = DecodeFixed64BE(iter->value().data() + 9);
        flush_marks.emplace_back(physical_id, logical_id, txn);
      }

      if (iter->value().size() - 16 > 4096) {
        std::string v;
        v.push_back(0);
        v.append(iter->value().data() + 17, iter->value().size() - 17);
        builder->Add(key, v);
      } else {
        memcpy(buf + 1, iter->value().data() + 17, iter->value().size() - 17);
        builder->Add(key, Slice(buf, iter->value().size() - 16));
      }
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  if (s.ok() && head && !options.use_log && !head->no_log()) head->write_flush_marks(flush_marks);

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

Status BuildTables(const std::string& dbname, Env* env, const Options& options,
                   TableCache* table_cache, Iterator* iter,
                   std::vector<FileMetaData>* metas, VersionSet* const vset,
                   std::set<uint64_t>* pending_outputs,
                   int64_t partition_length, ::tsdb::head::Head* head) {
  Status s;
  iter->SeekToFirst();

  char buf[4096];
  buf[0] = 0;
  Slice user_key;
  uint64_t physical_id, logical_id, txn;
  int64_t time, time_boundary;
  std::vector<log::RefFlush> flush_marks;

  std::unordered_map<int64_t, std::string> fnames;
  std::unordered_map<int64_t, WritableFile*> files;
  std::unordered_map<int64_t, TableBuilder*> builders;
  std::map<int64_t, FileMetaData> filemetas;

  if (iter->Valid()) {
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();

      user_key = ExtractUserKey(key);
      decodeKey(user_key, &physical_id, &time);
      time_boundary = time / partition_length * partition_length;

      if (head && !options.use_log) {
        logical_id = DecodeFixed64BE(iter->value().data() + 1);
        txn = DecodeFixed64BE(iter->value().data() + 9);
        flush_marks.emplace_back(physical_id, logical_id, txn);
      }

      auto b = fnames.find(time_boundary);
      if (b == fnames.end()) {
        FileMetaData meta;
        meta.number = vset->NewFileNumber();
        pending_outputs->insert(meta.number);
        meta.file_size = 0;
        meta.smallest.DecodeFrom(iter->key());
        meta.largest.DecodeFrom(iter->key());
        meta.time_boundary = time_boundary;
        meta.time_interval = partition_length;
        filemetas[time_boundary] = meta;

        std::string fname = TableFileName(dbname, meta.number);
        fnames[time_boundary] = fname;

        WritableFile* file;
        s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
          for (auto meta : filemetas) {
            pending_outputs->erase(meta.second.number);
            metas->push_back(meta.second);
          }
          return s;
        }
        files[time_boundary] = file;

        TableBuilder* builder = new TableBuilder(options, file);
        builders[time_boundary] = builder;
      }

      if (iter->value().size() - 16 > 4096) {
        std::string v;
        v.push_back(0);
        v.append(iter->value().data() + 17, iter->value().size() - 17);
        builders[time_boundary]->Add(key, v);
      } else {
        memcpy(buf + 1, iter->value().data() + 17, iter->value().size() - 17);
        builders[time_boundary]->Add(key,
                                     Slice(buf, iter->value().size() - 16));
      }

      if (!key.empty()) {
        filemetas[time_boundary].largest.DecodeFrom(key);
      }
    }

    // Finish and check for builder errors
    for (auto p : builders) {
      s = p.second->Finish();
      if (s.ok()) {
        filemetas[p.first].file_size = p.second->FileSize();
        assert(filemetas[p.first].file_size > 0);
      }
      delete p.second;
    }

    // Finish and check for file errors
    if (s.ok()) {
      for (auto p : files) s = p.second->Sync();
    }
    if (s.ok()) {
      for (auto p : files) {
        s = p.second->Close();
        delete p.second;
      }
    }

    if (s.ok()) {
      // Verify that the table is usable
      for (auto p : filemetas) {
        Iterator* it = table_cache->NewIterator(ReadOptions(), p.second.number,
                                                p.second.file_size);
        s = it->status();
        delete it;
      }
    }
  }

  if (s.ok() && head && !options.use_log && !head->no_log()) head->write_flush_marks(flush_marks);

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok()) {
    // Keep it
  } else {
    for (auto p : fnames) env->RemoveFile(p.second);
  }
  for (auto meta : filemetas) {
    pending_outputs->erase(meta.second.number);
    metas->push_back(meta.second);
  }
  return s;
}

}  // namespace leveldb
