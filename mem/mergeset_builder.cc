#include "base/Logging.hpp"
#include "mem/go_art.h"
#include "mem/mergeset_builder.h"
#include "mem/mergeset_version_set.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace tsdb {
namespace mem {

leveldb::Status BuildTable(const std::string& dbname, leveldb::Env* env, const leveldb::Options& options,
                           leveldb::TableCache* table_cache, leveldb::Iterator* iter,
                           leveldb::FileMetaData* meta) {
  leveldb::Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = leveldb::TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    leveldb::WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    leveldb::TableBuilder* builder = new leveldb::TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    leveldb::Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
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
      leveldb::Iterator* it = table_cache->NewIterator(leveldb::ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

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

leveldb::Status BuildTablesFromTrie(const std::string& dbname, leveldb::Env* env,
                                    const leveldb::Options& options,
                                    leveldb::TableCache* table_cache, struct art_tree* tree,
                                    std::vector<leveldb::FileMetaData>* metas, VersionSet* const vset,
                                    std::set<uint64_t>* pending_outputs) {
  leveldb::Status s;
  metas->clear();

  // Get the current sequence (for each traversed key).
  uint64_t last_sequence = vset->LastSequence();

  struct TrieConversionArg {
    std::string dbname;
    leveldb::Env* env;
    const leveldb::Options* options;
    leveldb::TableCache* table_cache;
    std::vector<leveldb::FileMetaData>* metas;
    VersionSet* const vset;
    std::set<uint64_t>* pending_outputs;

    size_t max_file_size;
    leveldb::Status s;

    leveldb::FileMetaData* meta;
    leveldb::WritableFile* file;
    leveldb::TableBuilder* builder;
    uint64_t* last_sequence;
    char key_buf[1024];
    char val_buf[9];
  };

  auto cb = [](void* data, const unsigned char* key, uint32_t key_len,
               void* value) -> bool {
    // printf("%s\n", std::string(reinterpret_cast<const char*>(key), key_len).c_str());
    struct TrieConversionArg* arg = (struct TrieConversionArg*)data;

    if (arg->file == nullptr) {
      arg->metas->emplace_back();
      arg->meta = &(arg->metas->back());
      arg->meta->number = arg->vset->NewFileNumber();
      arg->pending_outputs->insert(arg->meta->number);
      arg->meta->file_size = 0;
      std::string fname = leveldb::TableFileName(arg->dbname, arg->meta->number);
      leveldb::Status s = arg->env->NewWritableFile(fname, &arg->file);
      if (!s.ok()) {
        return true;
      }
      arg->builder = new leveldb::TableBuilder(*(arg->options), arg->file);
    }

    memcpy(arg->key_buf, key, key_len);
    leveldb::EncodeFixed64(arg->key_buf + key_len, ((*(arg->last_sequence)) << 8) | leveldb::kTypeValue);

    if (arg->meta->smallest.Empty())
      arg->meta->smallest.DecodeFrom(leveldb::Slice(arg->key_buf, key_len + 8));
    arg->meta->largest.DecodeFrom(leveldb::Slice(arg->key_buf, key_len + 8));

    leveldb::EncodeFixed64(arg->val_buf, (uint64_t)(uintptr_t)value);
    arg->builder->Add(leveldb::Slice(arg->key_buf, key_len + 8), leveldb::Slice(arg->val_buf, 8));

    if (arg->builder->FileSize() >= arg->max_file_size) {
      // Finish and check for builder errors
      leveldb::Status s = arg->builder->Finish();
      if (s.ok()) {
        arg->meta->file_size = arg->builder->FileSize();
        assert(arg->meta->file_size > 0);
      }
      else {
        arg->s = s;
        return true;
      }
      delete arg->builder;
      arg->builder = nullptr;

      // Finish and check for file errors
      if (s.ok())
        s = arg->file->Sync();
      else {
        arg->s = s;
        return true;
      }
      if (s.ok())
        s = arg->file->Close();
      else {
        arg->s = s;
        return true;
      }
      delete arg->file;
      arg->file = nullptr;

      if (s.ok()) {
        // Verify that the table is usable
        leveldb::Iterator* it = arg->table_cache->NewIterator(leveldb::ReadOptions(), arg->meta->number,
                                                              arg->meta->file_size);
        s = it->status();
        delete it;
        if (!s.ok()) {
          arg->s = s;
          return true;
        }
      }
    }
    *(arg->last_sequence) += 1;
    return false;
  };

  struct TrieConversionArg arg {
    .dbname = dbname,
    .env = env,
    .options = &options,
    .table_cache = table_cache,
    .metas = metas,
    .vset = vset,
    .pending_outputs = pending_outputs,
    .max_file_size = options.max_file_size,
    .meta = nullptr,
    .file = nullptr,
    .builder = nullptr,
    .last_sequence = &last_sequence,
  };

  unsigned char left[1] = {0};
  unsigned char right[1] = {255};
  art_range(tree, left, 1, right, 1, true, true, cb, &arg);

  // Close the last builder.
  s = arg.s;
  // LOG_DEBUG << s.ToString();
  if (arg.file) {
    s = arg.builder->Finish();
    if (s.ok()) {
      arg.meta->file_size = arg.builder->FileSize();
      assert(arg.meta->file_size > 0);
      // LOG_DEBUG << meta->file_size;
    }
    else
      LOG_DEBUG << s.ToString();
    delete arg.builder;
    arg.builder = nullptr;

    // Finish and check for file errors
    if (s.ok())
      s = arg.file->Sync();
    if (s.ok())
      s = arg.file->Close();
    delete arg.file;
    arg.file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      leveldb::Iterator* it = arg.table_cache->NewIterator(leveldb::ReadOptions(), arg.meta->number,
                                                           arg.meta->file_size);
      s = it->status();
      delete it;
    }
    // LOG_DEBUG << s.ToString();
  }

  // Update sequence.
  vset->SetLastSequence(last_sequence);

  if (s.ok()) {
    for (size_t i = 0; i < metas->size(); i++) {
      if (metas->at(i).file_size > 0) continue;
      std::string fname = leveldb::TableFileName(dbname, metas->at(i).number);
      env->RemoveFile(fname);
    }
  } else {
    for (size_t i = 0; i < metas->size(); i++) {
      std::string fname = leveldb::TableFileName(dbname, metas->at(i).number);
      env->RemoveFile(fname);
    }
  }

  for (size_t i = 0; i < metas->size(); i++)
    pending_outputs->erase(metas->at(i).number);
  return s;
}

}
}