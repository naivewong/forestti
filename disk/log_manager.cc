#include "disk/log_manager.h"

#include <boost/filesystem.hpp>

#include "base/Logging.hpp"
#include "util/coding.h"

namespace tsdb {
namespace disk {

LogManager::LogManager(leveldb::Env* env, const std::string& dir,
                       const std::string& prefix)
    : env_(env), dir_(dir), prefix_(prefix) {
  files_.reserve(LOG_MANAGER_RESERVED_SIZE);
  file_sizes_.reserve(LOG_MANAGER_RESERVED_SIZE);
  freed_sizes_.reserve(LOG_MANAGER_RESERVED_SIZE);

  leveldb::Status s = env_->CreateDirIfMissing(dir);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    abort();
  }

  std::vector<std::string> logs;
  boost::filesystem::path p(dir);
  boost::filesystem::directory_iterator end_itr;
  for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
    // If it's not a directory, list it. If you want to list directories too,
    // just remove this check.
    if (boost::filesystem::is_regular_file(itr->path())) {
      // assign current file name to current_file and echo it out to the
      // console.
      std::string current_file = itr->path().filename().string();
      if (current_file.size() > prefix_.size() + 3 &&
          memcmp(current_file.c_str(), (prefix_ + "log").c_str(),
                 prefix_.size() + 3) == 0)
        logs.push_back(current_file);
    }
  }
  std::sort(logs.begin(), logs.end(),
            [&](const std::string& l, const std::string& r) {
              return std::stoi(l.substr(prefix_.size() + 3)) <
                     std::stoi(r.substr(prefix_.size() + 3));
            });

  for (size_t i = 0; i < logs.size(); i++) {
    std::unique_ptr<leveldb::RandomRWFile> result;
    s = env_->NewRandomRWFile(dir_ + "/" + logs[i], &result,
                              leveldb::EnvOptions());
    if (!s.ok()) {
      LOG_ERROR << s.ToString();
      abort();
    }
    files_.push_back(std::move(result));
    file_sizes_.push_back(0);
    freed_sizes_.push_back(0);
  }
}

uint64_t LogManager::add_record(const std::string& data) {
  uint64_t idx1, idx2;
  lock_.lock();
  if (files_.empty() ||
      file_sizes_.back() + LOG_HEADER_SIZE + data.size() >= LOG_MAX_SIZE) {
    // Create blank file.
    leveldb::WritableFile* wf;
    leveldb::Status s = env_->NewWritableFile(
        dir_ + "/" + prefix_ + "log" + std::to_string(files_.size()), &wf,
        leveldb::EnvOptions());
    if (!s.ok()) {
      LOG_ERROR << s.ToString();
      abort();
    }
    wf->Append(std::string(LOG_MAX_SIZE, 0));
    delete wf;

    std::unique_ptr<leveldb::RandomRWFile> result;
    s = env_->NewRandomRWFile(
        dir_ + "/" + prefix_ + "log" + std::to_string(files_.size()), &result,
        leveldb::EnvOptions());
    if (!s.ok()) {
      LOG_ERROR << s.ToString();
      abort();
    }
    files_.push_back(std::move(result));
    file_sizes_.push_back(0);
    freed_sizes_.push_back(0);
  }
  idx1 = file_sizes_.size() - 1;
  idx2 = file_sizes_.back();
  file_sizes_.back() += LOG_HEADER_SIZE + data.size();
  leveldb::RandomRWFile* f = files_[idx1].get();

  char header[LOG_HEADER_SIZE];
  header[0] = Active;
  leveldb::EncodeFixed32(header + 1, data.size());
  f->Write(idx2, leveldb::Slice(header, LOG_HEADER_SIZE));
  f->Write(idx2 + LOG_HEADER_SIZE, data);
  lock_.unlock();
  return (idx1 << 32) | idx2;
}

std::pair<leveldb::Slice, char*> LogManager::read_record(uint64_t pos) {
  lock_.lock();
  uint64_t idx1 = pos >> 32;
  uint64_t idx2 = pos & 0x00000000ffffffff;

  leveldb::RandomRWFile* f = files_[idx1].get();

  char header[LOG_HEADER_SIZE];
  leveldb::Slice result;
  f->Read(idx2, LOG_HEADER_SIZE, &result, header);
  uint32_t size = leveldb::DecodeFixed32(header + 1);
  char* data = new char[size];
  f->Read(idx2 + LOG_HEADER_SIZE, size, &result, data);
  lock_.unlock();
  return std::make_pair(result, data);
}

void LogManager::free_record(uint64_t pos) {
  lock_.lock();
  uint64_t idx1 = pos >> 32;
  uint64_t idx2 = pos & 0x00000000ffffffff;
  char h[1];
  h[0] = Inactive;
  files_[idx1]->Write(idx2, leveldb::Slice(h, 1));
  lock_.unlock();
}

SequentialLogManager::SequentialLogManager(leveldb::Env* env,
                                           const std::string& dir,
                                           const std::string& prefix)
    : env_(env), dir_(dir), prefix_(prefix) {
  files_.reserve(LOG_MANAGER_RESERVED_SIZE);
  file_sizes_.reserve(LOG_MANAGER_RESERVED_SIZE);
  freed_sizes_.reserve(LOG_MANAGER_RESERVED_SIZE);

  leveldb::Status s = env_->CreateDirIfMissing(dir);
  if (!s.ok()) {
    LOG_ERROR << s.ToString();
    abort();
  }
}

SequentialLogManager::~SequentialLogManager() {
  for (size_t i = 0; i < files_.size(); i++) delete files_[i];
}

uint64_t SequentialLogManager::add_record(const std::string& data) {
  uint64_t idx1, idx2;
  lock_.lock();
  if (files_.empty() ||
      file_sizes_.back() + LOG_HEADER_SIZE + data.size() >= LOG_MAX_SIZE) {
    // Create blank file.
    leveldb::WritableFile* wf;
    leveldb::Status s = env_->NewWritableFile(
        dir_ + "/" + prefix_ + "log" + std::to_string(files_.size()), &wf,
        leveldb::EnvOptions());
    if (!s.ok()) {
      LOG_ERROR << s.ToString();
      abort();
    }
    files_.push_back(wf);
    file_sizes_.push_back(0);
    freed_sizes_.push_back(0);
  }
  idx1 = file_sizes_.size() - 1;
  idx2 = file_sizes_.back();
  file_sizes_.back() += LOG_HEADER_SIZE + data.size();
  leveldb::WritableFile* f = files_[idx1];

  char header[LOG_HEADER_SIZE];
  header[0] = Active;
  leveldb::EncodeFixed32(header + 1, data.size());
  f->Append(leveldb::Slice(header, LOG_HEADER_SIZE));
  f->Append(data);
  lock_.unlock();
  return (idx1 << 32) | idx2;
}

}  // namespace disk
}  // namespace tsdb