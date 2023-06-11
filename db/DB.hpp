#pragma once

#include <unordered_map>

#include "base/Channel.hpp"
#include "base/Error.hpp"
#include "base/Logging.hpp"
#include "base/Mutex.hpp"
#include "db/AppenderInterface.hpp"
#include "db/DBUtils.hpp"
#include "head/Head.hpp"
#include "leveldb/db.h"
#include "querier/tsdb_querier.h"
#include "third_party/httplib.h"
#include "third_party/ulid.hpp"

namespace tsdb {
namespace db {

class DB {
 private:
  std::string dir_;

  leveldb::DB* db_;
  std::unique_ptr<head::Head> head_;

  httplib::Server server_;

  error::Error err_;

  // Used for HTTP requests.
  std::unique_ptr<db::AppenderInterface> cached_appender_;
  querier::TSDBQuerier* cached_querier_;

  void init_http_proto_server();

 public:
  DB(const std::string& dir, leveldb::DB* db);

  std::string dir() { return dir_; }

  head::Head* head() { return head_.get(); }

  error::Error error() { return err_; }

  std::unique_ptr<db::AppenderInterface> appender() {
    return head_->appender();
  }

  querier::TSDBQuerier* querier(int64_t mint, int64_t maxt) {
    querier::TSDBQuerier* q =
        new querier::TSDBQuerier(db_, head_.get(), mint, maxt);
    return q;
  }

  void print_level(bool hex = false, bool print_stats = false) {
    db_->PrintLevel(hex, print_stats);
  }

  ~DB();
};

}  // namespace db
}  // namespace tsdb
