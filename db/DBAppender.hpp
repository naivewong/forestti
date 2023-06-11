#ifndef DBAPPENDER_H
#define DBAPPENDER_H

#include "db/AppenderInterface.hpp"
#include "db/DB.hpp"

namespace tsdb {
namespace db {

// DBAppender wraps the DB's head appender and triggers compactions on commit
// if necessary.
class DBAppender : public AppenderInterface {
 private:
  std::unique_ptr<db::AppenderInterface> app;
  db::DB *db;

 public:
  DBAppender(std::unique_ptr<db::AppenderInterface> &&app, db::DB *db)
      : app(std::move(app)), db(db) {}

  virtual std::pair<uint64_t, leveldb::Status> add(const label::Labels &lset, int64_t t,
                                           double v, uint64_t epoch = 0) override {
    return app->add(lset, t, v, epoch);
  }

  virtual leveldb::Status add_fast(uint64_t logical_id, int64_t t, double v) override {
    return app->add_fast(logical_id, t, v);
  }

  virtual leveldb::Status commit(bool release_labels = false) override {
    return app->commit(release_labels);
  }

  virtual leveldb::Status rollback() override {
    return app->rollback();
  }

  ~DBAppender() {}
};

}  // namespace db
}  // namespace tsdb

#endif