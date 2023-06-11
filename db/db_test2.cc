#include "chunk/XORChunk.hpp"
#include "db/version_set.h"
#include "gtest/gtest.h"
#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/MemSeries.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"
#include "leveldb/cache.h"
#include "querier/tsdb_querier.h"

namespace tsdb {
namespace db {

class TSDBTest : public testing::Test {
 public:
  void head_add(int64_t st, int64_t off = 0) {
    auto app = head_->appender();
    for (int i = 0; i < num_ts; i++) {
      ::tsdb::label::Labels lset;
      for (int j = 0; j < 10; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(i));
      lset.emplace_back("label_all", "label_all");

      auto r = app->add(lset, 0, 0 + off);
      if (!r.second.ok())
        std::cout << "TSDBTest::insert_tuple_with_labels failed" << std::endl;
      else if (r.first != ((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                              (uint64_t)(i % PRESERVED_BLOCKS))
        std::cout << "TSDBTest::insert_tuple_with_labels wrong id exp:" << i
                  << " got:" << r.first << std::endl;

      for (int k = 1; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      st + k * 1000, st + k * 1000 + off);
    }

    app->commit();
  }

  void head_add_fast(int64_t st, int64_t off = 0) {
    auto app = head_->appender();
    for (int i = 0; i < num_ts; i++) {
      for (int k = 0; k < tuple_size; k++)
        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                          (uint64_t)(i % PRESERVED_BLOCKS),
                      st + k * 1000, st + k * 1000 + off);
    }

    app->commit();
  }

  void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
    num_ts = num_ts_;
    tuple_size = tuple_size_;
    num_tuple = num_tuple_;
    head::MEM_TUPLE_SIZE = tuple_size_;
  }

  void setup(const std::string& dir, const std::string& snapshot_dir = "",
             bool sync_api = true, leveldb::DB* db = nullptr) {
    ::tsdb::head::Head* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::Head(dir, snapshot_dir, db, sync_api));
    db->SetHead(head_.get());
  }

  int num_ts;
  int tuple_size;
  int num_tuple;
  std::unique_ptr<::tsdb::head::Head> head_;
};

TEST_F(TSDBTest, TestL0Compaction) {
  leveldb::L0_MERGE_FACTOR = 4;
  std::string path = "/tmp/tsdb_test2";
  boost::filesystem::remove_all(path);
  leveldb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10 << 14;  // 10KB.
  options.max_file_size = 10 << 10;      // 10KB.
  options.use_log = false;

  leveldb::DB* db;
  ASSERT_TRUE(leveldb::DB::Open(options, path, &db).ok());
  set_parameters(10, 16, 600);
  setup(path, "", true, db);

  head_add(0, 0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000, 0);
  }

  sleep(3);
  db->PrintLevel();

  querier::TSDBQuerier* q =
      new querier::TSDBQuerier(db, head_.get(), 1600000, 4200000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label0", "label0_0")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4201);

    tsid++;
  }
  ASSERT_EQ(2, tsid);

  // close and reopen to test log.
  delete q;
  delete db;
  ASSERT_TRUE(leveldb::DB::Open(options, path, &db).ok());
  setup(path, "", true, db);
  sleep(3);
  db->PrintLevel();
  q = new querier::TSDBQuerier(db, head_.get(), 1600000, 4200000);
  matchers = std::vector<::tsdb::label::MatcherInterface*>(
      {new ::tsdb::label::EqualMatcher("label_all", "label_all")});
  ss.reset(q->select(matchers).release());
  tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "label_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1600;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_DOUBLE_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4201);

    tsid++;
  }
  ASSERT_EQ(num_ts + 1, tsid);
  delete q;
  delete db;
}

TEST_F(TSDBTest, TestGCThenCleanLogs) {
  leveldb::L0_MERGE_FACTOR = 4;
  std::string path = "/tmp/tsdb_test2";
  boost::filesystem::remove_all(path);
  leveldb::Options options;
  options.create_if_missing = true;
  // options.write_buffer_size = 10 << 14;  // 10KB.
  // options.max_file_size = 10 << 10;      // 10KB.
  options.use_log = false;

  head::MAX_HEAD_SAMPLES_LOG_SIZE = 64 * 1024 * 1024;

  leveldb::DB* db;
  ASSERT_TRUE(leveldb::DB::Open(options, path, &db).ok());
  set_parameters(100000, 32, 10);
  setup(path, "", true, db);

  head_->enable_concurrency();

  head_add(0, 0);
  for (int tuple = 1; tuple < num_tuple; tuple++) {
    head_add_fast(tuple * tuple_size * 1000, 0);
  }

  sleep(3);
  db->PrintLevel();

  std::thread t1(&head::Head::full_migrate, head_.get());
  std::thread t2(&head::Head::clean_samples_logs, head_.get());

  t1.join();
  t2.join();

  delete db;
}

}  // namespace db
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}