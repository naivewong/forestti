#include <boost/filesystem.hpp>

#include "base/ThreadPool.hpp"
#include "block/Block.hpp"
#include "compact/LeveledCompactor.hpp"
#include "db/DBUtils.hpp"
#include "db/db_impl.h"
#include "gtest/gtest.h"
#include "head/Head.hpp"
#include "label/EqualMatcher.hpp"
#include "querier/BlockQuerier.hpp"
#include "querier/ChunkSeriesIterator.hpp"
#include "querier/Querier.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"

namespace tsdb {
namespace compact {

class CompactTest : public testing::Test {
 public:
  std::shared_ptr<head::Head> head;
  std::unique_ptr<LeveledCompactor> compactor;
  std::string path;

  CompactTest() : path("/tmp/head_test") {}

  void clean(const std::string& path) { boost::filesystem::remove_all(path); }

  leveldb::Status setup(bool need_clean = true) {
    head.reset();
    if (need_clean) clean(path);

    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 10 << 13;  // 10KB.
    options.max_file_size = 10 << 10;      // 10KB.
    leveldb::DB* tmp_db;
    leveldb::Status st = leveldb::DB::Open(options, path, &tmp_db);
    if (!st.ok()) return st;

    std::shared_ptr<base::ThreadPool> pool =
        std::make_shared<base::ThreadPool>(8);
    std::unique_ptr<wal::WAL> wal = std::unique_ptr<wal::WAL>(
        new wal::WAL(tsdbutil::filepath_join(path, "wal"), pool,
                     db::DefaultOptions.wal_segment_size));
    head.reset(new head::Head(db::DefaultOptions.block_ranges[0],
                              std::move(wal), pool, path,
                              reinterpret_cast<leveldb::DBImpl*>(tmp_db)));
    if (head->init(-1)) return leveldb::Status::IOError("cannot init head");

    compactor.reset(
        new LeveledCompactor(db::DefaultOptions.block_ranges,
                             std::make_shared<base::Channel<char>>()));

    return st;
  }

  leveldb::Status compact(std::string& bp) {
    auto p = compactor->write(path, head, head->MinTime(), head->MaxTime() + 1,
                              nullptr);
    if (p.second) return leveldb::Status::IOError(p.second.error());
    if (ulid::CompareULIDs(p.first, ulid::ULID()) == 0)
      return leveldb::Status::IOError("empty block");
    head->truncate(head->MaxTime() + 1);
    bp = ulid::Marshal(p.first);
    return leveldb::Status::OK();
  }
};

TEST_F(CompactTest, Test1) {
  ASSERT_TRUE(setup().ok());
  auto app = head->appender();
  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 12;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    std::vector<int64_t> tmp_times;
    std::vector<double> tmp_values;
    tmp_times.push_back(0);
    tmp_values.push_back(0);
    for (int j = 0; j < num_samples; j++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      tmp_times.push_back(t);
      tmp_values.push_back(v);
      leveldb::Status st = app->add_fast(i + 1, t, v);
      ASSERT_TRUE(st.ok());
    }
    times.push_back(tmp_times);
    values.push_back(tmp_values);
    ASSERT_TRUE(app->commit().ok());
  }

  std::string bp1;
  leveldb::Status st = compact(bp1);
  printf("compact():%s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());

  std::shared_ptr<block::Block> b = std::make_shared<block::Block>(
      path + "/" + bp1, block::OriginalBlock, head->index().first);
  printf("Block:%s\n", b->error().error().c_str());
  ASSERT_FALSE(b->error());
  {
    std::shared_ptr<querier::QuerierInterface> bq =
        std::make_shared<querier::BlockQuerier>(b, 0, 10000000, false);
    std::shared_ptr<querier::QuerierInterface> hq =
        std::make_shared<querier::BlockQuerier>(head, 0, 10000000, true);
    querier::Querier q({bq, hq}, head->index().first);

    for (int i = 0; i < num_ts; i++) {
      tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label_" + std::to_string(j),
                          "value_" + std::to_string(i));

      std::vector<std::shared_ptr<label::MatcherInterface>> matchers;
      matchers.push_back(
          std::make_shared<label::EqualMatcher>(lset[0].label, lset[0].value));
      std::shared_ptr<querier::SeriesSetInterface> ss = q.select(matchers);

      int ts_count = 0;
      while (ss->next()) {
        std::shared_ptr<querier::SeriesInterface> s = ss->at();
        ASSERT_EQ((uint64_t)(i + 1), s->tsid());
        ASSERT_EQ(lset, s->labels());

        std::unique_ptr<querier::SeriesIteratorInterface> it = s->iterator();
        int count = 0;
        while (it->next()) {
          ASSERT_EQ(times[i][count], it->at().first);
          ASSERT_EQ(values[i][count], it->at().second);
          count++;
        }
        ASSERT_EQ(num_samples + 1, count);
        ts_count++;
      }
      ASSERT_EQ(1, ts_count);
    }
    printf("Finished\n");
  }

  for (int i = 0; i < num_ts; i++) {
    for (int j = 0; j < num_samples; j++) {
      int64_t t = (j + 1 + num_samples + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      times[i].push_back(t);
      values[i].push_back(v);
      leveldb::Status st = app->add_fast(i + 1, t, v);
      ASSERT_TRUE(st.ok());
    }
    ASSERT_TRUE(app->commit().ok());
  }

  std::string bp2;
  st = compact(bp2);
  printf("compact():%s\n", st.ToString().c_str());
  ASSERT_TRUE(st.ok());

  std::shared_ptr<block::Block> b2 = std::make_shared<block::Block>(
      path + "/" + bp2, block::OriginalBlock, head->index().first);
  printf("Block:%s\n", b2->error().error().c_str());
  ASSERT_FALSE(b2->error());

  {
    std::shared_ptr<querier::QuerierInterface> bq1 =
        std::make_shared<querier::BlockQuerier>(b, 0, 10000000, false);
    std::shared_ptr<querier::QuerierInterface> bq2 =
        std::make_shared<querier::BlockQuerier>(b2, 0, 10000000, false);
    std::shared_ptr<querier::QuerierInterface> hq =
        std::make_shared<querier::BlockQuerier>(head, 0, 10000000, true);
    querier::Querier q({bq1, bq2, hq}, head->index().first);

    for (int i = 0; i < num_ts; i++) {
      tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label_" + std::to_string(j),
                          "value_" + std::to_string(i));

      std::vector<std::shared_ptr<label::MatcherInterface>> matchers;
      matchers.push_back(
          std::make_shared<label::EqualMatcher>(lset[0].label, lset[0].value));
      std::shared_ptr<querier::SeriesSetInterface> ss = q.select(matchers);

      int ts_count = 0;
      while (ss->next()) {
        std::shared_ptr<querier::SeriesInterface> s = ss->at();
        ASSERT_EQ((uint64_t)(i + 1), s->tsid());
        ASSERT_EQ(lset, s->labels());

        std::unique_ptr<querier::SeriesIteratorInterface> it = s->iterator();
        int count = 0;
        while (it->next()) {
          ASSERT_EQ(times[i][count], it->at().first);
          ASSERT_EQ(values[i][count], it->at().second);
          count++;
        }
        ASSERT_EQ(2 * num_samples + 1, count);
        ts_count++;
      }
      ASSERT_EQ(1, ts_count);
    }
    printf("Finished\n");
  }
}

}  // namespace compact
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}