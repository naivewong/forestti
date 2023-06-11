#include <boost/filesystem.hpp>

#include "db/DB.hpp"
#include "gtest/gtest.h"
#include "label/EqualMatcher.hpp"
#include "label/RegexMatcher.hpp"

namespace tsdb {
namespace db {

std::deque<std::pair<int64_t, double>> generate_data(int64_t min, int64_t max,
                                                     int64_t step) {
  std::deque<std::pair<int64_t, double>> d;
  for (int64_t i = min; i < max; i += step)
    d.emplace_back(i + 10 * static_cast<int64_t>(static_cast<float>(rand()) /
                                                 static_cast<float>(RAND_MAX)),
                   static_cast<float>(rand()) / static_cast<float>(RAND_MAX));
  return d;
}

class TSDBTest : public testing::Test {
 public:
  std::unique_ptr<DB> db;
  std::string path;

  TSDBTest() : path("/tmp/tsdb_test") {}

  void clean() { boost::filesystem::remove_all(path); }

  leveldb::Status setup(bool need_clean = true) {
    db.reset();
    if (need_clean) clean();

    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 10 << 13;  // 10KB.
    options.max_file_size = 10 << 10;      // 10KB.
    leveldb::DB* tmp_db;
    leveldb::Status st = leveldb::DB::Open(options, path, &tmp_db);
    if (!st.ok()) return st;

    db.reset(new DB(path, DefaultOptions,
                    reinterpret_cast<leveldb::DBImpl*>(tmp_db)));
    if (db->error()) return leveldb::Status::Corruption(db->error().error());

    return st;
  }
};

TEST_F(TSDBTest, Test1) {
  ASSERT_TRUE(setup().ok());

  {
    auto data = generate_data(1, 12 * 3600 * 1000, 1000);
    {
      std::deque<label::Labels> lsets({
          {{"a", "1"}, {"b", "1"}},
          {{"a", "2"}, {"c", "1"}},
          {{"a", "3"}, {"d", "1"}},
          {{"a", "4"}, {"e", "1"}},
          {{"a", "5"}, {"f", "1"}},
          {{"a", "6"}, {"g", "1"}},
          {{"a", "7"}, {"h", "1"}},
          {{"a", "8"}, {"i", "1"}},
          {{"a", "9"}, {"j", "1"}},
          {{"a", "10"}, {"k", "1"}},
      });
      {
        auto app = db->appender();
        for (auto& d : data) {
          for (auto& lset : lsets) {
            auto p = app->add(lset, d.first, d.second);
            ASSERT_TRUE(p.second.ok());
          }
          auto e = app->commit();
          ASSERT_TRUE(e.ok());
        }
      }
      std::cout << "append finished" << std::endl;
      sleep(10);
      {
        auto q = db->querier(1, 12 * 3600 * 1000);
        ASSERT_FALSE(q.second);
        auto s = q.first->select({std::shared_ptr<label::MatcherInterface>(
                                      new label::EqualMatcher("b", "1")),
                                  std::shared_ptr<label::MatcherInterface>(
                                      new label::RegexMatcher("a", "*"))});
        ASSERT_TRUE(s);
        while (s->next()) {
          auto si = s->at();
          auto it = si->iterator();
          int i = 0;
          while (it->next()) {
            ASSERT_EQ(it->at(), data[i]);
            ++i;
          }
        }
      }
    }
    // {
    //   int64_t mint = data[data.size() / 4].first;
    //   {
    //     // test DB::del().
    //     auto e = (db->del(mint, 4 * 3600 * 1000,
    //                      {std::shared_ptr<label::MatcherInterface>(
    //                          new label::EqualMatcher("a", "1"))}));
    //     std::cout << e.error() << std::endl;
    //     ASSERT_FALSE(e);

    //     e = (db->del(1, mint,
    //                 {std::shared_ptr<label::MatcherInterface>(
    //                     new label::EqualMatcher("k", "1"))}));
    //     std::cout << e.error() << std::endl;
    //     ASSERT_FALSE(e);
    //   }
    //   ASSERT_FALSE(db->reload());
    //   {
    //     auto q = db->querier(1, 4 * 3600 * 1000);
    //     ASSERT_FALSE(q.second);
    //     auto s = q.first->select({std::shared_ptr<label::MatcherInterface>(
    //         new label::EqualMatcher("a", "1"))});
    //     ASSERT_TRUE(s);
    //     while (s->next()) {
    //       auto si = s->at();
    //       auto it = si->iterator();
    //       int i = 0;
    //       while (it->next()) {
    //         ASSERT_EQ(it->at(), data[i]);
    //         ++i;
    //       }
    //       ASSERT_EQ(i, data.size() / 4);
    //     }
    //   }
    //   {
    //     auto q = db->querier(1, 4 * 3600 * 1000);
    //     ASSERT_FALSE(q.second);
    //     auto s = q.first->select({std::shared_ptr<label::MatcherInterface>(
    //         new label::EqualMatcher("k", "1"))});
    //     ASSERT_TRUE(s);
    //     while (s->next()) {
    //       auto si = s->at();
    //       auto it = si->iterator();
    //       int i = data.size() / 4 + 1;
    //       while (it->next()) {
    //         ASSERT_EQ(it->at(), data[i]);
    //         ++i;
    //       }
    //       ASSERT_EQ(i, 4 * 3600);
    //     }
    //   }
    // }
  }
}

}  // namespace db
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}