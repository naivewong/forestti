#include "disk/log_manager.h"

#include <boost/filesystem.hpp>
#include <vector>

#include "gtest/gtest.h"
#include "util/random.h"
#include "util/testutil.h"

namespace tsdb {
namespace disk {

class LogManagerTest : public testing::Test {};

TEST_F(LogManagerTest, Test1) {
  boost::filesystem::remove_all("/tmp/test_log_manager");
  LogManager lm(leveldb::Env::Default(), "/tmp/test_log_manager", "kkp");

  std::vector<std::string> records;
  std::vector<uint64_t> pos;
  leveldb::Random rnd(1996);
  int num_records = 5000000;
  for (int i = 0; i < num_records; i++) {
    std::string s;
    leveldb::test::RandomString(&rnd, rnd.Uniform(500), &s);
    records.push_back(s);
    pos.push_back(lm.add_record(records.back()));
  }

  for (size_t i = 0; i < num_records; i++) {
    auto p = lm.read_record(pos[i]);
    ASSERT_EQ(records[i], std::string(p.first.data(), p.first.size()));
    delete[] p.second;
  }
}

}  // namespace disk
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}