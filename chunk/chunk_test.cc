#include <boost/filesystem.hpp>
#include <deque>
#include <iostream>

#include "chunk/ChunkReader.hpp"
#include "chunk/ChunkWriter.hpp"
#include "chunk/XORChunk.hpp"
#include "gtest/gtest.h"
#include "head/HeadUtils.hpp"
#include "leveldb/db.h"

namespace tsdb {
namespace chunk {

class ChunkTest : public testing::Test {
 public:
  std::shared_ptr<ChunkMeta> generate_chunk(uint64_t sid, int64_t starting_time,
                                            int num) {
    auto c = new XORChunk();
    auto app = c->appender();
    for (int i = 0; i < num; i++)
      app->append(starting_time + i * 1000, starting_time + i * 1000);
    return std::shared_ptr<ChunkMeta>(new ChunkMeta(
        head::pack_chunk_id(sid, 0), std::shared_ptr<ChunkInterface>(c),
        starting_time, starting_time + (num - 1) * 1000));
  }

  void validate_chunk(ChunkInterface* c, int64_t starting_time, int num) {
    auto it = c->iterator();
    int count = 0;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ(starting_time + count * 1000, p.first);
      ASSERT_EQ(starting_time + count * 1000, p.second);
      count++;
    }
    ASSERT_EQ(num, count);
  }
};

TEST_F(ChunkTest, Test1) {
  boost::filesystem::remove_all("/tmp/chunk_test");
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status s = leveldb::DB::Open(options, "/tmp/chunk_test", &db);
  std::cout << s.ToString() << std::endl;
  ASSERT_TRUE(s.ok());
  std::deque<std::shared_ptr<ChunkMeta>> d;
  d.push_back(generate_chunk(1, 100, 10));
  d.push_back(generate_chunk(1, 20000, 10));
  d.push_back(generate_chunk(19, 0, 3));
  {
    LevelDBChunkWriter writer(db);
    writer.write_chunks(d);
  }
  {
    LevelDBChunkReader reader(db);
    auto p = reader.chunk(1, 100);
    ASSERT_TRUE(p.second);
    validate_chunk(p.first.get(), 100, 10);

    p = reader.chunk(1, 20000);
    ASSERT_TRUE(p.second);
    validate_chunk(p.first.get(), 20000, 10);

    p = reader.chunk(19, 0);
    ASSERT_TRUE(p.second);
    validate_chunk(p.first.get(), 0, 3);

    p = reader.chunk(19, 2900);
    ASSERT_FALSE(p.second);
  }
  delete db;
}

TEST_F(ChunkTest, TestHeaderXOR) {
  XORChunk c(17);
  auto app = c.appender();
  for (int i = 0; i < 10; i++) app->append(i, i);

  XORChunk c2(c.bytes(), c.size());
  auto it = c2.xor_iterator();
  int count = 0;
  while (it->next()) {
    ASSERT_EQ(count, it->at().first);
    ASSERT_EQ(count, it->at().second);
    count++;
  }
  ASSERT_EQ(10, count);
}

}  // namespace chunk
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}