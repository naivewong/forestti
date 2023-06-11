#include <boost/filesystem.hpp>

#include "gtest/gtest.h"
#include "index/IndexReader.hpp"
#include "index/IndexWriter.hpp"

namespace tsdb {
namespace index {

class IndexTest : public testing::Test {};

TEST_F(IndexTest, Test1) {
  std::string path = "/tmp/indextest";
  boost::filesystem::remove_all(path);
  boost::filesystem::create_directory(path);

  int num_ts = 100;
  int num_chunks = 5;
  std::vector<tsdb::label::Labels> lsets;
  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back("label" + std::to_string(j),
                        "value" + std::to_string(j) + "_" + std::to_string(i));
    lsets.push_back(lset);
  }

  {
    IndexWriter writer(path + "/index");
    for (int i = 0; i < num_ts; i++) {
      std::deque<std::shared_ptr<chunk::ChunkMeta>> chunks;
      for (int j = 0; j < num_chunks; j++)
        chunks.push_back(std::make_shared<chunk::ChunkMeta>(
            i + 1, j * 3600 * 1000, (j + 1) * 3600 * 1000));
      writer.add_series(i + 1, lsets[i], chunks);
    }
  }

  {
    IndexReader reader(path + "/index");
    for (int i = 0; i < num_ts; i++) {
      std::deque<std::shared_ptr<chunk::ChunkMeta>> chunks;
      reader.series(i + 1, chunks);
      ASSERT_EQ(num_chunks, chunks.size());
      for (int j = 0; j < num_chunks; j++) {
        ASSERT_EQ(i + 1, chunks[j]->ref);
        ASSERT_EQ(j * 3600 * 1000, chunks[j]->min_time);
        ASSERT_EQ((j + 1) * 3600 * 1000, chunks[j]->max_time);
      }
    }
  }
}

}  // namespace index
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}