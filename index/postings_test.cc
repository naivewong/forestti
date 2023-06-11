#include "gtest/gtest.h"
#include "index/VectorPostings.hpp"
#include "index/prefix_postings.h"
#include "third_party/thread_pool.h"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace index {

class PostingsTest : public testing::Test {};

TEST_F(PostingsTest, TestPrefixPostings1) {
  {
    PrefixPostings p;
    p.insert(10);
    p.insert(65536 + 9);
    p.insert(8);
    p.insert(65536 + 7);
    p.insert(6);
    p.insert(65536 + 5);
    p.insert(4);
    p.insert(65536 + 3);
    p.insert(2);
    p.insert(65536 + 1);

    ASSERT_TRUE(p.next());
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(6, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(8, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(10, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 1, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 3, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 5, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 7, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 9, p.at());
  }
  {
    PrefixPostings p;
    p.insert(10);
    p.insert(65536 + 9);
    p.insert(8);
    p.insert(65536 + 7);
    p.insert(6);
    p.insert(65536 + 5);
    p.insert(4);
    p.insert(65536 + 3);
    p.insert(2);
    p.insert(65536 + 1);

    ASSERT_TRUE(p.seek(0));
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.seek(4));
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.seek(7));
    ASSERT_EQ(8, p.at());

    ASSERT_TRUE(p.seek(11));
    ASSERT_EQ(65536 + 1, p.at());

    ASSERT_FALSE(p.seek(65536 + 10));
  }
  {
    PrefixPostingsV2 p;
    p.insert(10);
    p.insert(65536 + 9);
    p.insert(8);
    p.insert(65536 + 7);
    p.insert(6);
    p.insert(65536 + 5);
    p.insert(4);
    p.insert(65536 + 3);
    p.insert(2);
    p.insert(65536 + 1);

    ASSERT_TRUE(p.next());
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(6, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(8, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(10, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 1, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 3, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 5, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 7, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 9, p.at());

    p.remove(65536 + 3);
    p.remove(65536 + 7);
    p.remove(65536 + 1);
    p.remove(65536 + 5);
    p.remove(65536 + 10);
    p.remove(65536 + 9);
    p.remove(65536 + 90);

    p.reset_cursor();
    ASSERT_TRUE(p.next());
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(6, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(8, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(10, p.at());
    ASSERT_FALSE(p.next());
  }
  {
    PrefixPostingsV2 p;
    p.insert(10);
    p.insert(65536 + 9);
    p.insert(8);
    p.insert(65536 + 7);
    p.insert(6);
    p.insert(65536 + 5);
    p.insert(4);
    p.insert(65536 + 3);
    p.insert(2);
    p.insert(65536 + 1);

    ASSERT_TRUE(p.seek(0));
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.seek(4));
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.seek(7));
    ASSERT_EQ(8, p.at());

    ASSERT_TRUE(p.seek(11));
    ASSERT_EQ(65536 + 1, p.at());

    ASSERT_FALSE(p.seek(65536 + 10));
  }
  {
    PrefixPostingsV3 p;
    p.insert(10);
    p.insert(65536 + 9);
    p.insert(8);
    p.insert(65536 + 7);
    p.insert(6);
    p.insert(65536 + 5);
    p.insert(4);
    p.insert(65536 + 3);
    p.insert(2);
    p.insert(65536 + 1);

    ASSERT_TRUE(p.next());
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(6, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(8, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(10, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 1, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 3, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 5, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 7, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(65536 + 9, p.at());

    p.remove(65536 + 3);
    p.remove(65536 + 7);
    p.remove(65536 + 1);
    p.remove(65536 + 5);
    p.remove(65536 + 10);
    p.remove(65536 + 9);
    p.remove(65536 + 90);

    p.reset_cursor();
    ASSERT_TRUE(p.next());
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(6, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(8, p.at());
    ASSERT_TRUE(p.next());
    ASSERT_EQ(10, p.at());
    ASSERT_FALSE(p.next());
  }
  {
    PrefixPostingsV3 p;
    p.insert(10);
    p.insert(65536 + 9);
    p.insert(8);
    p.insert(65536 + 7);
    p.insert(6);
    p.insert(65536 + 5);
    p.insert(4);
    p.insert(65536 + 3);
    p.insert(2);
    p.insert(65536 + 1);

    ASSERT_TRUE(p.seek(0));
    ASSERT_EQ(2, p.at());
    ASSERT_TRUE(p.seek(4));
    ASSERT_EQ(4, p.at());
    ASSERT_TRUE(p.seek(7));
    ASSERT_EQ(8, p.at());

    ASSERT_TRUE(p.seek(11));
    ASSERT_EQ(65536 + 1, p.at());

    ASSERT_FALSE(p.seek(65536 + 10));
  }
}

TEST_F(PostingsTest, TestPrefixPostings2) {
  {
    PrefixPostings p;
    std::vector<uint64_t> v;
    p.insert(2);
    v.push_back(2);
    int num = 1000;
    for (int i = 1; i < num; i++) {
      v.push_back(v.back() + rand() % 25 + 2);
      p.insert(v.back());
    }

    {
      // Iteration.
      for (int i = 0; i < num; i++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(v[i], p.at());
      }
      ASSERT_FALSE(p.next());
      p.reset_cursor();
    }
    {
      // Seek.
      ASSERT_TRUE(p.seek(v[0] - 1));
      ASSERT_EQ(v[0], p.at());
      ASSERT_TRUE(p.seek(v[4]));
      ASSERT_EQ(v[4], p.at());
      ASSERT_TRUE(p.seek(v[500] - 1));
      ASSERT_EQ(v[500], p.at());
      ASSERT_TRUE(p.seek(v[600] + 1));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[600] + 1));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[0]));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[600]));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[999]));
      ASSERT_EQ(v[999], p.at());
      ASSERT_FALSE(p.seek(v[999] + 10));
    }
  }
  {
    PrefixPostingsV2 p;
    std::vector<uint64_t> v;
    p.insert(2);
    v.push_back(2);
    int num = 1000;
    for (int i = 1; i < num; i++) {
      v.push_back(v.back() + rand() % 25 + 2);
      p.insert(v.back());
    }

    {
      // Iteration.
      for (int i = 0; i < num; i++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(v[i], p.at());
      }
      ASSERT_FALSE(p.next());
      p.reset_cursor();
    }
    {
      // Seek.
      ASSERT_TRUE(p.seek(v[0] - 1));
      ASSERT_EQ(v[0], p.at());
      ASSERT_TRUE(p.seek(v[4]));
      ASSERT_EQ(v[4], p.at());
      ASSERT_TRUE(p.seek(v[500] - 1));
      ASSERT_EQ(v[500], p.at());
      ASSERT_TRUE(p.seek(v[600] + 1));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[600] + 1));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[0]));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[600]));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[999]));
      ASSERT_EQ(v[999], p.at());
      ASSERT_FALSE(p.seek(v[999] + 10));
    }
  }
  {
    PrefixPostingsV3 p;
    std::vector<uint64_t> v;
    p.insert(2);
    v.push_back(2);
    int num = 1000;
    for (int i = 1; i < num; i++) {
      v.push_back(v.back() + rand() % 25 + 2);
      p.insert(v.back());
    }

    {
      // Iteration.
      for (int i = 0; i < num; i++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(v[i], p.at());
      }
      ASSERT_FALSE(p.next());
      p.reset_cursor();
    }
    {
      // Seek.
      ASSERT_TRUE(p.seek(v[0] - 1));
      ASSERT_EQ(v[0], p.at());
      ASSERT_TRUE(p.seek(v[4]));
      ASSERT_EQ(v[4], p.at());
      ASSERT_TRUE(p.seek(v[500] - 1));
      ASSERT_EQ(v[500], p.at());
      ASSERT_TRUE(p.seek(v[600] + 1));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[600] + 1));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[0]));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[600]));
      ASSERT_EQ(v[601], p.at());
      ASSERT_TRUE(p.seek(v[999]));
      ASSERT_EQ(v[999], p.at());
      ASSERT_FALSE(p.seek(v[999] + 10));
    }
  }
}

struct SeekEntry {
  uint64_t seek;
  uint64_t val;
  bool found;
  SeekEntry(uint64_t seek, uint64_t val, bool found)
      : seek(seek), val(val), found(found) {}
};

TEST_F(PostingsTest, BenchOneBlock) {
  int num = 1000;
  std::vector<uint64_t> ls1, ls2;
  ls1.push_back(2);
  ls2.push_back(655360);
  for (int i = 1; i < num; i++) {
    ls1.push_back(ls1.back() + rand() % 10 + 2);
    ls2.push_back(ls2.back() + rand() % 10 + 2);
  }

  std::vector<SeekEntry> table1 = {SeekEntry(ls1[0] - 1, ls1[0], true),
                                   SeekEntry(ls1[50], ls1[50], true),
                                   SeekEntry(ls1[100], ls1[100], true),
                                   SeekEntry(ls1[150] + 1, ls1[151], true),
                                   SeekEntry(ls1[200], ls1[200], true),
                                   SeekEntry(ls1[250], ls1[250], true),
                                   SeekEntry(ls1[300] + 1, ls1[301], true),
                                   SeekEntry(ls1[350], ls1[350], true),
                                   SeekEntry(ls1[400], ls1[400], true),
                                   SeekEntry(ls1[450] + 1, ls1[451], true),
                                   SeekEntry(ls1[500], ls1[500], true),
                                   SeekEntry(ls1[550], ls1[550], true),
                                   SeekEntry(ls1[600] + 1, ls1[601], true),
                                   SeekEntry(ls1[650], ls1[650], true),
                                   SeekEntry(ls1[700], ls1[700], true),
                                   SeekEntry(ls1[750] + 1, ls1[751], true),
                                   SeekEntry(ls1[800], ls1[800], true),
                                   SeekEntry(ls1[850], ls1[850], true),
                                   SeekEntry(ls1[900] + 1, ls1[901], true),
                                   SeekEntry(ls1[950], ls1[950], true),
                                   SeekEntry(ls1[999], ls1[999], true)};
  std::vector<SeekEntry> table2 = {SeekEntry(ls2[0] - 1, ls2[0], true),
                                   SeekEntry(ls2[50], ls2[50], true),
                                   SeekEntry(ls2[100], ls2[100], true),
                                   SeekEntry(ls2[150] + 1, ls2[151], true),
                                   SeekEntry(ls2[200], ls2[200], true),
                                   SeekEntry(ls2[250], ls2[250], true),
                                   SeekEntry(ls2[300] + 1, ls2[301], true),
                                   SeekEntry(ls2[350], ls2[350], true),
                                   SeekEntry(ls2[400], ls2[400], true),
                                   SeekEntry(ls2[450] + 1, ls2[451], true),
                                   SeekEntry(ls2[500], ls2[500], true),
                                   SeekEntry(ls2[550], ls2[550], true),
                                   SeekEntry(ls2[600] + 1, ls2[601], true),
                                   SeekEntry(ls2[650], ls2[650], true),
                                   SeekEntry(ls2[700], ls2[700], true),
                                   SeekEntry(ls2[750] + 1, ls2[751], true),
                                   SeekEntry(ls2[800], ls2[800], true),
                                   SeekEntry(ls2[850], ls2[850], true),
                                   SeekEntry(ls2[900] + 1, ls2[901], true),
                                   SeekEntry(ls2[950], ls2[950], true),
                                   SeekEntry(ls2[999], ls2[999], true)};

  {
    VectorPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.push_back(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls1[j], p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianIteration_one_block_key=0 time:%dms mem_size:%u\n",
           t.since_start_nano() / 1000000, p.mem_size());
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls1[j], p.at());
      }
      p.reset_cursor();
    }
    printf(
        "prefixCompressedPostingsIteration_one_block_key=0 time:%dms "
        "mem_size:%u\n",
        t.since_start_nano() / 1000000, p.mem_size());
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls1[j], p.at());
      }
      p.reset_cursor();
    }
    printf(
        "prefixCompressedPostingsV2Iteration_one_block_key=0 time:%dms "
        "mem_size:%u\n",
        t.since_start_nano() / 1000000, p.mem_size());
  }

  {
    VectorPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.push_back(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table1.size(); j++) {
        ASSERT_EQ(table1[j].found, p.seek(table1[j].seek));
        ASSERT_EQ(table1[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianSeek_one_block_key=0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table1.size(); j++) {
        ASSERT_EQ(table1[j].found, p.seek(table1[j].seek));
        ASSERT_EQ(table1[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsSeek_one_block_key=0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table1.size(); j++) {
        ASSERT_EQ(table1[j].found, p.seek(table1[j].seek));
        ASSERT_EQ(table1[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsV2Seek_one_block_key=0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }

  {
    VectorPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.push_back(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls2[j], p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianIteration_one_block_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls2[j], p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsIteration_one_block_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls2[j], p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsV2Iteration_one_block_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }

  {
    VectorPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.push_back(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table2.size(); j++) {
        ASSERT_EQ(table2[j].found, p.seek(table2[j].seek));
        ASSERT_EQ(table2[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianSeek_one_block_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table2.size(); j++) {
        ASSERT_EQ(table2[j].found, p.seek(table2[j].seek));
        ASSERT_EQ(table2[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsSeek_one_block_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table2.size(); j++) {
        ASSERT_EQ(table2[j].found, p.seek(table2[j].seek));
        ASSERT_EQ(table2[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsV2Seek_one_block_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
}

TEST_F(PostingsTest, BenchManyBlocks) {
  int num = 100000;
  std::vector<uint64_t> ls1, ls2;
  ls1.push_back(2);
  ls2.push_back(655360);
  for (int i = 1; i < num; i++) {
    ls1.push_back(ls1.back() + rand() % 25 + 2);
    ls2.push_back(ls2.back() + rand() % 25 + 2);
  }

  std::vector<SeekEntry> table1 = {SeekEntry(ls1[0] - 1, ls1[0], true),
                                   SeekEntry(ls1[1000], ls1[1000], true),
                                   SeekEntry(ls1[1001], ls1[1001], true),
                                   SeekEntry(ls1[2000] + 1, ls1[2001], true),
                                   SeekEntry(ls1[3000], ls1[3000], true),
                                   SeekEntry(ls1[3001], ls1[3001], true),
                                   SeekEntry(ls1[4000] + 1, ls1[4001], true),
                                   SeekEntry(ls1[5000], ls1[5000], true),
                                   SeekEntry(ls1[5001], ls1[5001], true),
                                   SeekEntry(ls1[6000] + 1, ls1[6001], true),
                                   SeekEntry(ls1[10000], ls1[10000], true),
                                   SeekEntry(ls1[10001], ls1[10001], true),
                                   SeekEntry(ls1[20000] + 1, ls1[20001], true),
                                   SeekEntry(ls1[30000], ls1[30000], true),
                                   SeekEntry(ls1[30001], ls1[30001], true),
                                   SeekEntry(ls1[40000] + 1, ls1[40001], true),
                                   SeekEntry(ls1[50000], ls1[50000], true),
                                   SeekEntry(ls1[50001], ls1[50001], true),
                                   SeekEntry(ls1[60000] + 1, ls1[60001], true),
                                   SeekEntry(ls1[70000], ls1[70000], true),
                                   SeekEntry(ls1[70001], ls1[70001], true),
                                   SeekEntry(ls1[80000] + 1, ls1[80001], true),
                                   SeekEntry(ls1[99999], ls1[99999], true)};
  std::vector<SeekEntry> table2 = {SeekEntry(ls2[0] - 1, ls2[0], true),
                                   SeekEntry(ls2[1000], ls2[1000], true),
                                   SeekEntry(ls2[1001], ls2[1001], true),
                                   SeekEntry(ls2[2000] + 1, ls2[2001], true),
                                   SeekEntry(ls2[3000], ls2[3000], true),
                                   SeekEntry(ls2[3001], ls2[3001], true),
                                   SeekEntry(ls2[4000] + 1, ls2[4001], true),
                                   SeekEntry(ls2[5000], ls2[5000], true),
                                   SeekEntry(ls2[5001], ls2[5001], true),
                                   SeekEntry(ls2[6000] + 1, ls2[6001], true),
                                   SeekEntry(ls2[10000], ls2[10000], true),
                                   SeekEntry(ls2[10001], ls2[10001], true),
                                   SeekEntry(ls2[20000] + 1, ls2[20001], true),
                                   SeekEntry(ls2[30000], ls2[30000], true),
                                   SeekEntry(ls2[30001], ls2[30001], true),
                                   SeekEntry(ls2[40000] + 1, ls2[40001], true),
                                   SeekEntry(ls2[50000], ls2[50000], true),
                                   SeekEntry(ls2[50001], ls2[50001], true),
                                   SeekEntry(ls2[60000] + 1, ls2[60001], true),
                                   SeekEntry(ls2[70000], ls2[70000], true),
                                   SeekEntry(ls2[70001], ls2[70001], true),
                                   SeekEntry(ls2[80000] + 1, ls2[80001], true),
                                   SeekEntry(ls2[99999], ls2[99999], true)};

  {
    VectorPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.push_back(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls1[j], p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianIteration_many_blocks_key=0 time:%dms mem_size:%u\n",
           t.since_start_nano() / 1000000, p.mem_size());
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls1[j], p.at());
      }
      p.reset_cursor();
    }
    printf(
        "prefixCompressedPostingsIteration_many_blocks_key=0 time:%dms "
        "mem_size:%u\n",
        t.since_start_nano() / 1000000, p.mem_size());
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls1[j], p.at());
      }
      p.reset_cursor();
    }
    printf(
        "prefixCompressedPostingsV2Iteration_many_blocks_key=0 time:%dms "
        "mem_size:%u\n",
        t.since_start_nano() / 1000000, p.mem_size());
  }

  {
    VectorPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.push_back(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table1.size(); j++) {
        ASSERT_EQ(table1[j].found, p.seek(table1[j].seek));
        ASSERT_EQ(table1[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianSeek_many_blocks_key=0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table1.size(); j++) {
        ASSERT_EQ(table1[j].found, p.seek(table1[j].seek));
        ASSERT_EQ(table1[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsSeek_many_blocks_key=0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls1.size(); i++) p.insert(ls1[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table1.size(); j++) {
        ASSERT_EQ(table1[j].found, p.seek(table1[j].seek));
        ASSERT_EQ(table1[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsV2Seek_many_blocks_key=0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }

  {
    VectorPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.push_back(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls2[j], p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianIteration_many_blocks_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls2[j], p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsIteration_many_blocks_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < num; j++) {
        ASSERT_TRUE(p.next());
        ASSERT_EQ(ls2[j], p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsV2Iteration_many_blocks_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }

  {
    VectorPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.push_back(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table2.size(); j++) {
        ASSERT_EQ(table2[j].found, p.seek(table2[j].seek));
        ASSERT_EQ(table2[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("bigEndianSeek_many_blocks_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostings p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table2.size(); j++) {
        ASSERT_EQ(table2[j].found, p.seek(table2[j].seek));
        ASSERT_EQ(table2[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsSeek_many_blocks_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
  {
    PrefixPostingsV2 p;
    for (size_t i = 0; i < ls2.size(); i++) p.insert(ls2[i]);
    Timer t;
    t.start();
    for (int i = 0; i < 100000; i++) {
      for (size_t j = 0; j < table2.size(); j++) {
        ASSERT_EQ(table2[j].found, p.seek(table2[j].seek));
        ASSERT_EQ(table2[j].val, p.at());
      }
      p.reset_cursor();
    }
    printf("prefixCompressedPostingsV2Seek_many_blocks_key>0 time:%dms\n",
           t.since_start_nano() / 1000000);
  }
}

}  // namespace index
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}