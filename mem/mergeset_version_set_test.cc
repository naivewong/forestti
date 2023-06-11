// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "mem/mergeset_version_set.h"

#include "db/version_set.h"
#include "util/logging.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files);

}

namespace tsdb {
namespace mem {

class FindFileTest : public testing::Test {
 public:
  FindFileTest() : disjoint_sorted_files_(true) {}

  ~FindFileTest() {
    for (int i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           leveldb::SequenceNumber smallest_seq = 100,
           leveldb::SequenceNumber largest_seq = 100) {
    leveldb::FileMetaData* f = new leveldb::FileMetaData;
    f->number = files_.size() + 1;
    f->smallest = leveldb::InternalKey(smallest, smallest_seq, leveldb::kTypeValue);
    f->largest = leveldb::InternalKey(largest, largest_seq, leveldb::kTypeValue);
    files_.push_back(f);
  }

  int Find(const char* key) {
    leveldb::InternalKey target(key, 100, leveldb::kTypeValue);
    leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
    return FindFile(cmp, files_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
    leveldb::Slice s(smallest != nullptr ? smallest : "");
    leveldb::Slice l(largest != nullptr ? largest : "");
    return leveldb::SomeFileOverlapsRange(cmp, disjoint_sorted_files_, files_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }

  bool disjoint_sorted_files_;

 private:
  std::vector<leveldb::FileMetaData*> files_;
};

TEST_F(FindFileTest, Empty) {
  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(!Overlaps("a", "z"));
  ASSERT_TRUE(!Overlaps(nullptr, "z"));
  ASSERT_TRUE(!Overlaps("a", nullptr));
  ASSERT_TRUE(!Overlaps(nullptr, nullptr));
}

TEST_F(FindFileTest, Single) {
  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(!Overlaps("a", "b"));
  ASSERT_TRUE(!Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(!Overlaps(nullptr, "j"));
  ASSERT_TRUE(!Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST_F(FindFileTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("251", "299"));
  ASSERT_TRUE(!Overlaps("451", "500"));
  ASSERT_TRUE(!Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST_F(FindFileTest, MultipleNullBoundaries) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(!Overlaps(nullptr, "149"));
  ASSERT_TRUE(!Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST_F(FindFileTest, OverlapSequenceChecks) {
  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(!Overlaps("199", "199"));
  ASSERT_TRUE(!Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST_F(FindFileTest, OverlappingFiles) {
  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

class AddBoundaryInputsTest : public testing::Test {
 public:
  std::vector<leveldb::FileMetaData*> level_files_;
  std::vector<leveldb::FileMetaData*> compaction_files_;
  std::vector<leveldb::FileMetaData*> all_files_;
  leveldb::InternalKeyComparator icmp_;

  AddBoundaryInputsTest() : icmp_(leveldb::BytewiseComparator()) {}

  ~AddBoundaryInputsTest() {
    for (size_t i = 0; i < all_files_.size(); ++i) {
      delete all_files_[i];
    }
    all_files_.clear();
  }

  leveldb::FileMetaData* CreateFileMetaData(uint64_t number, leveldb::InternalKey smallest,
                                   leveldb::InternalKey largest) {
    leveldb::FileMetaData* f = new leveldb::FileMetaData();
    f->number = number;
    f->smallest = smallest;
    f->largest = largest;
    all_files_.push_back(f);
    return f;
  }
};

TEST_F(AddBoundaryInputsTest, TestEmptyFileSets) {
  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_TRUE(compaction_files_.empty());
  ASSERT_TRUE(level_files_.empty());
}

TEST_F(AddBoundaryInputsTest, TestEmptyLevelFiles) {
  leveldb::FileMetaData* f1 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 1, leveldb::kTypeValue)));
  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(1, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_TRUE(level_files_.empty());
}

TEST_F(AddBoundaryInputsTest, TestEmptyCompactionFiles) {
  leveldb::FileMetaData* f1 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 1, leveldb::kTypeValue)));
  level_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_TRUE(compaction_files_.empty());
  ASSERT_EQ(1, level_files_.size());
  ASSERT_EQ(f1, level_files_[0]);
}

TEST_F(AddBoundaryInputsTest, TestNoBoundaryFiles) {
  leveldb::FileMetaData* f1 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 1, leveldb::kTypeValue)));
  leveldb::FileMetaData* f2 =
      CreateFileMetaData(1, leveldb::InternalKey("200", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("200", 1, leveldb::kTypeValue)));
  leveldb::FileMetaData* f3 =
      CreateFileMetaData(1, leveldb::InternalKey("300", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("300", 1, leveldb::kTypeValue)));

  level_files_.push_back(f3);
  level_files_.push_back(f2);
  level_files_.push_back(f1);
  compaction_files_.push_back(f2);
  compaction_files_.push_back(f3);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(2, compaction_files_.size());
}

TEST_F(AddBoundaryInputsTest, TestOneBoundaryFiles) {
  leveldb::FileMetaData* f1 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 3, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 2, leveldb::kTypeValue)));
  leveldb::FileMetaData* f2 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 1, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("200", 3, leveldb::kTypeValue)));
  leveldb::FileMetaData* f3 =
      CreateFileMetaData(1, leveldb::InternalKey("300", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("300", 1, leveldb::kTypeValue)));

  level_files_.push_back(f3);
  level_files_.push_back(f2);
  level_files_.push_back(f1);
  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(2, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_EQ(f2, compaction_files_[1]);
}

TEST_F(AddBoundaryInputsTest, TestTwoBoundaryFiles) {
  leveldb::FileMetaData* f1 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 6, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 5, leveldb::kTypeValue)));
  leveldb::FileMetaData* f2 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("300", 1, leveldb::kTypeValue)));
  leveldb::FileMetaData* f3 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 4, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 3, leveldb::kTypeValue)));

  level_files_.push_back(f2);
  level_files_.push_back(f3);
  level_files_.push_back(f1);
  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(3, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_EQ(f3, compaction_files_[1]);
  ASSERT_EQ(f2, compaction_files_[2]);
}

TEST_F(AddBoundaryInputsTest, TestDisjoinFilePointers) {
  leveldb::FileMetaData* f1 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 6, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 5, leveldb::kTypeValue)));
  leveldb::FileMetaData* f2 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 6, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 5, leveldb::kTypeValue)));
  leveldb::FileMetaData* f3 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 2, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("300", 1, leveldb::kTypeValue)));
  leveldb::FileMetaData* f4 =
      CreateFileMetaData(1, leveldb::InternalKey("100", 4, leveldb::kTypeValue),
                         leveldb::InternalKey(leveldb::InternalKey("100", 3, leveldb::kTypeValue)));

  level_files_.push_back(f2);
  level_files_.push_back(f3);
  level_files_.push_back(f4);

  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(3, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_EQ(f4, compaction_files_[1]);
  ASSERT_EQ(f3, compaction_files_[2]);
}

}
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}