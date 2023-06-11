#include "mem/mergeset_version_edit.h"

#include "db/version_edit.h"

#include "gtest/gtest.h"

namespace tsdb {
namespace mem {

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  leveldb::Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

TEST(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  for (int i = 0; i < 2; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(i, kBig + 300 + i, kBig + 400 + i,
                 leveldb::InternalKey("foo", kBig + 500 + i, leveldb::kTypeValue),
                 leveldb::InternalKey("zoo", kBig + 600 + i, leveldb::kTypeDeletion));
    edit.RemoveFile(i, kBig + 700 + i);
    edit.SetCompactPointer(i, leveldb::InternalKey("x", kBig + 900 + i, leveldb::kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

}
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}