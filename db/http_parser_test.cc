#include <snappy.h>

#include <iostream>

#include "db/DB.pb.h"
#include "db/HttpParser.hpp"
#include "util/testutil.h"

namespace tsdb {
namespace db {

class ParserTest : public testing::Test {};

TEST_F(ParserTest, TestProtoInsertSamples1) {
  InsertSamples samples;
  {
    InsertSample* tmp_sample = samples.add_samples();
    Tags* tmp_lset = tmp_sample->mutable_lset();
    NewTags(label::Labels({{"fuck", "CUHK"}, {"cuhk", "sucks"}}), tmp_lset);
    tmp_sample->set_t(8917);
    tmp_sample->set_v(0.4444);
  }
  {
    InsertSample* tmp_sample = samples.add_samples();
    Tags* tmp_lset = tmp_sample->mutable_lset();
    NewTags(label::Labels({{"label1", "l1"}, {"label2", "l2"}}), tmp_lset);
    tmp_sample->set_t(1625387863000);
    tmp_sample->set_v(0.3);
  }
  std::string data;
  samples.SerializeToString(&data);

  std::string compressed_data;
  snappy::Compress(data.data(), data.size(), &compressed_data);

  std::string decompressed_data;
  snappy::Uncompress(compressed_data.data(), compressed_data.size(),
                     &decompressed_data);

  InsertSamples parsed_samples;
  parsed_samples.ParseFromString(decompressed_data);
  ASSERT_EQ(2, parsed_samples.samples_size());
  {
    InsertSample tmp_sample = parsed_samples.samples(0);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"fuck", "CUHK"}, {"cuhk", "sucks"}}), lset));
    ASSERT_EQ((int64_t)(8917), tmp_sample.t());
    ASSERT_EQ(0.4444, tmp_sample.v());
  }
  {
    InsertSample tmp_sample = parsed_samples.samples(1);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(0.3, tmp_sample.v());
  }
}

TEST_F(ParserTest, TestProtoInsertSamples2) {
  InsertSamples samples;
  Add(&samples, {{"fuck", "CUHK"}, {"cuhk", "sucks"}}, 8917, 0.4444);
  Add(&samples, {{"label1", "l1"}, {"label2", "l2"}}, 1625387863000, 0.3);

  std::string data;
  samples.SerializeToString(&data);

  std::string compressed_data;
  snappy::Compress(data.data(), data.size(), &compressed_data);

  std::string decompressed_data;
  snappy::Uncompress(compressed_data.data(), compressed_data.size(),
                     &decompressed_data);

  InsertSamples parsed_samples;
  parsed_samples.ParseFromString(decompressed_data);
  ASSERT_EQ(2, parsed_samples.samples_size());
  {
    InsertSample tmp_sample = parsed_samples.samples(0);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"fuck", "CUHK"}, {"cuhk", "sucks"}}), lset));
    ASSERT_EQ((int64_t)(8917), tmp_sample.t());
    ASSERT_EQ(0.4444, tmp_sample.v());
  }
  {
    InsertSample tmp_sample = parsed_samples.samples(1);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(0.3, tmp_sample.v());
  }
}

TEST_F(ParserTest, TestProtoQuery1) {
  QueryRequest qr;
  qr.set_return_metric(true);
  Tags* tags = qr.mutable_lset();
  NewTags(label::Labels({{"label1", "l1"}, {"label2", "l2"}}), tags);
  qr.set_mint(0);
  qr.set_maxt(100);

  std::string data;
  qr.SerializeToString(&data);

  std::string compressed_data;
  snappy::Compress(data.data(), data.size(), &compressed_data);

  std::string decompressed_data;
  snappy::Uncompress(compressed_data.data(), compressed_data.size(),
                     &decompressed_data);

  QueryRequest qr2;
  qr2.ParseFromString(decompressed_data);

  label::Labels lset;
  NewTags(&lset, qr2.mutable_lset());
  ASSERT_EQ(0, label::lbs_compare(
                   label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
  ASSERT_TRUE(qr2.return_metric());
  ASSERT_EQ(0, qr2.mint());
  ASSERT_EQ(100, qr2.maxt());
}

TEST_F(ParserTest, TestProtoQuery2) {
  QueryRequest qr;
  Add(&qr, true, {{"label1", "l1"}, {"label2", "l2"}}, 0, 100);

  std::string data;
  qr.SerializeToString(&data);

  std::string compressed_data;
  snappy::Compress(data.data(), data.size(), &compressed_data);

  std::string decompressed_data;
  snappy::Uncompress(compressed_data.data(), compressed_data.size(),
                     &decompressed_data);

  QueryRequest qr2;
  qr2.ParseFromString(decompressed_data);

  label::Labels lset;
  NewTags(&lset, qr2.mutable_lset());
  ASSERT_EQ(0, label::lbs_compare(
                   label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
  ASSERT_TRUE(qr2.return_metric());
  ASSERT_EQ(0, qr2.mint());
  ASSERT_EQ(100, qr2.maxt());
}

TEST_F(ParserTest, TestProtoQueryResult1) {
  std::string data, compressed_data, decompressed_data;
  {
    QueryResults results;
    Add(&results, {{"label1", "l1"}, {"label2", "l2"}}, 0, {1, 2}, {10, 20});

    data.clear();
    compressed_data.clear();
    results.SerializeToString(&data);
    snappy::Compress(data.data(), data.size(), &compressed_data);
  }
  {
    QueryResults results;
    decompressed_data.clear();
    snappy::Uncompress(compressed_data.data(), compressed_data.size(),
                       &decompressed_data);
    results.ParseFromString(decompressed_data);
    ASSERT_EQ(1, results.results_size());
    {
      label::Labels lset;
      std::vector<int64_t> t;
      std::vector<double> v;
      uint64_t id;
      Parse(&results, 0, &lset, &id, &t, &v);
      ASSERT_EQ(0,
                label::lbs_compare(
                    label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
      ASSERT_EQ(0, id);
      ASSERT_EQ(std::vector<int64_t>({1, 2}), t);
      ASSERT_EQ(std::vector<double>({10, 20}), v);
    }
  }

  {
    QueryResults results;
    Add(&results, {}, 0, {1, 2}, {10, 20});

    data.clear();
    compressed_data.clear();
    results.SerializeToString(&data);
    snappy::Compress(data.data(), data.size(), &compressed_data);
  }
  {
    QueryResults results;
    decompressed_data.clear();
    snappy::Uncompress(compressed_data.data(), compressed_data.size(),
                       &decompressed_data);
    results.ParseFromString(decompressed_data);
    ASSERT_EQ(1, results.results_size());
    {
      label::Labels lset;
      std::vector<int64_t> t;
      std::vector<double> v;
      uint64_t id;
      Parse(&results, 0, &lset, &id, &t, &v);
      ASSERT_TRUE(lset.empty());
      ASSERT_EQ(0, id);
      ASSERT_EQ(std::vector<int64_t>({1, 2}), t);
      ASSERT_EQ(std::vector<double>({10, 20}), v);
    }
  }
}

}  // namespace db
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}