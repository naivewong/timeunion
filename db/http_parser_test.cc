#include <snappy.h>

#include <iostream>

#include "db/HttpParser.hpp"
#include "db/DB.pb.h"
#include "util/testutil.h"

namespace tsdb {
namespace db {

class ParserTest : public testing::Test {};

TEST_F(ParserTest, TestInsertParser1) {
  std::string data(
      "\
    {\
      \"samples\": [\
        {\
          \"type\":  1,\
          \"lset\":  {\"label1\":\"l1\", \"label2\":\"l2\"},\
          \"lsets\": [\
            {\"TS\":\"t1\"},\
            {\"TS\":\"t2\"}\
          ],\
          \"t\":     1625387863000,\
          \"v\":     [0.1, 0.2]\
        },\
        {\
          \"type\":  1,\
          \"id\":    100,\
          \"slots\":  [0, 1],\
          \"t\":     1625387863000,\
          \"v\":     [0.1, 0.2]\
        },\
        {\
          \"type\":  0,\
          \"lset\":  {\"label1\":\"l1\", \"label2\":\"l2\"},\
          \"t\":     1625387863000,\
          \"v\":     [0.3]\
        }\
      ]\
    }\
  ");
  // std::string
  // data("{\"type\":1,\"id\":100,\"lset\":{\"label1\":\"l1\",\"label2\":\"l2\"},\"lsets\":[{\"TS\":\"t1\"},{\"TS\":\"t2\"}],\"t\":1625387863000,\"v\":[0.1,0.2]}");

  InsertParser parser(data);

  ASSERT_TRUE(parser.next());
  ASSERT_EQ((InsertType)(kGroup), parser.type());
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                            parser.lset()));
  ASSERT_EQ(2, parser.lsets().size());
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"TS", "t1"}}), parser.lsets()[0]));
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"TS", "t2"}}), parser.lsets()[1]));
  ASSERT_EQ((int64_t)(1625387863000), parser.t());
  ASSERT_EQ(std::vector<double>({0.1, 0.2}), parser.v());

  ASSERT_TRUE(parser.next());
  ASSERT_EQ((InsertType)(kGroup), parser.type());
  ASSERT_EQ((uint64_t)(100), parser.id());
  ASSERT_EQ(std::vector<int>({0, 1}), parser.slots());
  ASSERT_EQ((int64_t)(1625387863000), parser.t());
  ASSERT_EQ(std::vector<double>({0.1, 0.2}), parser.v());

  ASSERT_TRUE(parser.next());
  ASSERT_EQ((InsertType)(kTS), parser.type());
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                            parser.lset()));
  ASSERT_EQ((int64_t)(1625387863000), parser.t());
  ASSERT_EQ(std::vector<double>({0.3}), parser.v());

  ASSERT_FALSE(parser.next());
}

TEST_F(ParserTest, TestInsertParser2) {
  // std::string data("\
  //   {\
  //     \"samples\": [\
  //       {\
  //         \"type\":  1,\
  //         \"lset\":  {\"label1\":\"l1\", \"label2\":\"l2\"},\
  //         \"lsets\": [\
  //           {\"TS\":\"t1\"},\
  //           {\"TS\":\"t2\"}\
  //         ],\
  //         \"t\":     1625387863000,\
  //         \"v\":     [0.1, 0.2]\
  //       },\
  //       {\
  //         \"type\":  1,\
  //         \"id\":    100,\
  //         \"t\":     1625387863000,\
  //         \"v\":     [0.1, 0.2]\
  //       },\
  //       {\
  //         \"type\":  0,\
  //         \"lset\":  {\"label1\":\"l1\", \"label2\":\"l2\"},\
  //         \"t\":     1625387863000,\
  //         \"v\":     [0.3]\
  //       }\
  //     ]\
  //   }\
  // ");
  InsertEncoder encoder;
  encoder.add({{"label1", "l1"}, {"label2", "l2"}},
              {{{"TS", "t1"}}, {{"TS", "t2"}}}, 1625387863000, {0.1, 0.2});
  encoder.add(100, 1625387863000, {0.1, 0.2});
  encoder.add({{"label1", "l1"}, {"label2", "l2"}}, 1625387863000, 0.3);
  encoder.close();

  std::cout << encoder.str() << std::endl;
  InsertParser parser(encoder.str());

  ASSERT_TRUE(parser.next());
  ASSERT_EQ((InsertType)(kGroup), parser.type());
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                            parser.lset()));
  ASSERT_EQ(2, parser.lsets().size());
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"TS", "t1"}}), parser.lsets()[0]));
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"TS", "t2"}}), parser.lsets()[1]));
  ASSERT_EQ((int64_t)(1625387863000), parser.t());
  ASSERT_EQ(std::vector<double>({0.1, 0.2}), parser.v());

  ASSERT_TRUE(parser.next());
  ASSERT_EQ((InsertType)(kGroup), parser.type());
  ASSERT_EQ((uint64_t)(100), parser.id());
  ASSERT_EQ((int64_t)(1625387863000), parser.t());
  ASSERT_EQ(std::vector<double>({0.1, 0.2}), parser.v());

  ASSERT_TRUE(parser.next());
  ASSERT_EQ((InsertType)(kTS), parser.type());
  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                            parser.lset()));
  ASSERT_EQ((int64_t)(1625387863000), parser.t());
  ASSERT_EQ(std::vector<double>({0.3}), parser.v());

  ASSERT_FALSE(parser.next());
}

TEST_F(ParserTest, TestInsertResult1) {
  {
    InsertResultEncoder encoder;
    encoder.add(0, {0, 1, 2, 3});
    encoder.add(1, {});
    encoder.add(2, {0, 1, 2, 3, 6});
    encoder.add(3, {});
    encoder.close();

    InsertResultParser parser(encoder.str());
    ASSERT_TRUE(parser.next());
    ASSERT_EQ(0, parser.id());
    ASSERT_EQ(std::vector<int>({0, 1, 2, 3}), parser.slots());

    ASSERT_TRUE(parser.next());
    ASSERT_EQ(1, parser.id());
    ASSERT_EQ(std::vector<int>(), parser.slots());

    ASSERT_TRUE(parser.next());
    ASSERT_EQ(2, parser.id());
    ASSERT_EQ(std::vector<int>({0, 1, 2, 3, 6}), parser.slots());

    ASSERT_TRUE(parser.next());
    ASSERT_EQ(3, parser.id());
    ASSERT_EQ(std::vector<int>(), parser.slots());

    ASSERT_FALSE(parser.next());
  }
}

TEST_F(ParserTest, TestQueryParser1) {
  std::string data(
      "\
    {\
      \"matchers\": {\"label1\":\"l1\", \"label2\":\"l2\"},\
      \"mint\":     1625387863000,\
      \"maxt\":     1625387963000\
    }\
  ");

  QueryParser parser(data);

  ASSERT_FALSE(parser.error());

  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                            parser.matchers()));
  ASSERT_EQ((int64_t)(1625387863000), parser.mint());
  ASSERT_EQ((int64_t)(1625387963000), parser.maxt());
}

TEST_F(ParserTest, TestQueryParser2) {
  QueryEncoder encoder(1, label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                       1625387863000, 1625387963000);

  QueryParser parser(encoder.str());

  ASSERT_FALSE(parser.error());

  ASSERT_EQ(
      0, label::lbs_compare(label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                            parser.matchers()));
  ASSERT_EQ((int64_t)(1625387863000), parser.mint());
  ASSERT_EQ((int64_t)(1625387963000), parser.maxt());
}

TEST_F(ParserTest, TestQueryResultEncoder1) {
  {
    QueryResultEncoder encoder;
    encoder.write({{"label1", "l1"}, {"label2", "l2"}}, 0, {1, 2}, {10, 20});
    encoder.close();
    std::cout << encoder.str() << std::endl;
  }
  {
    QueryResultEncoder encoder;
    encoder.write({{"label1", "l1"}, {"label2", "l2"}},
                  {{{"ts", "1"}}, {{"ts", "2"}}}, 0, {0, 1}, {{1, 2}, {3, 4}},
                  {{10, 20}, {30, 40}});
    encoder.close();
    std::cout << encoder.str() << std::endl;
  }
  {
    QueryResultEncoder encoder;
    encoder.write({{"label1", "l1"}, {"label2", "l2"}}, 0, {1, 2}, {10, 20});
    encoder.write({{"label1", "l1"}, {"label2", "l2"}},
                  {{{"ts", "1"}}, {{"ts", "2"}}}, 0, {0, 1}, {{1, 2}, {3, 4}},
                  {{10, 20}, {30, 40}});
    encoder.close();
    std::cout << encoder.str() << std::endl;
  }
}

TEST_F(ParserTest, TestQueryResultParser1) {
  {
    QueryResultEncoder encoder;
    encoder.write({{"label1", "l1"}, {"label2", "l2"}}, 0, {1, 2}, {10, 20});
    encoder.write({{"label1", "l1"}, {"label2", "l2"}},
                  {{{"ts", "1"}}, {{"ts", "2"}}}, 1, {0, 1}, {{1, 2}, {3, 4}},
                  {{10, 20}, {30, 40}});
    encoder.close();
    std::cout << encoder.str() << std::endl;

    QueryResultParser parser(encoder.str());
    ASSERT_TRUE(parser.next());
    ASSERT_EQ((uint64_t)(0), parser.id());
    ASSERT_EQ(0, label::lbs_compare(
                     label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                     parser.lset()));
    ASSERT_TRUE(parser.lsets().empty());
    ASSERT_EQ(1, parser.timestamps().size());
    ASSERT_EQ(1, parser.values().size());
    ASSERT_EQ(std::vector<int64_t>({1, 2}), parser.timestamps()[0]);
    ASSERT_EQ(std::vector<double>({10.0, 20.0}), parser.values()[0]);

    ASSERT_TRUE(parser.next());
    ASSERT_EQ((uint64_t)(1), parser.id());
    ASSERT_EQ(0, label::lbs_compare(
                     label::Labels({{"label1", "l1"}, {"label2", "l2"}}),
                     parser.lset()));
    ASSERT_EQ(2, parser.lsets().size());
    ASSERT_EQ(2, parser.timestamps().size());
    ASSERT_EQ(2, parser.values().size());
    ASSERT_EQ(
        0, label::lbs_compare(label::Labels({{"ts", "1"}}), parser.lsets()[0]));
    ASSERT_EQ(
        0, label::lbs_compare(label::Labels({{"ts", "2"}}), parser.lsets()[1]));
    ASSERT_EQ(std::vector<int64_t>({1, 2}), parser.timestamps()[0]);
    ASSERT_EQ(std::vector<int64_t>({3, 4}), parser.timestamps()[1]);
    ASSERT_EQ(std::vector<double>({10.0, 20.0}), parser.values()[0]);
    ASSERT_EQ(std::vector<double>({30.0, 40.0}), parser.values()[1]);

    ASSERT_FALSE(parser.next());
  }
}

TEST_F(ParserTest, TestProtoInsertSamples1) {
  InsertSamples samples;
  {
    InsertSample* tmp_sample = samples.add_samples();
    tmp_sample->set_type(InsertSample_InsertType_GROUP);
    Tags* tmp_lset = tmp_sample->mutable_lset();
    NewTags(label::Labels({{"label1", "l1"}, {"label2", "l2"}}), tmp_lset);
    tmp_lset = tmp_sample->add_lsets();
    NewTags(label::Labels({{"TS", "t1"}}), tmp_lset);
    tmp_lset = tmp_sample->add_lsets();
    NewTags(label::Labels({{"TS", "t2"}}), tmp_lset);
    tmp_sample->set_t(1625387863000);
    tmp_sample->add_v(0.1);
    tmp_sample->add_v(0.2);
  }
  {
    InsertSample* tmp_sample = samples.add_samples();
    tmp_sample->set_type(InsertSample_InsertType_GROUP);
    tmp_sample->set_id(100);
    tmp_sample->add_slots(0);
    tmp_sample->add_slots(1);
    tmp_sample->set_t(1625387863000);
    tmp_sample->add_v(0.1);
    tmp_sample->add_v(0.2);
  }
  {
    InsertSample* tmp_sample = samples.add_samples();
    tmp_sample->set_type(InsertSample_InsertType_TS);
    Tags* tmp_lset = tmp_sample->mutable_lset();
    NewTags(label::Labels({{"label1", "l1"}, {"label2", "l2"}}), tmp_lset);
    tmp_sample->set_t(1625387863000);
    tmp_sample->add_v(0.3);
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
  ASSERT_EQ(3, parsed_samples.samples_size());
  {
    InsertSample tmp_sample = parsed_samples.samples(0);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
    ASSERT_EQ(2, tmp_sample.lsets_size());
    lset.clear();
    NewTags(&lset, tmp_sample.mutable_lsets(0));
    ASSERT_EQ(0, label::lbs_compare(label::Labels({{"TS", "t1"}}), lset));
    lset.clear();
    NewTags(&lset, tmp_sample.mutable_lsets(1));
    ASSERT_EQ(0, label::lbs_compare(label::Labels({{"TS", "t2"}}), lset));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(2, tmp_sample.v_size());
    ASSERT_EQ(0.1, tmp_sample.v(0));
    ASSERT_EQ(0.2, tmp_sample.v(1));
  }
  {
    InsertSample tmp_sample = parsed_samples.samples(1);
    ASSERT_EQ(2, tmp_sample.slots_size());
    ASSERT_EQ(0, tmp_sample.slots(0));
    ASSERT_EQ(1, tmp_sample.slots(1));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(2, tmp_sample.v_size());
    ASSERT_EQ(0.1, tmp_sample.v(0));
    ASSERT_EQ(0.2, tmp_sample.v(1));
  }
  {
    InsertSample tmp_sample = parsed_samples.samples(2);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(1, tmp_sample.v_size());
    ASSERT_EQ(0.3, tmp_sample.v(0));
  }
}

TEST_F(ParserTest, TestProtoInsertSamples2) {
  InsertSamples samples;
  Add(&samples, {{"label1", "l1"}, {"label2", "l2"}},
      {{{"TS", "t1"}}, {{"TS", "t2"}}}, 1625387863000, {0.1, 0.2});
  Add(&samples, 100, {0, 1}, 1625387863000, {0.1, 0.2});
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
  ASSERT_EQ(3, parsed_samples.samples_size());
  {
    InsertSample tmp_sample = parsed_samples.samples(0);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
    ASSERT_EQ(2, tmp_sample.lsets_size());
    lset.clear();
    NewTags(&lset, tmp_sample.mutable_lsets(0));
    ASSERT_EQ(0, label::lbs_compare(label::Labels({{"TS", "t1"}}), lset));
    lset.clear();
    NewTags(&lset, tmp_sample.mutable_lsets(1));
    ASSERT_EQ(0, label::lbs_compare(label::Labels({{"TS", "t2"}}), lset));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(2, tmp_sample.v_size());
    ASSERT_EQ(0.1, tmp_sample.v(0));
    ASSERT_EQ(0.2, tmp_sample.v(1));
  }
  {
    InsertSample tmp_sample = parsed_samples.samples(1);
    ASSERT_EQ(2, tmp_sample.slots_size());
    ASSERT_EQ(0, tmp_sample.slots(0));
    ASSERT_EQ(1, tmp_sample.slots(1));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(2, tmp_sample.v_size());
    ASSERT_EQ(0.1, tmp_sample.v(0));
    ASSERT_EQ(0.2, tmp_sample.v(1));
  }
  {
    InsertSample tmp_sample = parsed_samples.samples(2);
    label::Labels lset;
    NewTags(&lset, tmp_sample.mutable_lset());
    ASSERT_EQ(0,
              label::lbs_compare(
                  label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
    ASSERT_EQ((int64_t)(1625387863000), tmp_sample.t());
    ASSERT_EQ(1, tmp_sample.v_size());
    ASSERT_EQ(0.3, tmp_sample.v(0));
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
    Add(&results, {{"label1", "l1"}, {"label2", "l2"}},
        {{{"ts", "1"}}, {{"ts", "2"}}}, 1, {{1, 2}, {3, 4}},
        {{10, 20}, {30, 40}});

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
    ASSERT_EQ(2, results.results_size());
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
    {
      label::Labels lset;
      std::vector<label::Labels> lsets;
      std::vector<std::vector<int64_t>> t;
      std::vector<std::vector<double>> v;
      uint64_t id;
      Parse(&results, 1, &lset, &lsets, &id, &t, &v);
      ASSERT_EQ(0,
                label::lbs_compare(
                    label::Labels({{"label1", "l1"}, {"label2", "l2"}}), lset));
      ASSERT_EQ(2, lsets.size());
      ASSERT_EQ(0, label::lbs_compare(label::Labels({{"ts", "1"}}), lsets[0]));
      ASSERT_EQ(0, label::lbs_compare(label::Labels({{"ts", "2"}}), lsets[1]));
      ASSERT_EQ(1, id);
      ASSERT_EQ(std::vector<std::vector<int64_t>>({{1, 2}, {3, 4}}), t);
      ASSERT_EQ(std::vector<std::vector<double>>({{10, 20}, {30, 40}}), v);
    }
  }

  {
    QueryResults results;
    Add(&results, {}, 0, {1, 2}, {10, 20});
    Add(&results, {}, {}, 1, {{1, 2}, {3, 4}}, {{10, 20}, {30, 40}});

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
    ASSERT_EQ(2, results.results_size());
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
    {
      label::Labels lset;
      std::vector<label::Labels> lsets;
      std::vector<std::vector<int64_t>> t;
      std::vector<std::vector<double>> v;
      uint64_t id;
      Parse(&results, 1, &lset, &lsets, &id, &t, &v);
      ASSERT_TRUE(lset.empty());
      ASSERT_TRUE(lsets.empty());
      ASSERT_EQ(1, id);
      ASSERT_EQ(std::vector<std::vector<int64_t>>({{1, 2}, {3, 4}}), t);
      ASSERT_EQ(std::vector<std::vector<double>>({{10, 20}, {30, 40}}), v);
    }
  }
}

}  // namespace db
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}