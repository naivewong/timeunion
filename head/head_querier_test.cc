#include <boost/filesystem.hpp>

#include "gtest/gtest.h"
#include "head/MMapHeadWithTrie.hpp"
#include "head/HeadSeriesSet.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"

namespace tsdb {
namespace head {

class HeadTest : public testing::Test {
 public:
  std::unique_ptr<MMapHeadWithTrie> head_;
  std::unique_ptr<db::AppenderInterface> app_;

 public:
  void setup(uint64_t last_series_id, leveldb::DB* db) {
    std::string path = "/tmp/head_querier_test";
    boost::filesystem::remove_all(path);
    boost::filesystem::create_directory(path);
    head_.reset(new MMapHeadWithTrie(last_series_id, path, path, db));
    app_ = head_->appender();
  }

  std::pair<uint64_t, leveldb::Status> add(const ::tsdb::label::Labels& lset,
                                        int64_t t, double v) {
    auto p = app_->add(lset, t, v);
    app_->commit();
    return p;
  }

  leveldb::Status add_fast(uint64_t id, int64_t t, double v) {
    leveldb::Status e = app_->add_fast(id, t, v);
    app_->commit();
    return e;
  }
};

TEST_F(HeadTest, Test1) {
  setup(0, nullptr);
  int num_ts = 10;
  int num_labels = 10;
  int num_samples = 5;

  std::set<std::string> syms;
  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
      syms.insert("label" + std::to_string(j));
      syms.insert("label" + std::to_string(j) + "_" + std::to_string(i));
    }
    lset.emplace_back("label_all", "value_all");
    syms.insert("label_all");
    syms.insert("value_all");
    auto p = add(lset, 0, 0);
    ASSERT_TRUE(p.second.ok());
    ASSERT_EQ((uint64_t)(i + 1), p.first);
  }

  for (int i = 0; i < num_samples; i++) {
    for (int j = 0; j < num_ts; j++) {
      ASSERT_TRUE(add_fast(j + 1, i * 1000, i * 1000).ok());
    }
  }

  ASSERT_EQ(syms, head_->symbols());

  HeadQuerier q(head_.get(), 1000, 3000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss = q.select(matchers);
  uint64_t tsid = 1;
  while (ss->next()) {
    ASSERT_EQ(tsid, ss->current_tsid());
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back(
          "label" + std::to_string(j),
          "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
    lset.emplace_back("label_all", "value_all");
    ASSERT_EQ(lset, series->labels());

    std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
        series->iterator();
    int i = 1;
    while (it->next()) {
      auto p = it->at();
      ASSERT_EQ((int64_t)(i * 1000), p.first);
      ASSERT_EQ((double)(i * 1000), p.second);
      i++;
    }
    ASSERT_EQ(i, 4);

    tsid++;
  }
  ASSERT_EQ(num_ts + 1, tsid);
}

}  // namespace head.
}  // namespace tsdb.

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // ::testing::GTEST_FLAG(filter) = "DiskTest.*";
  return RUN_ALL_TESTS();
}