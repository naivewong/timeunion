#include "gtest/gtest.h"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

class MockIterator : public SeriesIteratorInterface {
 private:
  std::vector<int64_t> t_;
  std::vector<double> v_;
  mutable int idx_;

 public:
  MockIterator(const std::vector<int64_t>& t, const std::vector<double>& v)
      : t_(t), v_(v), idx_(-1) {}

  bool seek(int64_t t) const {
    if (idx_ == -1) idx_ = 0;
    for (; idx_ < t_.size(); idx_++) {
      if (t_[idx_] >= t) break;
    }
    return idx_ < t_.size();
  }

  std::pair<int64_t, double> at() const { return {t_[idx_], v_[idx_]}; }

  bool next() const {
    idx_++;
    if (idx_ >= t_.size()) return false;
    return true;
  }

  bool error() const { return false; }
};

class MergeIteratorTest : public testing::Test {};

TEST_F(MergeIteratorTest, Test1) {
  {
    MergeSeriesIterator it;
    it.push_back(new MockIterator({1, 3, 5, 7, 9}, {1.1, 1.1, 1.1, 1.1, 1.1}));
    it.push_back(new MockIterator({2, 4, 6, 8, 10}, {2.1, 2.1, 2.1, 2.1, 2.1}));
    it.push_back(new MockIterator({3, 4}, {3.1, 3.1}));
    it.push_back(new MockIterator({8, 9}, {4.1, 4.1}));

    std::vector<int64_t> t({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    std::vector<double> v({1.1, 2.1, 3.1, 3.1, 1.1, 2.1, 1.1, 4.1, 4.1, 2.1});
    int i = 0;
    while (it.next()) {
      ASSERT_EQ(t[i], it.at().first);
      ASSERT_EQ(v[i], it.at().second);
      i++;
    }
    ASSERT_EQ(10, i);
  }

  {
    MergeSeriesIterator it;
    it.push_back(new MockIterator({1, 3, 5, 7, 9}, {1.1, 1.1, 1.1, 1.1, 1.1}));
    it.push_back(new MockIterator({2, 4, 6, 8, 10}, {2.1, 2.1, 2.1, 2.1, 2.1}));
    it.push_back(new MockIterator({3, 4}, {3.1, 3.1}));
    it.push_back(new MockIterator({8, 9}, {4.1, 4.1}));

    ASSERT_TRUE(it.seek(5));
    std::vector<int64_t> t({5, 6, 7, 8, 9, 10});
    std::vector<double> v({1.1, 2.1, 1.1, 4.1, 4.1, 2.1});
    int i = 0;
    ASSERT_EQ(t[i], it.at().first);
    ASSERT_EQ(v[i], it.at().second);
    i++;
    while (it.next()) {
      ASSERT_EQ(t[i], it.at().first);
      ASSERT_EQ(v[i], it.at().second);
      i++;
    }
    ASSERT_EQ(6, i);
  }

  {
    MergeSeriesIterator it;
    it.push_back(new MockIterator({1, 3, 5, 7, 9}, {1.1, 1.1, 1.1, 1.1, 1.1}));
    it.push_back(new MockIterator({2, 4, 6, 8, 10}, {2.1, 2.1, 2.1, 2.1, 2.1}));
    it.push_back(new MockIterator({3, 4}, {3.1, 3.1}));
    it.push_back(new MockIterator({8, 9}, {4.1, 4.1}));

    ASSERT_TRUE(it.seek(5));
    ASSERT_TRUE(it.seek(8));
    std::vector<int64_t> t({8, 9, 10});
    std::vector<double> v({4.1, 4.1, 2.1});
    int i = 0;
    ASSERT_EQ(t[i], it.at().first);
    ASSERT_EQ(v[i], it.at().second);
    i++;
    while (it.next()) {
      ASSERT_EQ(t[i], it.at().first);
      ASSERT_EQ(v[i], it.at().second);
      i++;
    }
    ASSERT_EQ(3, i);
  }
}

}  // namespace querier
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}