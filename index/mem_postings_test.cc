#include "gtest/gtest.h"
#include "index/MemPostings.hpp"
#include "label/Label.hpp"

class MemPostingsTest : public testing::Test {
 public:
  tsdb::index::MemPostingsWithTrie postings_with_trie_;
};

TEST_F(MemPostingsTest, Test1) {
  int num_ts = 10000;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < 10; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));
    postings_with_trie_.add(i, lset);
  }

  for (int i = 0; i < num_ts; i++) {
    for (int j = 0; j < 10; j++) {
      auto p = postings_with_trie_.get("label_" + std::to_string(j),
                                       "value_" + std::to_string(i));
      int num = 0;
      while (p->next()) {
        ASSERT_EQ(i, p->at());
        num++;
      }
      ASSERT_EQ(1, num);
    }
  }

  auto p = postings_with_trie_.get(tsdb::label::ALL_POSTINGS_KEYS.label,
                                   tsdb::label::ALL_POSTINGS_KEYS.value);
  int num = 0;
  while (p->next()) {
    ASSERT_EQ(num, p->at());
    num++;
  }
  ASSERT_EQ(num_ts, num);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}