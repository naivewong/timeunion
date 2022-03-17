#include <cstdlib>
#include <vector>

#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "gtest/gtest.h"
#include "port/port_stdcxx.h"

namespace tsdb {
namespace chunk {

class NullSupportedXORChunkTest : public testing::Test {
 public:
  NullSupportedXORChunk* c;
};

TEST_F(NullSupportedXORChunkTest, Test1) {
  std::vector<int64_t> t;
  std::vector<double> v;

  c = new NullSupportedXORChunk();
  auto app = c->appender();
  for (int i = 0; i < 1000; i++) {
    t.push_back(i * 1000 + rand() % 100);
    v.push_back((double)(rand()) / (double)(RAND_MAX));
    app->append(t.back(), v.back());
  }

  {
    auto it = c->iterator();
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[i], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  delete c;
}

class NullSupportedXORGroupChunkTest : public testing::Test {
 public:
  NullSupportedXORGroupChunk* c;
};

TEST_F(NullSupportedXORGroupChunkTest, Test1) {
  std::vector<int64_t> t;
  std::vector<std::vector<double>> v;
  std::vector<int> seqs({1, 3, 4, 6, 19, 32, 33, 40});

  c = new NullSupportedXORGroupChunk(seqs);
  auto app = c->appender();
  for (int i = 0; i < 1000; i++) {
    t.push_back(i * 1000 + rand() % 100);
    std::vector<double> tmp;
    for (size_t j = 0; j < seqs.size(); j++)
      tmp.push_back((double)(rand()) / (double)(RAND_MAX));
    v.push_back(tmp);
    app->append(t.back(), v.back());
  }

  {
    auto it = c->iterator(19);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[i][4], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  std::string s;
  c->encode(&s);

  delete c;
  c = new NullSupportedXORGroupChunk(
      reinterpret_cast<const uint8_t*>(s.c_str()), s.size());

  {
    auto it = c->iterator(19);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[i][4], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  {
    auto it = c->iterator(1);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[i][0], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  {
    auto it = c->iterator(40);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[i][7], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  std::vector<int> result_slots;
  std::vector<int64_t> result_t;
  std::vector<std::vector<double>> result_v;
  c->decode(&result_slots, &result_t, &result_v);
  ASSERT_EQ(seqs, result_slots);
  ASSERT_EQ(t, result_t);
  ASSERT_EQ(result_v.size(), v[0].size());
  ASSERT_EQ(result_v[0].size(), v.size());
  for (size_t i = 0; i < result_v.size(); i++)
    for (size_t j = 0; j < result_v[i].size(); j++)
      ASSERT_EQ(v[j][i], result_v[i][j]);

  delete c;
}

TEST_F(NullSupportedXORGroupChunkTest, Test2) {
  std::vector<int64_t> t;
  std::vector<std::vector<double>> v;
  std::vector<int> seqs({1, 3, 4, 6, 19, 32, 33, 40});

  for (int i = 0; i < 1000; i++) {
    t.push_back(i * 1000 + rand() % 100);
  }
  for (size_t i = 0; i < seqs.size(); i++) {
    std::vector<double> tmp;
    for (size_t j = 0; j < 1000; j++)
      tmp.push_back((double)(rand()) / (double)(RAND_MAX));
    v.push_back(tmp);
  }

  std::string s;
  NullSupportedXORGroupChunk::encode(&s, seqs, t, v);
  c = new NullSupportedXORGroupChunk(
      reinterpret_cast<const uint8_t*>(s.c_str()), s.size());

  {
    auto it = c->iterator(19);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[4][i], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  {
    auto it = c->iterator(1);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[0][i], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }

  {
    auto it = c->iterator(40);
    int i = 0;
    while (it->next()) {
      ASSERT_EQ(t[i], it->at().first);
      ASSERT_EQ(v[7][i], it->at().second);
      i++;
    }
    ASSERT_EQ(1000, i);
  }
  delete c;
}

class ChunkBench : public testing::Test {};

TEST_F(ChunkBench, XORSize) {
  std::vector<XORChunk> chunks;
  chunks.reserve(100);
  size_t total_size = 0, tuple_size = 32;
  int64_t interval = 30, st = 0;
  while (tuple_size * chunks.size() * interval < 7200) {
    chunks.emplace_back();
    auto app = chunks.back().appender();
    for (int k = 0; k < tuple_size; k++)
      app->append(st + k * interval * 1000 + (rand() % 200), (rand() % 200));
    total_size += chunks.back().size();
  }

  std::string raw;
  for (auto& c : chunks)
    raw.append(reinterpret_cast<const char*>(c.bytes()), c.size());

  std::string compressed;
  leveldb::port::Snappy_Compress(raw.data(), raw.size(), &compressed);
  printf("#samples:%d span(s):%d #chunks:%d total_size:%d compressed_size:%d\n",
         tuple_size * chunks.size(), tuple_size * chunks.size() * interval,
         chunks.size(), total_size, compressed.size());
}

TEST_F(ChunkBench, GroupXORSize) {
  std::vector<NullSupportedXORGroupChunk> chunks;
  chunks.reserve(100);
  std::vector<int> slots;
  for (int i = 0; i < 101; i++) slots.push_back(i);
  size_t total_size = 0, tuple_size = 32;
  int64_t interval = 30, st = 0;
  while (tuple_size * chunks.size() * interval < 7200) {
    chunks.emplace_back(slots);
    std::vector<std::vector<double>> tfluc;
    for (int j = 0; j < tuple_size; j++) tfluc.emplace_back(101, rand() % 200);
    auto app = chunks.back().appender();
    for (int k = 0; k < tuple_size; k++)
      app->append(st + k * interval * 1000 + (int64_t)(tfluc[k][0]), tfluc[k]);
  }

  std::string raw;
  for (auto& c : chunks) c.encode(&raw);

  std::string compressed;
  leveldb::port::Snappy_Compress(raw.data(), raw.size(), &compressed);
  printf("#samples:%d span(s):%d #chunks:%d total_size:%d compressed_size:%d\n",
         tuple_size * chunks.size(), tuple_size * chunks.size() * interval,
         chunks.size(), raw.size(), compressed.size());
}

}  // namespace chunk
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}