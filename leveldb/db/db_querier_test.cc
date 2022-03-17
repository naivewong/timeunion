#include "db/db_querier.h"

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/partition_index.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include <boost/filesystem.hpp>

#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "gtest/gtest.h"
#include "head/Head.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"

namespace leveldb {

class MemTest : public testing::Test {
 public:
  InternalKeyComparator cmp_;
  std::vector<MemTable*> mem_;
  SequenceNumber seq_;
  std::unique_ptr<::tsdb::head::MMapHeadWithTrie> head_;

  MemTest() : cmp_(BytewiseComparator()), mem_({new MemTable(cmp_)}), seq_(0) {
    mem_[0]->Ref();
  }

  ~MemTest() {
    for (MemTable* t : mem_) t->Unref();
  }

  void setup() {
    ::tsdb::head::MMapHeadWithTrie* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::MMapHeadWithTrie(0, "", ".", nullptr));
  }

  void setup(const std::string& log_dir) {
    ::tsdb::head::MMapHeadWithTrie* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::MMapHeadWithTrie(0, log_dir, ".", nullptr));
  }

  void _clean_files(const std::string& dir, const std::string& prefix) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "array", 5) == 0) ||
            (current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "ninfo", 5) == 0) ||
            (current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "block", 5) == 0) ||
            (current_file.size() - prefix.size() > 4 &&
             memcmp(current_file.c_str() + prefix.size(), "tail", 4) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_mmap_arrays(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 9 &&
             memcmp(current_file.c_str(), "8intarray", 9) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str(), "8gintarray", 10) == 0) ||
            (current_file.size() > 11 &&
             memcmp(current_file.c_str(), "8floatarray", 11) == 0) ||
            (current_file.size() > 12 &&
             memcmp(current_file.c_str(), "8gfloatarray", 12) == 0) ||
            (current_file.size() > 9 &&
             memcmp(current_file.c_str(), "8xorarray", 9) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_logs(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if (current_file.size() > tsdb::head::HEAD_SAMPLES_LOG_NAME.size() &&
            memcmp(current_file.c_str(),
                   tsdb::head::HEAD_SAMPLES_LOG_NAME.c_str(),
                   tsdb::head::HEAD_SAMPLES_LOG_NAME.size()) == 0) {
          to_remove.push_back(itr->path().string());
        } else if (current_file.size() >
                       tsdb::head::HEAD_FLUSHES_LOG_NAME.size() &&
                   memcmp(current_file.c_str(),
                          tsdb::head::HEAD_FLUSHES_LOG_NAME.c_str(),
                          tsdb::head::HEAD_FLUSHES_LOG_NAME.size()) == 0) {
          to_remove.push_back(itr->path().string());
        } else if (current_file == "head.log")
          to_remove.push_back(itr->path().string());
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
    boost::filesystem::remove(dir + "/mmap_arrays_safe");
    boost::filesystem::remove(dir + "/mmap_labels_safe");
  }

  void clean_mmap_labels(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 6 &&
             memcmp(current_file.c_str(), "labels", 6) == 0) ||
            (current_file.size() > 7 &&
             memcmp(current_file.c_str(), "glabels", 7) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_files() {
    _clean_files(".", "sym_");
    _clean_files(".", "ln_");
    _clean_files(".", "lv_");
    clean_mmap_arrays(".");
    clean_mmap_labels(".");
    clean_logs(".");
  }

  uint64_t add_index(const ::tsdb::label::Labels& lset) {
    auto app = head_->appender();
    auto r = app->add(lset, 0, 0);
    if (r.second) std::cout << "MemTest::add_index failed" << std::endl;
    return r.first;
  }

  void add_index(const ::tsdb::label::Labels& glset,
                 const std::vector<::tsdb::label::Labels>& ilsets,
                 uint64_t* gid, std::vector<int>* slots) {
    auto app = head_->appender();
    app->add(glset, ilsets, 0, std::vector<double>(ilsets.size(), 0), gid,
             slots);
  }

  void Add(const Slice& key, const Slice& value) {
    mem_[0]->Add(seq_++, kTypeValue, key, value);
  }

  void Add(int i, const Slice& key, const Slice& value) {
    mem_[i]->Add(seq_++, kTypeValue, key, value);
  }

  void add_mem() {
    mem_.push_back(new MemTable(cmp_));
    mem_.back()->Ref();
  }

  MemTable* mem() { return mem_[0]; }
  std::vector<MemTable*> mems() { return mem_; }
};

TEST_F(MemTest, Test1) {
  clean_files();
  setup(".");

  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
  std::set<std::string> syms;

  uint64_t id = 1;
  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
      syms.insert("label" + std::to_string(j));
      syms.insert("label" + std::to_string(j) + "_" + std::to_string(i));
    }
    ASSERT_EQ(id, add_index(lset));
    id++;
  }

  for (int t = 0; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(key, data);
    }
  }

  // MemSeriesIterator.
  {
    MemSeriesIterator it(mem(), 25 * 1000, 75 * 1000, 1);
    int i = 25, j = 0;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 75);
    ASSERT_EQ(j, 1);
  }

  {
    MemSeriesIterator it(mem(), 25 * 1000, 75 * 1000, 1);
    int i = 33, j = 2;
    ASSERT_TRUE(it.seek(33 * 1000 + 150));
    auto p = it.at();
    ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
    ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
    j++;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 75);
    ASSERT_EQ(j, 1);
  }

  {
    MemSeriesIterator it(mem(), -1, 101 * 1000, 1);
    int i = 0, j = 0;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 100);
    ASSERT_EQ(j, 0);
  }
}

TEST_F(MemTest, TestCache) {
  clean_files();
  setup(".");

  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;

  uint64_t id = 1;
  for (int i = 0; i < num_ts; i++) {
    ::tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
    }
    ASSERT_EQ(id, add_index(lset));
    id++;
  }

  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}}, &gid, &slots);
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1}), slots);

  for (int t = 0; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(key, data);
    }
  }

  for (int t = 0; t < num_tuples; t++) {
    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.c_str(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(key, data);
  }

  Cache* c = NewLRUCache(32 * 1024 * 1024);

  // MemSeriesIterator.
  {
    MemSeriesIterator it(mem(), 25 * 1000, 75 * 1000, 1, c);
    int i = 25, j = 0;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 75);
    ASSERT_EQ(j, 1);
    std::cout << "cache usage: " << c->GetUsage() << std::endl;
  }

  {
    MemSeriesIterator it(mem(), 25 * 1000, 75 * 1000, gid, 0, c);
    int i = 25, j = 0;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 75);
    ASSERT_EQ(j, 1);
    std::cout << "cache usage: " << c->GetUsage() << std::endl;
  }

  {
    MemSeriesIterator it(mem(), 25 * 1000, 75 * 1000, 1, c);
    int i = 33, j = 2;
    ASSERT_TRUE(it.seek(33 * 1000 + 150));
    auto p = it.at();
    ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
    ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
    j++;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 75);
    ASSERT_EQ(j, 1);
    std::cout << "cache usage: " << c->GetUsage() << std::endl;
  }

  {
    MemSeriesIterator it(mem(), 25 * 1000, 75 * 1000, gid, 0, c);
    int i = 33, j = 2;
    ASSERT_TRUE(it.seek(33 * 1000 + 150));
    auto p = it.at();
    ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
    ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
    j++;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 75);
    ASSERT_EQ(j, 1);
    std::cout << "cache usage: " << c->GetUsage() << std::endl;
  }

  {
    MemSeriesIterator it(mem(), -1, 101 * 1000, 1, c);
    int i = 0, j = 0;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 100);
    ASSERT_EQ(j, 0);
    std::cout << "cache usage: " << c->GetUsage() << std::endl;
  }

  {
    MemSeriesIterator it(mem(), -1, 101 * 1000, gid, 0, c);
    int i = 0, j = 0;
    while (it.next()) {
      auto p = it.at();
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
      ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
      j++;
      if (j == MEM_TUPLE_SIZE) {
        j = 0;
        i++;
      }
    }
    ASSERT_EQ(i, 100);
    ASSERT_EQ(j, 0);
    std::cout << "cache usage: " << c->GetUsage() << std::endl;
  }
}

TEST_F(MemTest, Test2) {
  clean_files();
  setup(".");

  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
  std::set<std::string> syms;

  uint64_t id = 1;
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
    ASSERT_EQ(id, add_index(lset));
    id++;
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1}), slots);

  for (int t = 0; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(key, data);
    }
  }

  for (int t = 0; t < num_tuples; t++) {
    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.c_str(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(key, data);
  }

  // MemSeriesSet.
  DBQuerier q(mem(), head_.get(), 25 * 1000, 75 * 1000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);
  ASSERT_FALSE(ss->error());

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(tsid | 0x8000000000000000, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
}

TEST_F(MemTest, Test3) {
  clean_files();
  setup(".");

  add_mem();
  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
  std::set<std::string> syms;

  uint64_t id = 1;
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
    ASSERT_EQ(id, add_index(lset));
    id++;
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}, {{"e", "f"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  syms.insert("e");
  syms.insert("f");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1, 2}), slots);

  for (int t = 0; t < num_tuples / 2; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(0, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100),
                   (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(0, key, data);
  }
  for (int t = num_tuples / 2; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(1, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(
        std::vector<int>({0, 1}));  // only the first two.
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(1, key, data);
  }

  // MemSeriesSet.
  DBQuerier q(mems()[1], mems()[0], head_.get(), 25 * 1000, 75 * 1000);
  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);
  ASSERT_FALSE(ss->error());

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(gid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"e", "f"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 50);
      ASSERT_EQ(j, 0);

      // Test seek.
      it = series->iterator();
      ASSERT_FALSE(it->seek(65 * 1000));

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
}

class DiskTest : public testing::Test {
 public:
  InternalKeyComparator cmp_;
  SeqAsceInternalKeyComparator asce_cmp_;
  std::vector<MemTable*> mem_;
  std::unique_ptr<::tsdb::head::MMapHeadWithTrie> head_;
  SequenceNumber seq_;
  uint64_t file_num_;

  DiskTest()
      : cmp_(BytewiseComparator()),
        asce_cmp_(BytewiseComparator()),
        mem_({new MemTable(cmp_)}),
        seq_(0),
        file_num_(1) {
    mem_[0]->Ref();
  }

  ~DiskTest() {
    for (MemTable* t : mem_) t->Unref();
  }

  void setup() {
    ::tsdb::head::MMapHeadWithTrie* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::MMapHeadWithTrie(0, "", ".", nullptr));
  }

  void setup(const std::string& log_dir) {
    ::tsdb::head::MMapHeadWithTrie* p = head_.release();
    delete p;
    head_.reset(new ::tsdb::head::MMapHeadWithTrie(0, log_dir, ".", nullptr));
  }

  void _clean_files(const std::string& dir, const std::string& prefix) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "array", 5) == 0) ||
            (current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "ninfo", 5) == 0) ||
            (current_file.size() - prefix.size() > 5 &&
             memcmp(current_file.c_str() + prefix.size(), "block", 5) == 0) ||
            (current_file.size() - prefix.size() > 4 &&
             memcmp(current_file.c_str() + prefix.size(), "tail", 4) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_mmap_arrays(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 9 &&
             memcmp(current_file.c_str(), "8intarray", 9) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str(), "8gintarray", 10) == 0) ||
            (current_file.size() > 11 &&
             memcmp(current_file.c_str(), "8floatarray", 11) == 0) ||
            (current_file.size() > 12 &&
             memcmp(current_file.c_str(), "8gfloatarray", 12) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_logs(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if (current_file.size() > tsdb::head::HEAD_SAMPLES_LOG_NAME.size() &&
            memcmp(current_file.c_str(),
                   tsdb::head::HEAD_SAMPLES_LOG_NAME.c_str(),
                   tsdb::head::HEAD_SAMPLES_LOG_NAME.size()) == 0) {
          to_remove.push_back(itr->path().string());
        } else if (current_file == "head.log")
          to_remove.push_back(itr->path().string());
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
    boost::filesystem::remove(dir + "/mmap_arrays_safe");
    boost::filesystem::remove(dir + "/mmap_labels_safe");
  }

  void clean_mmap_labels(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 6 &&
             memcmp(current_file.c_str(), "labels", 6) == 0) ||
            (current_file.size() > 7 &&
             memcmp(current_file.c_str(), "glabels", 7) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }

  void clean_files() {
    _clean_files(".", "sym_");
    _clean_files(".", "ln_");
    _clean_files(".", "lv_");
    clean_mmap_arrays(".");
    clean_mmap_labels(".");
    clean_logs(".");
  }

  uint64_t add_index(const ::tsdb::label::Labels& lset) {
    auto app = head_->appender();
    auto r = app->add(lset, 0, 0);
    if (r.second) std::cout << "DiskTest::add_index failed" << std::endl;
    return r.first;
  }

  void add_index(const ::tsdb::label::Labels& glset,
                 const std::vector<::tsdb::label::Labels>& ilsets,
                 uint64_t* gid, std::vector<int>* slots) {
    auto app = head_->appender();
    app->add(glset, ilsets, 0, std::vector<double>(ilsets.size(), 0), gid,
             slots);
  }

  void Add(const Slice& key, const Slice& value) {
    mem_[0]->Add(0, kTypeValue, key, value);
  }

  void Add(int i, const Slice& key, const Slice& value) {
    mem_[i]->Add(seq_++, kTypeValue, key, value);
  }

  void add_mem() {
    mem_.push_back(new MemTable(cmp_));
    mem_.back()->Ref();
  }

  MemTable* mem() { return mem_[0]; }
  std::vector<MemTable*> mems() { return mem_; }

  void flush_mem(int i, int64_t time_boundary, int64_t time_interval,
                 FileMetaData* meta) {
    Options opts;
    TableCache* tc = new TableCache(".", opts, 500);

    meta->number = file_num_++;
    meta->time_boundary = time_boundary;
    meta->time_interval = time_interval;
    Iterator* iter = mem_[i]->NewIterator();

    Status s = BuildTable(".", Env::Default(), opts, tc, iter, meta);
    if (!s.ok()) {
      std::cout << "Cannot build table: " << s.ToString() << std::endl;
      return;
    }

    delete tc;
  }
};

// Single partition, single SSTable.
TEST_F(DiskTest, L0Test1) {
  clean_files();
  setup(".");

  boost::filesystem::remove_all("0");
  boost::filesystem::remove("000001.ldb");
  boost::filesystem::create_directory("0");
  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
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
    add_index(lset);
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1}), slots);

  for (int t = 0; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(key, data);
  }

  FileMetaData* f = new FileMetaData();
  f->refs = 1;
  flush_mem(0, 0, num_tuples * 1000, f);
  Options opts;
  TableCache* tc = new TableCache(".", opts, 500);
  VersionSet vset(".", &opts, tc, &cmp_, &asce_cmp_);

  // Version* v = new Version();
  // v->Ref();
  vset.current()->TEST_add_file(0, f);
  vset.current()->TEST_add_time_partition(0, 0);
  vset.current()->TEST_add_time_partition_index(0, 0);

  DBQuerier q(
      head_.get(), 0, vset.current(),
      std::vector<std::pair<int64_t, int64_t>>({{0, num_tuples * 1000}}),
      std::vector<int>({0}), &cmp_, 25 * 1000, 75 * 1000);

  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);
  ASSERT_FALSE(ss->error());

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(gid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);

  delete tc;
}

// Single partition, two SSTables.
TEST_F(DiskTest, L0Test2) {
  clean_files();
  setup(".");

  boost::filesystem::remove_all("0");
  boost::filesystem::remove("000001.ldb");
  boost::filesystem::remove("000002.ldb");
  boost::filesystem::create_directory("0");
  add_mem();
  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
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
    add_index(lset);
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}, {{"e", "f"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  syms.insert("e");
  syms.insert("f");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1, 2}), slots);

  for (int t = 0; t < num_tuples / 2; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(0, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100),
                   (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(0, key, data);
  }
  for (int t = num_tuples / 2; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(1, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(std::vector<int>({0, 1}));
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(1, key, data);
  }

  FileMetaData* f = new FileMetaData();
  f->refs = 1;
  flush_mem(0, 0, num_tuples * 1000, f);
  Options opts;
  TableCache* tc = new TableCache(".", opts, 500);
  VersionSet vset(".", &opts, tc, &cmp_, &asce_cmp_);
  vset.current()->TEST_add_file(0, f);
  vset.current()->TEST_add_time_partition(0, 0);
  vset.current()->TEST_add_time_partition_index(0, 0);
  f = new FileMetaData();
  f->refs = 1;
  flush_mem(1, 0, num_tuples * 1000, f);
  vset.current()->TEST_add_file(0, f);

  DBQuerier q(
      head_.get(), 0, vset.current(),
      std::vector<std::pair<int64_t, int64_t>>({{0, num_tuples * 1000}}),
      std::vector<int>({0}), &cmp_, 25 * 1000, 75 * 1000);

  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);
  ASSERT_FALSE(ss->error());

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(gid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 75);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"e", "f"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 50);
      ASSERT_EQ(j, 0);

      // Test seek.
      it = series->iterator();
      ASSERT_FALSE(it->seek(65 * 1000));

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);

  delete tc;
}

// Two partitions, two SSTables (MergeSeriesSet).
TEST_F(DiskTest, L0Test3) {
  clean_files();
  setup(".");

  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
  boost::filesystem::remove_all("0");
  boost::filesystem::remove_all(std::to_string(num_tuples * 1000));
  boost::filesystem::remove("000001.ldb");
  boost::filesystem::remove("000002.ldb");
  boost::filesystem::remove("000003.ldb");
  boost::filesystem::remove("000004.ldb");
  boost::filesystem::create_directory("0");
  boost::filesystem::create_directory(std::to_string(num_tuples * 1000));
  add_mem();
  add_mem();
  add_mem();
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
    add_index(lset);
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}, {{"e", "f"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  syms.insert("e");
  syms.insert("f");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1, 2}), slots);

  for (int t = 0; t < num_tuples / 2; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(0, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(std::vector<int>({0, 1}));
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(0, key, data);
  }
  for (int t = num_tuples / 2; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(1, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100),
                   (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(1, key, data);
  }
  for (int t = num_tuples; t < 3 * num_tuples / 2; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(2, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(std::vector<int>({0, 1}));
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(2, key, data);
  }
  for (int t = 3 * num_tuples / 2; t < 2 * num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(3, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100),
                   (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string tmp;
    c.encode(&tmp);
    PutVarint32(&data, tmp.size());
    data.append(tmp.data(), tmp.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(3, key, data);
  }

  FileMetaData* f = new FileMetaData();
  f->refs = 1;
  flush_mem(0, 0, num_tuples * 1000, f);
  Options opts;
  TableCache* tc = new TableCache(".", opts, 500);
  VersionSet vset(".", &opts, tc, &cmp_, &asce_cmp_);
  vset.current()->TEST_add_file(0, f);
  f = new FileMetaData();
  f->refs = 1;
  flush_mem(1, 0, num_tuples * 1000, f);
  vset.current()->TEST_add_file(0, f);
  vset.current()->TEST_add_time_partition(0, 0);
  vset.current()->TEST_add_time_partition_index(0, 0);
  f = new FileMetaData();
  f->refs = 1;
  flush_mem(2, num_tuples * 1000, num_tuples * 1000, f);
  vset.current()->TEST_add_file(0, f);
  f = new FileMetaData();
  f->refs = 1;
  flush_mem(3, num_tuples * 1000, num_tuples * 1000, f);
  vset.current()->TEST_add_file(0, f);
  vset.current()->TEST_add_time_partition(0, num_tuples * 1000);
  vset.current()->TEST_add_time_partition_index(0, 2);

  DBQuerier q(
      head_.get(), 0, vset.current(),
      std::vector<std::pair<int64_t, int64_t>>(
          {{0, num_tuples * 1000}, {num_tuples * 1000, num_tuples * 1000}}),
      std::vector<int>({0, 2}), &cmp_, 25 * 1000, 125 * 1000);

  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(gid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"e", "f"}}),
                lset);
      it = series->iterator();
      i = 50, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 100);
      ASSERT_EQ(j, 0);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 100);
      ASSERT_EQ(j, 0);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);

  delete tc;
}

// Two partitions, one SSTable (MergeSeriesSet).
// Need to write sst ranges.
TEST_F(DiskTest, L1Test1) {
  clean_files();
  setup(".");

  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
  boost::filesystem::remove_all("0");
  boost::filesystem::remove_all(std::to_string(num_tuples * 1000));
  boost::filesystem::remove("000001.ldb");
  boost::filesystem::remove("000002.ldb");
  boost::filesystem::create_directory("0");
  boost::filesystem::create_directory(std::to_string(num_tuples * 1000));
  add_mem();
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
    add_index(lset);
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}, {{"e", "f"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  syms.insert("e");
  syms.insert("f");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1, 2}), slots);

  for (int t = 0; t < num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(0, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100),
                   (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string l;
    c.encode(&l);
    PutVarint32(&data, l.size());
    data.append(l.c_str(), l.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(0, key, data);
  }
  for (int t = num_tuples; t < 2 * num_tuples; t++) {
    for (int i = 0; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(1, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(std::vector<int>({0, 1}));
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string l;
    c.encode(&l);
    PutVarint32(&data, l.size());
    data.append(l.c_str(), l.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(1, key, data);
  }

  FileMetaData* f = new FileMetaData();
  f->refs = 1;
  flush_mem(0, 0, num_tuples * 1000, f);
  Options opts;
  TableCache* tc = new TableCache(".", opts, 500);
  VersionSet vset(".", &opts, tc, &cmp_, &asce_cmp_);
  vset.current()->TEST_add_file(1, f);
  vset.current()->TEST_add_time_partition(1, 0);
  vset.current()->TEST_add_time_partition_index(1, 0);
  f = new FileMetaData();
  f->refs = 1;
  flush_mem(1, num_tuples * 1000, num_tuples * 1000, f);
  vset.current()->TEST_add_file(1, f);
  vset.current()->TEST_add_time_partition(1, num_tuples * 1000);
  vset.current()->TEST_add_time_partition_index(1, 1);

  DBQuerier q(
      head_.get(), 1, vset.current(),
      std::vector<std::pair<int64_t, int64_t>>(
          {{0, num_tuples * 1000}, {num_tuples * 1000, num_tuples * 1000}}),
      std::vector<int>({0, 1}), &cmp_, 25 * 1000, 125 * 1000);

  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(gid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"e", "f"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 100);
      ASSERT_EQ(j, 0);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 100);
      ASSERT_EQ(j, 0);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);

  delete tc;
}

// Two partitions, one SSTable (MergeSeriesSet).
TEST_F(DiskTest, L2Test1) {
  clean_files();
  setup(".");

  int num_ts = 10;
  int num_labels = 10;
  int num_tuples = 100;
  boost::filesystem::remove("000001.ldb");  // TS 0-5.
  boost::filesystem::remove("000002.ldb");  // TS 6-11.
  boost::filesystem::remove("000003.ldb");  // TS 6-11 patch.
  boost::filesystem::remove("000004.ldb");
  boost::filesystem::remove("000005.ldb");
  boost::filesystem::remove("000006.ldb");
  add_mem();
  add_mem();
  add_mem();
  add_mem();
  add_mem();
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
    add_index(lset);
  }

  // add a group.
  std::vector<int> slots;
  uint64_t gid;
  add_index({{"group", "1"}, {"label_all", "value_all"}},
            {{{"a", "b"}}, {{"c", "d"}}, {{"e", "f"}}}, &gid, &slots);
  syms.insert("group");
  syms.insert("1");
  syms.insert("a");
  syms.insert("b");
  syms.insert("c");
  syms.insert("d");
  syms.insert("e");
  syms.insert("f");
  ASSERT_EQ((uint64_t)(num_ts + 1) | 0x8000000000000000, gid);
  ASSERT_EQ(std::vector<int>({0, 1, 2}), slots);

  for (int t = 0; t < num_tuples; t++) {
    for (int i = 0; i < num_ts / 2; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(0, key, data);
    }

    for (int i = num_ts / 2; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(1, key, data);
    }

    for (int i = num_ts / 2; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100 + 1);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(2, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(slots);
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100),
                   (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string l;
    c.encode(&l);
    PutVarint32(&data, l.size());
    data.append(l.c_str(), l.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(1, key, data);
  }
  for (int t = num_tuples; t < 2 * num_tuples; t++) {
    for (int i = 0; i < num_ts / 2; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(3, key, data);
    }

    for (int i = num_ts / 2; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(4, key, data);
    }

    for (int i = num_ts / 2; i < num_ts; i++) {
      std::string data;
      tsdb::chunk::XORChunk c;
      auto app = c.appender();
      for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
        app->append(t * 1000 + j * 100, t * 1000 + j * 100 + 1);
      // data.append(reinterpret_cast<const char*>(c.bytes() + 2), c.size() -
      // 2);
      PutVarint64(&data, 0);
      PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
      PutVarint32(&data, c.size());
      data.append(reinterpret_cast<const char*>(c.bytes()), c.size());

      std::string key;
      encodeKey(&key, "", i + 1, t * 1000);
      Add(5, key, data);
    }

    std::string data;
    tsdb::chunk::NullSupportedXORGroupChunk c(std::vector<int>({0, 1}));
    auto app = c.appender();
    for (size_t j = 0; j < MEM_TUPLE_SIZE; j++)
      app->append(t * 1000 + j * 100,
                  {(double)(t * 1000 + j * 100), (double)(t * 1000 + j * 100)});
    PutVarint64(&data, 0);
    PutVarint64(&data, (MEM_TUPLE_SIZE - 1) * 100);
    std::string l;
    c.encode(&l);
    PutVarint32(&data, l.size());
    data.append(l.c_str(), l.size());

    std::string key;
    encodeKey(&key, "", gid, t * 1000);
    Add(4, key, data);
  }

  FileMetaData* f = new FileMetaData();
  f->refs = 1;
  f->start_id = 1;
  f->end_id = num_ts / 2;
  flush_mem(0, 0, num_tuples * 1000, f);
  Options opts;
  TableCache* tc = new TableCache(".", opts, 500);
  VersionSet vset(".", &opts, tc, &cmp_, &asce_cmp_);
  vset.current()->TEST_add_file(2, f);
  vset.current()->TEST_add_time_partition(2, 0);
  vset.current()->TEST_add_time_partition_index(2, 0);

  f = new FileMetaData();
  f->refs = 1;
  f->start_id = num_ts / 2 + 1;
  f->end_id = gid;
  flush_mem(1, 0, num_tuples * 1000, f);
  vset.current()->TEST_add_file(2, f);

  f = new FileMetaData();
  f->refs = 1;
  f->start_id = num_ts / 2 + 1;
  f->end_id = gid;
  f->num_patches = 1;
  flush_mem(2, 0, num_tuples * 1000, f);
  vset.current()->TEST_add_file(2, f);

  f = new FileMetaData();
  f->refs = 1;
  f->start_id = 1;
  f->end_id = num_ts / 2;
  flush_mem(3, num_tuples * 1000, num_tuples * 1000, f);
  vset.current()->TEST_add_file(2, f);
  vset.current()->TEST_add_time_partition(2, num_tuples * 1000);
  vset.current()->TEST_add_time_partition_index(2, 3);

  f = new FileMetaData();
  f->refs = 1;
  f->start_id = num_ts / 2 + 1;
  f->end_id = gid;
  flush_mem(4, num_tuples * 1000, num_tuples * 1000, f);
  vset.current()->TEST_add_file(2, f);

  f = new FileMetaData();
  f->refs = 1;
  f->start_id = num_ts / 2 + 1;
  f->end_id = gid;
  f->num_patches = 1;
  flush_mem(5, num_tuples * 1000, num_tuples * 1000, f);
  vset.current()->TEST_add_file(2, f);

  DBQuerier q(
      head_.get(), 2, vset.current(),
      std::vector<std::pair<int64_t, int64_t>>(
          {{0, num_tuples * 1000}, {num_tuples * 1000, num_tuples * 1000}}),
      std::vector<int>({0, 3}), &cmp_, 25 * 1000, 125 * 1000);

  std::vector<::tsdb::label::MatcherInterface*> matchers(
      {new ::tsdb::label::EqualMatcher("label_all", "value_all")});
  auto ss = q.select(matchers);

  int tsid = 1;
  while (ss->next()) {
    std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();

    if (series->type() == tsdb::querier::kTypeSeries) {
      ASSERT_EQ(tsid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back(
            "label" + std::to_string(j),
            "label" + std::to_string(j) + "_" + std::to_string(tsid - 1));
      lset.emplace_back("label_all", "value_all");
      ASSERT_EQ(lset, series->labels());

      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100 + (tsid > num_ts / 2 ? 1 : 0)),
                  p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100 + (tsid > num_ts / 2 ? 1 : 0)),
                it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100 + (tsid > num_ts / 2 ? 1 : 0)),
                  p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);
    } else {
      ASSERT_EQ(gid, ss->current_tsid());
      ::tsdb::label::Labels lset;
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"a", "b"}}),
                lset);
      std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
          series->iterator();
      int i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"c", "d"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 125);
      ASSERT_EQ(j, 1);

      lset.clear();
      ASSERT_TRUE(series->next());
      series->labels(lset);
      ASSERT_EQ(tsdb::label::Labels(
                    {{"group", "1"}, {"label_all", "value_all"}, {"e", "f"}}),
                lset);
      it = series->iterator();
      i = 25, j = 0;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 100);
      ASSERT_EQ(j, 0);

      // Test seek.
      it = series->iterator();
      i = 65, j = 0;
      ASSERT_TRUE(it->seek(65 * 1000));
      ASSERT_EQ((int64_t)(i * 1000 + j * 100), it->at().first);
      ASSERT_EQ((double)(i * 1000 + j * 100), it->at().second);
      j++;
      while (it->next()) {
        auto p = it->at();
        ASSERT_EQ((int64_t)(i * 1000 + j * 100), p.first);
        ASSERT_EQ((double)(i * 1000 + j * 100), p.second);
        j++;
        if (j == MEM_TUPLE_SIZE) {
          j = 0;
          i++;
        }
      }
      ASSERT_EQ(i, 100);
      ASSERT_EQ(j, 0);

      ASSERT_FALSE(series->next());
    }

    tsid++;
  }
  ASSERT_EQ(num_ts + 2, tsid);
}

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // ::testing::GTEST_FLAG(filter) = "DiskTest.*";
  return RUN_ALL_TESTS();
}