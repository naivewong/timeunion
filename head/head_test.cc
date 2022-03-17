#include "head/Head.hpp"

#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <vector>

#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "gtest/gtest.h"
#include "head/HeadAppender.hpp"
#include "head/HeadSeriesSet.hpp"
#include "head/HeadWithTrie.hpp"
#include "head/MMapHeadWithTrie.hpp"
#include "label/EqualMatcher.hpp"
#include "leveldb/db/log_reader.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/third_party/thread_pool.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"

namespace tsdb {
namespace head {

void mem_usage(double& vm_usage, double& resident_set) {
  vm_usage = 0.0;
  resident_set = 0.0;
  std::ifstream stat_stream("/proc/self/stat",
                            std::ios_base::in);  // get info from proc directory
  // create some variables to get info
  std::string pid, comm, state, ppid, pgrp, session, tty_nr;
  std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  std::string utime, stime, cutime, cstime, priority, nice;
  std::string O, itrealvalue, starttime;
  unsigned long vsize;
  long rss;
  stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
      tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
      stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
      starttime >> vsize >> rss;  // don't care about the rest
  stat_stream.close();
  long page_size_kb = sysconf(_SC_PAGE_SIZE) /
                      1024;  // for x86-64 is configured to use 2MB pages
  vm_usage = vsize / 1024.0;
  resident_set = rss * page_size_kb;
}

std::vector<label::Labels> read_labels(int num, const std::string& name) {
  std::vector<label::Labels> lsets;
  std::ifstream file(name);
  std::string line;
  while (getline(file, line) && lsets.size() < num) {
    rapidjson::Document d;
    d.Parse(line.c_str());
    label::Labels lset;
    for (auto& m : d.GetObject())
      lset.emplace_back(m.name.GetString(), m.value.GetString());
    std::sort(lset.begin(), lset.end());
    lsets.push_back(lset);
  }
  return lsets;
}

class MMapTest : public testing::Test {};

TEST_F(MMapTest, Test1) {
  MMapXORChunk::remove_all(16, ".");

  MMapXORChunk mxc(".", 16, 10);
  for (int i = 0; i < 40; i++) {
    auto p = mxc.alloc_slot(i);
    {
      ::tsdb::chunk::XORChunkV2 c(p.second);
      auto app = c.appender();
      for (int j = 0; j < 11; j++) app->append(j * 100, j * 110);

      ::tsdb::chunk::XORIteratorV2 it(p.second, 256);
      int count = 0;
      while (it.next()) {
        ASSERT_EQ(count * 100, it.at().first);
        ASSERT_EQ(count * 110, it.at().second);
        count++;
      }
      ASSERT_EQ(11, count);
    }
    {
      ::tsdb::chunk::BitStreamV2 c(p.second, 2);
      ::tsdb::chunk::XORAppenderV2 app(c, 0, 0, 0, 0xff, 0);
      for (int j = 0; j < 11; j++) app.append(j * 200, j * 210);

      ::tsdb::chunk::XORIteratorV2 it(p.second, 256);
      int count = 0;
      while (it.next()) {
        ASSERT_EQ(count * 200, it.at().first);
        ASSERT_EQ(count * 210, it.at().second);
        count++;
      }
      ASSERT_EQ(11, count);
    }
  }
}

TEST_F(MMapTest, Test2) {
  MMapGroupXORChunk::remove_all(8, ".", "group");

  MMapGroupXORChunk mxc(".", 8, 10, "group");
  for (int i = 0; i < 40; i++) {
    auto p1 = mxc.alloc_slot(i, std::numeric_limits<uint32_t>::max());
    auto p2 = mxc.alloc_slot(i, 0);
    auto p3 = mxc.alloc_slot(i, 1);

    {
      ::tsdb::chunk::NullSupportedXORGroupChunkV2 c({0, 1}, p1.second,
                                                    {p2.second, p3.second});
      auto app = c.appender();
      for (int j = 0; j < 11; j++) app->append(j * 100, {j * 110, j * 110});

      auto it = c.iterator(0);
      int count = 0;
      while (it->next()) {
        ASSERT_EQ(count * 100, it->at().first);
        ASSERT_EQ(count * 110, it->at().second);
        count++;
      }
      ASSERT_EQ(11, count);

      it = c.iterator(1);
      count = 0;
      while (it->next()) {
        ASSERT_EQ(count * 100, it->at().first);
        ASSERT_EQ(count * 110, it->at().second);
        count++;
      }
      ASSERT_EQ(11, count);
    }
    {
      ::tsdb::chunk::BitStreamV2 b1(p1.second, 2);
      ::tsdb::chunk::NullSupportedXORGroupTimeAppenderV2 app1(&b1);
      for (int j = 0; j < 11; j++) app1.append(j * 200);

      ::tsdb::chunk::BitStreamV2 b2(p2.second);
      ::tsdb::chunk::NullSupportedXORGroupValueAppenderV2 app2(&b2);
      for (int j = 0; j < 11; j++) app2.append(j * 210);

      ::tsdb::chunk::BitStreamV2 b3(p3.second);
      ::tsdb::chunk::NullSupportedXORGroupValueAppenderV2 app3(&b3);
      for (int j = 0; j < 11; j++) app3.append(j * 310);

      ::tsdb::chunk::NullSupportedXORGroupIteratorV2 it1(b1, b2);
      int count = 0;
      while (it1.next()) {
        ASSERT_EQ(count * 200, it1.at().first);
        ASSERT_EQ(count * 210, it1.at().second);
        count++;
      }
      ASSERT_EQ(11, count);

      ::tsdb::chunk::NullSupportedXORGroupIteratorV2 it2(b1, b3);
      count = 0;
      while (it2.next()) {
        ASSERT_EQ(count * 200, it2.at().first);
        ASSERT_EQ(count * 310, it2.at().second);
        count++;
      }
      ASSERT_EQ(11, count);
    }
  }
}

TEST_F(MMapTest, Test3) {
  MMapGroupXORChunk::remove_all(8, ".", "group");

  MMapGroupXORChunk mxc(".", 8, 10, "group");
  auto p1 = mxc.alloc_slot(1, 0);
  std::vector<double> vals;
  {
    ::tsdb::chunk::BitStreamV2 b1(p1.second);
    ::tsdb::chunk::NullSupportedXORGroupValueAppenderV2 app1(&b1);
    app1.append(1.1);
    app1.append(std::numeric_limits<double>::max());
    app1.append(2.2);
    app1.append(3.2);
    chunk::NullSupportedXORGroupValueIterator it(p1.second, b1.size(), 4);
    while (it.next()) {
      std::cout << it.at() << " ";
      vals.push_back(it.at());
    }
    std::cout << std::endl;
  }

  vals[2] = 3.3;

  {
    ::tsdb::chunk::BitStreamV2 b1(p1.second);
    ::tsdb::chunk::NullSupportedXORGroupValueAppenderV2 app1(&b1);
    for (auto v : vals) app1.append(v);
    chunk::NullSupportedXORGroupValueIterator it(p1.second, b1.size(), 4);
    while (it.next()) {
      std::cout << it.at() << " ";
    }
    std::cout << std::endl;
  }
}

class HeadWithTrieTest : public testing::Test {
 public:
  std::unique_ptr<HeadWithTrie> head;

  void setup() {
    HeadWithTrie* p = head.release();
    delete p;
    head.reset(new HeadWithTrie(0, "", ".", nullptr));
  }

  void setup(const std::string& log_dir) {
    HeadWithTrie* p = head.release();
    delete p;
    head.reset(new HeadWithTrie(0, log_dir, ".", nullptr));
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

  void clean_logs(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if (current_file.size() > HEAD_SAMPLES_LOG_NAME.size() &&
            memcmp(current_file.c_str(), HEAD_SAMPLES_LOG_NAME.c_str(),
                   HEAD_SAMPLES_LOG_NAME.size()) == 0) {
          to_remove.push_back(itr->path().string());
        } else if (current_file == "head.log")
          to_remove.push_back(itr->path().string());
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
    clean_logs(".");
  }
};

TEST_F(HeadWithTrieTest, Test1) {
  clean_files();
  setup();

  auto app = head->appender();
  int num_ts = 10000;
  int num_labels = 10;
  std::set<std::string> syms;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));
      syms.insert("label_" + std::to_string(j));
      syms.insert("value_" + std::to_string(i));
    }
    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
  }

  // Symbols.
  ASSERT_EQ(syms, head->symbols());

  // Label names.
  std::vector<std::string> lnames;
  for (int j = 0; j < num_labels; j++)
    lnames.push_back("label_" + std::to_string(j));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, head->label_names());

  // Label values.
  std::vector<std::string> lvalues;
  for (int i = 0; i < num_ts; i++)
    lvalues.push_back("value_" + std::to_string(i));
  std::sort(lvalues.begin(), lvalues.end());
  for (int i = 0; i < num_labels; i++)
    ASSERT_EQ(lvalues, head->label_values("label_" + std::to_string(i)));

  // Postings.
  for (int i = 0; i < num_ts; i++) {
    for (int j = 0; j < num_labels; j++) {
      auto p = head->postings("label_" + std::to_string(j),
                              "value_" + std::to_string(i));
      ASSERT_TRUE(p.second);
      int num = 0;
      while (p.first->next()) {
        ASSERT_EQ(i + 1, p.first->at());
        num++;
      }
      ASSERT_EQ(1, num);
    }
  }
  auto p = head->postings(label::ALL_POSTINGS_KEYS.label,
                          label::ALL_POSTINGS_KEYS.value);
  ASSERT_TRUE(p.second);
  int num = 1;
  while (p.first->next()) {
    ASSERT_EQ(num, p.first->at());
    num++;
  }
  ASSERT_EQ(num_ts + 1, num);
}

TEST_F(HeadWithTrieTest, TestSize1) {
  clean_files();
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  HeadWithTrie head(0, "", ".", nullptr);
  {
    std::vector<label::Labels> lsets =
        read_labels(20000, "../test/timeseries.json");
    auto app = head.appender();
    for (size_t i = 0; i < lsets.size(); i++) app->add(lsets[i], 0, 0);
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
  std::cout << "#Tags: " << head.num_tags() << std::endl;
}

TEST_F(HeadWithTrieTest, TestSize2) {
  clean_files();
  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Head head(0, "", nullptr);
  {
    std::vector<label::Labels> lsets =
        read_labels(20000, "../test/timeseries.json");
    auto app = head.appender();
    for (size_t i = 0; i < lsets.size(); i++) app->add(lsets[i], 0, 0);
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(HeadWithTrieTest, TestSize3) {
  clean_files();
  int num_ts = 100000;
  int num_labels = 10;

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  HeadWithTrie head(0, "", ".", nullptr);
  {
    auto app = head.appender();
    for (int i = 0; i < num_ts; i++) {
      label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label" + std::to_string(j),
                          "value" + std::to_string(i));
      app->add(lset, 0, 0);
    }
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
  std::cout << "#Tags: " << head.num_tags() << std::endl;
}

TEST_F(HeadWithTrieTest, TestSize4) {
  clean_files();
  int num_ts = 100000;
  int num_labels = 10;

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Head head(0, "", nullptr);
  {
    auto app = head.appender();
    for (int i = 0; i < num_ts; i++) {
      label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label" + std::to_string(j),
                          "value" + std::to_string(i));
      app->add(lset, 0, 0);
    }
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(HeadWithTrieTest, TestSize5) {
  clean_files();
  int num_ts = 100000;
  int num_labels = 10;

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  HeadWithTrie head(0, "", ".", nullptr);
  {
    auto app = head.appender();
    for (int i = 0; i < num_ts; i++) {
      label::Labels lset;
      for (int j = 0; j < num_labels; j++)
        lset.emplace_back("label" + std::to_string(j),
                          "value" + std::to_string(i));
      app->add(lset, 0, 0);
      app->add(lset, 1001, 99.3);
      app->add(lset, 2002, 99.4);
      app->add(lset, 3000, 99.5);
      app->add(lset, 4007, 99.6);
      app->add(lset, 5009, 99.37);
      app->add(lset, 6004, 99.8);
    }
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(HeadWithTrieTest, TestSamplesLog1) {
  clean_files();
  setup(".");

  auto app = head->TEST_appender();
  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 12;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    std::vector<int64_t> tmp_times;
    std::vector<double> tmp_values;
    tmp_times.push_back(0);
    tmp_values.push_back(0);
    for (int j = 0; j < num_samples; j++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      tmp_times.push_back(t);
      tmp_values.push_back(v);
      ASSERT_TRUE(app->add_fast(i + 1, t, v).ok());
    }
    times.push_back(tmp_times);
    values.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[i][count + 8], it.at().first);
      ASSERT_EQ(values[i][count + 8], it.at().second);
      count++;
    }
    ASSERT_EQ((num_samples + 1) % 8, count);
  }

  setup(".");
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[i][count + 8], it.at().first);
      ASSERT_EQ(values[i][count + 8], it.at().second);
      count++;
    }
    ASSERT_EQ((num_samples + 1) % 8, count);
  }
}

TEST_F(HeadWithTrieTest, TestSamplesLog2) {
  clean_files();
  setup(".");

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  auto app = head->TEST_appender();
  int num_ts = 100000;
  int num_labels = 10;
  int num_samples = 1000;
  // std::vector<std::vector<int64_t>> times;
  // std::vector<std::vector<double>> values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    // std::vector<int64_t> tmp_times;
    // std::vector<double> tmp_values;
    // tmp_times.push_back(0);
    // tmp_values.push_back(0);
    for (int j = 0; j < num_samples; j++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      // tmp_times.push_back(t);
      // tmp_values.push_back(v);
      ASSERT_TRUE(app->add_fast(i + 1, t, v).ok());
    }
    // times.push_back(tmp_times);
    // values.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }
  uint64_t d = timer.since_start_nano();
  std::cout << "[Insertion duration (nanos)]:" << d << " [throughput]:"
            << ((double)(num_ts * num_samples) / (double)(d)*1000000000)
            << std::endl;
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;

  // for (int i = 0; i < num_ts; i++) {
  //   tsdb::label::Labels lset;
  //   for (int j = 0; j < num_labels; j++)
  //     lset.emplace_back("label_" + std::to_string(j), "value_" +
  //     std::to_string(i));

  //   tsdb::label::Labels result_lset;
  //   std::string content;
  //   head->series(i + 1, result_lset, &content);
  //   ASSERT_EQ(lset, result_lset);
  //   HeadIterator it(&content, 0, 10000000);
  //   int count = 0;
  //   while (it.next()) {
  //     ASSERT_EQ(times[i][count + 8], it.at().first);
  //     ASSERT_EQ(values[i][count + 8], it.at().second);
  //     count++;
  //   }
  //   ASSERT_EQ((num_samples + 1) % 8, count);
  // }

  setup(".");
  // for (int i = 0; i < num_ts; i++) {
  //   tsdb::label::Labels lset;
  //   for (int j = 0; j < num_labels; j++)
  //     lset.emplace_back("label_" + std::to_string(j), "value_" +
  //     std::to_string(i));

  //   tsdb::label::Labels result_lset;
  //   std::string content;
  //   head->series(i + 1, result_lset, &content);
  //   ASSERT_EQ(lset, result_lset);
  //   HeadIterator it(&content, 0, 10000000);
  //   int count = 0;
  //   while (it.next()) {
  //     ASSERT_EQ(times[i][count + 8], it.at().first);
  //     ASSERT_EQ(values[i][count + 8], it.at().second);
  //     count++;
  //   }
  //   ASSERT_EQ((num_samples + 1) % 8, count);
  // }
}

class MMapHeadWithTrieTest : public testing::Test {
 public:
  std::unique_ptr<MMapHeadWithTrie> head;

  void setup() {
    MMapHeadWithTrie* p = head.release();
    delete p;
    head.reset(new MMapHeadWithTrie(0, "", ".", nullptr));
  }

  void setup(const std::string& log_dir) {
    MMapHeadWithTrie* p = head.release();
    delete p;
    head.reset(new MMapHeadWithTrie(0, log_dir, ".", nullptr));
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
             memcmp(current_file.c_str(), "8xorarray", 9) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str(), "8gxorarray", 10) == 0)) {
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
        if (current_file.size() > HEAD_SAMPLES_LOG_NAME.size() &&
            memcmp(current_file.c_str(), HEAD_SAMPLES_LOG_NAME.c_str(),
                   HEAD_SAMPLES_LOG_NAME.size()) == 0) {
          to_remove.push_back(itr->path().string());
        } else if (current_file.size() > HEAD_FLUSHES_LOG_NAME.size() &&
                   memcmp(current_file.c_str(), HEAD_FLUSHES_LOG_NAME.c_str(),
                          HEAD_FLUSHES_LOG_NAME.size()) == 0) {
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
};

TEST_F(MMapHeadWithTrieTest, TestOutOfOrder) {
  clean_files();
  setup();

  auto app = head->appender();
  int num_ts = 10;
  int num_labels = 10;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));
    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    ASSERT_TRUE(app->add_fast(i + 1, 1000, 1.1).ok());
    ASSERT_TRUE(app->add_fast(i + 1, 2000, 1.1).ok());
    ASSERT_TRUE(app->add_fast(i + 1, 3000, 1.1).ok());
    ASSERT_TRUE(app->add_fast(i + 1, 5000, 1.1).ok());
    ASSERT_TRUE(app->add_fast(i + 1, 3000, 2.1).ok());
    ASSERT_TRUE(app->add_fast(i + 1, 4000, 3.1).ok());
    ASSERT_TRUE(app->commit().ok());
  }

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(0, it.at().first);
    ASSERT_EQ(0, it.at().second);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(1000, it.at().first);
    ASSERT_EQ(1.1, it.at().second);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(2000, it.at().first);
    ASSERT_EQ(1.1, it.at().second);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(3000, it.at().first);
    ASSERT_EQ(2.1, it.at().second);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(4000, it.at().first);
    ASSERT_EQ(3.1, it.at().second);
    ASSERT_TRUE(it.next());
    ASSERT_EQ(5000, it.at().first);
    ASSERT_EQ(1.1, it.at().second);
    ASSERT_FALSE(it.next());
  }

  int num_groups = 100;
  for (int i = 0; i < num_groups; i++) {
    uint64_t gid;
    std::vector<int> slots;
    tsdb::label::Labels lset;
    lset.emplace_back("group", "all");
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_0");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},0,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(0, slots[0]);

    lset.clear();
    slots.clear();
    lset.emplace_back("group", "all");
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_1");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},1,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(1, slots[0]);

    ASSERT_TRUE(app->add(gid, 2, {2.2, 2.2}).ok());
    ASSERT_TRUE(app->add(gid, 5, {3.2, 3.2}).ok());
    ASSERT_TRUE(app->add(gid, {0}, 2, {3.3}).ok());
    ASSERT_TRUE(app->add(gid, {1}, 4, {3.3}).ok());
    ASSERT_TRUE(app->commit().ok());
  }

  for (int i = 0; i < num_groups; i++) {
    tsdb::label::Labels g_lset({{"group", "group_" + std::to_string(i)}});
    tsdb::label::Labels tmp_lset1;
    tsdb::label::Labels tmp_lset2;
    std::string content;
    label::EqualMatcher m1("group", "all");
    HeadGroupSeries hs(head.get(),
                       (uint64_t)(i + num_ts + 1) | 0x8000000000000000, 0,
                       10000000, {&m1});
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("group", "all");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_0");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      ASSERT_TRUE(it->next());
      ASSERT_EQ(0, it->at().first);
      ASSERT_EQ(1.1, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(2, it->at().first);
      ASSERT_EQ(3.3, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(5, it->at().first);
      ASSERT_EQ(3.2, it->at().second);
      ASSERT_FALSE(it->next());
    }

    tmp_lset1.clear();
    tmp_lset2.clear();
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("group", "all");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_1");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      ASSERT_TRUE(it->next());
      ASSERT_EQ(1, it->at().first);
      ASSERT_EQ(1.1, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(2, it->at().first);
      ASSERT_EQ(2.2, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(4, it->at().first);
      ASSERT_EQ(3.3, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(5, it->at().first);
      ASSERT_EQ(3.2, it->at().second);
      ASSERT_FALSE(it->next());
    }
    ASSERT_FALSE(hs.next());
  }
}

TEST_F(MMapHeadWithTrieTest, TestMemPosingsSize) {
  index::MemPostingsWithTrie trie;
  double vm, rs;
  mem_usage(vm, rs);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rs / 1024) << "MB\n"
            << std::endl;

  for (int i = 0; i < 1000000; i++) {
    label::Labels lset;
    for (int j = 0; j < 20; j++) {
      lset.emplace_back("label" + std::to_string(j),
                        "label" + std::to_string(j) + "_" + std::to_string(i));
    }
    trie.add(i + 1, lset);
  }

  mem_usage(vm, rs);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rs / 1024) << "MB\n"
            << std::endl;
}

TEST_F(MMapHeadWithTrieTest, TestRepeatedAdd) {
  clean_files();
  setup();

  auto app = head->appender();
  int num_labels = 10;
  tsdb::label::Labels lset;
  for (int j = 0; j < num_labels; j++)
    lset.emplace_back("label" + std::to_string(j),
                      "label" + std::to_string(j) + "_0");
  auto r = app->add(lset, 0, 0);
  ASSERT_EQ(1, r.first);
  r = app->add(lset, 1, 0);
  ASSERT_EQ(1, r.first);
}

TEST_F(MMapHeadWithTrieTest, Test1) {
  clean_files();
  setup();

  auto app = head->appender();
  int num_ts = 10000;
  int num_labels = 10;
  std::set<std::string> syms;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));
      syms.insert("label_" + std::to_string(j));
      syms.insert("value_" + std::to_string(i));
    }
    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    ASSERT_TRUE(app->commit().ok());
  }

  int num_groups = 100;
  syms.insert("group");
  syms.insert("group_value_0");
  syms.insert("group_value_1");
  syms.insert("group_value_2");
  for (int i = 0; i < num_groups; i++) {
    uint64_t gid;
    std::vector<int> slots;
    syms.insert("group_" + std::to_string(i));
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_0");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},0,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(0, slots[0]);

    lset.clear();
    slots.clear();
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_1");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},1,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(1, slots[0]);

    lset.clear();
    slots.clear();
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_2");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},2,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(2, slots[0]);

    ASSERT_TRUE(app->add(gid, 3, {2.2, 2.2, 2.2}).ok());
    ASSERT_TRUE(app->add(gid, {1}, 4, {3.3}).ok());
  }

  // Symbols.
  ASSERT_EQ(syms, head->symbols());

  // Label names.
  std::vector<std::string> lnames;
  lnames.push_back("group");
  for (int j = 0; j < num_labels; j++)
    lnames.push_back("label_" + std::to_string(j));
  std::sort(lnames.begin(), lnames.end());
  ASSERT_EQ(lnames, head->label_names());

  // Label values.
  std::vector<std::string> lvalues(
      {"group_value_0", "group_value_1", "group_value_2"});
  for (int i = 0; i < num_ts; i++)
    lvalues.push_back("value_" + std::to_string(i));
  std::sort(lvalues.begin(), lvalues.end());
  for (int i = 0; i < num_labels; i++)
    ASSERT_EQ(lvalues, head->label_values("label_" + std::to_string(i)));

  // Postings.
  for (int i = 0; i < num_ts; i++) {
    for (int j = 0; j < num_labels; j++) {
      auto p = head->postings("label_" + std::to_string(j),
                              "value_" + std::to_string(i));
      ASSERT_TRUE(p.second);
      int num = 0;
      while (p.first->next()) {
        ASSERT_EQ(i + 1, p.first->at());
        num++;
      }
      ASSERT_EQ(1, num);
    }
  }
  auto p = head->postings(label::ALL_POSTINGS_KEYS.label,
                          label::ALL_POSTINGS_KEYS.value);
  ASSERT_TRUE(p.second);
  int num = 1;
  while (p.first->next()) {
    if (num > num_ts)
      ASSERT_EQ((uint64_t)(num) | 0x8000000000000000, p.first->at());
    else
      ASSERT_EQ(num, p.first->at());
    num++;
  }
  ASSERT_EQ(num_ts + num_groups + 1, num);
}

TEST_F(MMapHeadWithTrieTest, Test2) {
  clean_files();
  setup(".");

  auto app = head->appender();
  int num_ts = 10000;
  int num_labels = 10;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    std::vector<int64_t> tmp_times;
    std::vector<double> tmp_values;
    tmp_times.push_back(0);
    tmp_values.push_back(0);
    for (int j = 0; j < 6; j++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      tmp_times.push_back(t);
      tmp_values.push_back(v);
      ASSERT_TRUE(app->add_fast(i + 1, t, v).ok());
    }
    times.push_back(tmp_times);
    values.push_back(tmp_values);
    ASSERT_TRUE(app->commit().ok());
  }

  int num_groups = 100;
  for (int i = 0; i < num_groups; i++) {
    uint64_t gid;
    std::vector<int> slots;
    tsdb::label::Labels lset;
    lset.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_0");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},0,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(0, slots[0]);

    lset.clear();
    slots.clear();
    lset.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_1");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},1,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(1, slots[0]);

    lset.clear();
    slots.clear();
    lset.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j), "group_value_2");
    ASSERT_TRUE(app->add({{"group", "group_" + std::to_string(i)}}, {lset},2,
                          {1.1}, &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(1, slots.size());
    ASSERT_EQ(2, slots[0]);

    ASSERT_TRUE(app->add(gid, 3, {2.2, 2.2, 2.2}).ok());
    ASSERT_TRUE(app->add(gid, {1}, 4, {3.3}).ok());
    ASSERT_TRUE(app->commit().ok());
  }

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[i][count], it.at().first);
      ASSERT_EQ(values[i][count], it.at().second);
      count++;
    }
    ASSERT_EQ(7, count);
  }

  for (int i = 0; i < num_groups; i++) {
    tsdb::label::Labels g_lset({{"group", "group_" + std::to_string(i)}});
    tsdb::label::Labels tmp_lset1;
    tsdb::label::Labels tmp_lset2;
    std::string content;
    label::EqualMatcher m1("label", "group_value");
    label::EqualMatcher m2("label_0", "group_value_1");
    HeadGroupSeries hs(head.get(),
                       (uint64_t)(i + num_ts + 1) | 0x8000000000000000, 0,
                       10000000, {&m1, &m2});
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_1");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      ASSERT_TRUE(it->next());
      ASSERT_EQ(1, it->at().first);
      ASSERT_EQ(1.1, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(3, it->at().first);
      ASSERT_EQ(2.2, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(4, it->at().first);
      ASSERT_EQ(3.3, it->at().second);
      ASSERT_FALSE(it->next());
    }
    ASSERT_FALSE(hs.next());
  }

  setup(".");

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[i][count], it.at().first);
      ASSERT_EQ(values[i][count], it.at().second);
      count++;
    }
    ASSERT_EQ(7, count);
  }

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[i][count], it.at().first);
      ASSERT_EQ(values[i][count], it.at().second);
      count++;
    }
    ASSERT_EQ(7, count);
  }

  for (int i = 0; i < num_groups; i++) {
    tsdb::label::Labels g_lset({{"group", "group_" + std::to_string(i)}});
    tsdb::label::Labels tmp_lset1;
    tsdb::label::Labels tmp_lset2;
    std::string content;
    label::EqualMatcher m1("label", "group_value");
    label::EqualMatcher m2("label_0", "group_value_2");
    HeadGroupSeries hs(head.get(),
                       (uint64_t)(i + num_ts + 1) | 0x8000000000000000, 0,
                       10000000, {&m1, &m2});
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_2");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      ASSERT_TRUE(it->next());
      ASSERT_EQ(2, it->at().first);
      ASSERT_EQ(1.1, it->at().second);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(3, it->at().first);
      ASSERT_EQ(2.2, it->at().second);
      ASSERT_FALSE(it->next());
    }

    ASSERT_FALSE(hs.next());
  }
}

TEST_F(MMapHeadWithTrieTest, TestSamplesLog1) {
  clean_files();
  setup(".");

  MAX_HEAD_SAMPLES_LOG_SIZE = 1 * 1024 * 1024;

  auto app = head->TEST_appender();
  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 44;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  std::vector<int64_t> tmp_times;
  std::vector<double> tmp_values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    ASSERT_TRUE(app->TEST_commit().ok());
    tmp_times.push_back(0);
    tmp_values.push_back(0);
  }
  times.push_back(tmp_times);
  values.push_back(tmp_values);

  for (int j = 0; j < num_samples; j++) {
    tmp_times.clear();
    tmp_values.clear();
    for (int i = 0; i < num_ts; i++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      tmp_times.push_back(t);
      tmp_values.push_back(v);
      ASSERT_TRUE(app->add_fast(i + 1, t, v).ok());
    }
    times.push_back(tmp_times);
    values.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }

  int num_groups = 100;
  std::vector<std::vector<int64_t>> gtimes;
  std::vector<std::vector<double>> gvalues;
  for (int i = 0; i < num_groups; i++) {
    uint64_t gid;
    std::vector<int> slots;
    tsdb::label::Labels lset1;
    lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset1.emplace_back("label_" + std::to_string(j), "group_value_0");
    tsdb::label::Labels lset2;
    lset2.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset2.emplace_back("label_" + std::to_string(j), "group_value_1");
    tsdb::label::Labels lset3;
    lset3.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset3.emplace_back("label_" + std::to_string(j), "group_value_2");
    std::vector<int64_t> tmp_times;
    std::vector<double> tmp_values;
    tmp_times.push_back(0);
    tmp_values.push_back(0);
    ASSERT_TRUE(
        app->add({{"group", "group_" + std::to_string(i)}},
                 {lset1, lset2, lset3}, tmp_times.back(),
                 {tmp_values.back(), tmp_values.back(), tmp_values.back()},
                 &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(3, slots.size());
    ASSERT_EQ(0, slots[0]);
    ASSERT_EQ(1, slots[1]);
    ASSERT_EQ(2, slots[2]);

    for (int j = 0; j < num_samples - 1; j++) {
      tmp_times.push_back((j + 1) * 1000 + rand() % 100);
      tmp_values.push_back(static_cast<double>(rand()) /
                           static_cast<double>(RAND_MAX));
      ASSERT_TRUE(
          app->add(gid, tmp_times.back(),
                   {tmp_values.back(), tmp_values.back(), tmp_values.back()}).ok());
    }
    gtimes.push_back(tmp_times);
    gvalues.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[num_samples / 8 * 8 + count][i], it.at().first);
      ASSERT_EQ(values[num_samples / 8 * 8 + count][i], it.at().second);
      count++;
    }
    ASSERT_EQ((num_samples + 1) % 8, count);
  }

  for (int i = 0; i < num_groups; i++) {
    tsdb::label::Labels g_lset({{"group", "group_" + std::to_string(i)}});
    tsdb::label::Labels tmp_lset1;
    tsdb::label::Labels tmp_lset2;
    std::string content;
    label::EqualMatcher m1("label", "group_value");
    label::EqualMatcher m2("label_0", "group_value_1");
    HeadGroupSeries hs(head.get(),
                       (uint64_t)(i + num_ts + 1) | 0x8000000000000000, 0,
                       10000000, {&m1, &m2});
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_1");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      int count = 0;
      while (it->next()) {
        ASSERT_EQ(gtimes[i][num_samples / 8 * 8 + count], it->at().first);
        ASSERT_EQ(gvalues[i][num_samples / 8 * 8 + count], it->at().second);
        count++;
      }
      ASSERT_EQ(num_samples % 8, count);
    }
    ASSERT_FALSE(hs.next());
  }

  ASSERT_TRUE(head->clean_samples_logs().ok());

  clean_mmap_arrays(".");
  setup(".");

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[num_samples / 8 * 8 + count][i], it.at().first);
      ASSERT_EQ(values[num_samples / 8 * 8 + count][i], it.at().second);
      count++;
    }
    ASSERT_EQ((num_samples + 1) % 8, count);
  }

  for (int i = 0; i < num_groups; i++) {
    tsdb::label::Labels g_lset({{"group", "group_" + std::to_string(i)}});
    tsdb::label::Labels tmp_lset1;
    tsdb::label::Labels tmp_lset2;
    std::string content;
    label::EqualMatcher m1("label", "group_value");
    label::EqualMatcher m2("label_0", "group_value_2");
    HeadGroupSeries hs(head.get(),
                       (uint64_t)(i + num_ts + 1) | 0x8000000000000000, 0,
                       10000000, {&m1, &m2});
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_2");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      int count = 0;
      while (it->next()) {
        ASSERT_EQ(gtimes[i][num_samples / 8 * 8 + count], it->at().first);
        ASSERT_EQ(gvalues[i][num_samples / 8 * 8 + count], it->at().second);
        count++;
      }
      ASSERT_EQ(num_samples % 8, count);
    }
    ASSERT_FALSE(hs.next());
  }
}

TEST_F(MMapHeadWithTrieTest, TestSamplesLog2) {
  clean_files();
  setup(".");

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  Timer timer;
  timer.start();
  auto app = head->TEST_appender();
  int num_ts = 100000;
  int num_labels = 10;
  int num_samples = 1000;
  // std::vector<std::vector<int64_t>> times;
  // std::vector<std::vector<double>> values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    // std::vector<int64_t> tmp_times;
    // std::vector<double> tmp_values;
    // tmp_times.push_back(0);
    // tmp_values.push_back(0);
    for (int j = 0; j < num_samples; j++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      // tmp_times.push_back(t);
      // tmp_values.push_back(v);
      ASSERT_TRUE(app->add_fast(i + 1, t, v).ok());
    }
    // times.push_back(tmp_times);
    // values.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }
  uint64_t d = timer.since_start_nano();
  std::cout << "[Insertion duration (nanos)]:" << d << " [throughput]:"
            << ((double)(num_ts * num_samples) / (double)(d)*1000000000)
            << std::endl;
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;

  setup(".");
}

TEST_F(MMapHeadWithTrieTest, TestSamplesLogBGClean) {
  clean_files();
  setup(".");

  MAX_HEAD_SAMPLES_LOG_SIZE = 1 * 1024 * 1024;

  auto app = head->TEST_appender();
  int num_ts = 10000;
  int num_labels = 10;
  int num_samples = 95;
  std::vector<std::vector<int64_t>> times;
  std::vector<std::vector<double>> values;
  std::vector<int64_t> tmp_times;
  std::vector<double> tmp_values;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    auto r = app->add(lset, 0, 0);
    ASSERT_EQ(i + 1, r.first);
    ASSERT_TRUE(app->TEST_commit().ok());
    tmp_times.push_back(0);
    tmp_values.push_back(0);
  }
  times.push_back(tmp_times);
  values.push_back(tmp_values);

  for (int j = 0; j < num_samples; j++) {
    tmp_times.clear();
    tmp_values.clear();
    for (int i = 0; i < num_ts; i++) {
      int64_t t = (j + 1) * 1000 + rand() % 100;
      double v = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
      tmp_times.push_back(t);
      tmp_values.push_back(v);
      ASSERT_TRUE(app->add_fast(i + 1, t, v).ok());
    }
    times.push_back(tmp_times);
    values.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }

  int num_groups = 100;
  std::vector<std::vector<int64_t>> gtimes;
  std::vector<std::vector<double>> gvalues;
  for (int i = 0; i < num_groups; i++) {
    uint64_t gid;
    std::vector<int> slots;
    tsdb::label::Labels lset1;
    lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset1.emplace_back("label_" + std::to_string(j), "group_value_0");
    tsdb::label::Labels lset2;
    lset2.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset2.emplace_back("label_" + std::to_string(j), "group_value_1");
    tsdb::label::Labels lset3;
    lset3.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      lset3.emplace_back("label_" + std::to_string(j), "group_value_2");
    std::vector<int64_t> tmp_times;
    std::vector<double> tmp_values;
    tmp_times.push_back(0);
    tmp_values.push_back(0);
    ASSERT_TRUE(
        app->add({{"group", "group_" + std::to_string(i)}},
                 {lset1, lset2, lset3}, tmp_times.back(),
                 {tmp_values.back(), tmp_values.back(), tmp_values.back()},
                 &gid, &slots).ok());
    ASSERT_EQ((uint64_t)(i + num_ts + 1) | 0x8000000000000000, gid);
    ASSERT_EQ(3, slots.size());
    ASSERT_EQ(0, slots[0]);
    ASSERT_EQ(1, slots[1]);
    ASSERT_EQ(2, slots[2]);

    for (int j = 0; j < num_samples - 1; j++) {
      tmp_times.push_back((j + 1) * 1000 + rand() % 100);
      tmp_values.push_back(static_cast<double>(rand()) /
                           static_cast<double>(RAND_MAX));
      ASSERT_TRUE(
          app->add(gid, tmp_times.back(),
                   {tmp_values.back(), tmp_values.back(), tmp_values.back()}).ok());
    }
    gtimes.push_back(tmp_times);
    gvalues.push_back(tmp_values);
    ASSERT_TRUE(app->TEST_commit().ok());
  }

  sleep(5);

  clean_mmap_arrays(".");
  setup(".");

  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++)
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(i));

    tsdb::label::Labels result_lset;
    std::string content;
    head->series(i + 1, result_lset, &content);
    ASSERT_EQ(lset, result_lset);
    HeadIterator it(&content, 0, 10000000);
    int count = 0;
    while (it.next()) {
      ASSERT_EQ(times[num_samples / 8 * 8 + count][i], it.at().first);
      ASSERT_EQ(values[num_samples / 8 * 8 + count][i], it.at().second);
      count++;
    }
    ASSERT_EQ((num_samples + 1) % 8, count);
  }

  for (int i = 0; i < num_groups; i++) {
    tsdb::label::Labels g_lset({{"group", "group_" + std::to_string(i)}});
    tsdb::label::Labels tmp_lset1;
    tsdb::label::Labels tmp_lset2;
    std::string content;
    label::EqualMatcher m1("label", "group_value");
    label::EqualMatcher m2("label_0", "group_value_2");
    HeadGroupSeries hs(head.get(),
                       (uint64_t)(i + num_ts + 1) | 0x8000000000000000, 0,
                       10000000, {&m1, &m2});
    ASSERT_TRUE(hs.next());
    ASSERT_EQ(g_lset, hs.labels());
    tmp_lset1 = g_lset;
    tmp_lset1.emplace_back("label", "group_value");
    for (int j = 0; j < num_labels; j++)
      tmp_lset1.emplace_back("label_" + std::to_string(j), "group_value_2");
    hs.labels(tmp_lset2);
    ASSERT_EQ(tmp_lset1, tmp_lset2);
    {
      auto it = hs.iterator();
      int count = 0;
      while (it->next()) {
        ASSERT_EQ(gtimes[i][num_samples / 8 * 8 + count], it->at().first);
        ASSERT_EQ(gvalues[i][num_samples / 8 * 8 + count], it->at().second);
        count++;
      }
      ASSERT_EQ(num_samples % 8, count);
    }
    ASSERT_FALSE(hs.next());
  }
}

TEST_F(MMapHeadWithTrieTest, TestOOM1) {
  clean_files();
  setup(".");

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
  int num_ts = 8000000;
  int num_labels = 20;
  Timer timer;
  timer.start();
  auto app = head->appender();
  uint64_t last_d = 0;
  for (int i = 0; i < num_ts; i++) {
    tsdb::label::Labels lset;
    for (int j = 0; j < num_labels; j++) {
      lset.emplace_back("label_" + std::to_string(j),
                        "value_" + std::to_string(j) + "_" + std::to_string(i));
    }
    app->add(lset, 0, 0);
    // app->rollback();
    if ((i + 1) % (num_ts / 10) == 0) {
      uint64_t d = timer.since_start_nano();
      std::cout << "[Insertion duration (ms)]:" << ((d - last_d) / 1000000)
                << " [throughput]:"
                << ((double)(num_ts / 10) / (double)(d - last_d) * 1000000000)
                << std::endl;
      last_d = d;
      mem_usage(vm, rss);
      mem_usage(vm, rss);
      std::cout << "Virtual Memory: " << (vm / 1024)
                << "MB\nResident set size: " << (rss / 1024) << "MB\n"
                << std::endl;
    }
  }
  uint64_t d = timer.since_start_nano();
  std::cout << "[Total Insertion duration (ms)]:" << (d / 1000000)
            << " [Total throughput]:"
            << ((double)(num_ts) / (double)(d)*1000000000) << std::endl;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;
}

class MemGroupTest : public testing::Test {
 public:
  MMapMemGroup* g;

  void clean_mmap_arrays(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 9 &&
             memcmp(current_file.c_str(), "8intarray", 9) == 0) ||
            (current_file.size() > 11 &&
             memcmp(current_file.c_str(), "8floatarray", 11) == 0) ||
            (current_file.size() > 12 &&
             memcmp(current_file.c_str(), "8gfloatarray", 12) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str(), "8gxorarray", 10) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
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
    clean_mmap_arrays(".");
    clean_mmap_labels(".");
  }
};

TEST_F(MemGroupTest, TestVectorSize) {
  int num = 10000;
  std::vector<std::vector<std::string>> vecs;
  vecs.reserve(num);
  std::vector<label::Labels> lsets =
      read_labels(20000, "../test/timeseries.json");
  std::set<std::string> lsets_set;
  for (const auto& lset : lsets) {
    for (const auto& l : lset) lsets_set.insert(l.label + "$" + l.value);
  }
  std::cout << "size:" << lsets_set.size() << std::endl;

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  for (size_t i = 0; i < num; i++) {
    vecs.emplace_back();
    for (const auto& s : lsets_set) vecs.back().push_back(s);
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
}

TEST_F(MemGroupTest, TestTrieSize) {
  int num = 10000;
  std::vector<cedar::da<int>*> tries;
  tries.reserve(num);
  std::vector<label::Labels> lsets =
      read_labels(20000, "../test/timeseries.json");

  double vm, rss;
  mem_usage(vm, rss);
  std::cout << "Virtual Memory: " << (vm / 1024)
            << "MB\nResident set size: " << (rss / 1024) << "MB\n"
            << std::endl;

  for (size_t i = 0; i < num; i++) {
    tries.push_back(new cedar::da<int>());
    for (const auto& lset : lsets) {
      for (const auto& l : lset)
        tries.back()->update((l.label + "$" + l.value).c_str(),
                             l.label.size() + l.value.size() + 1, 0);
    }
  }
  double vm_after, rss_after;
  mem_usage(vm_after, rss_after);
  std::cout << "Virtual Memory: " << (vm_after / 1024)
            << "MB\nResident set size: " << (rss_after / 1024) << "MB\n"
            << std::endl;
  std::cout << "Increased Virtual Memory: " << ((vm_after - vm) / 1024)
            << "MB\nIncreased Resident set size: " << ((rss_after - rss) / 1024)
            << "MB\n"
            << std::endl;
}

// From fewer to more TS.
TEST_F(MemGroupTest, Test1) {
  clean_files();

  label::Labels g_lset({{"group", "system"}, {"location", "CUHK"}});
  MMapGroupXORChunk m_xor(".", 8, 10000, "g");
  MAX_HEAD_LABELS_FILE_SIZE = 32 * 1024 * 1024;
#if USE_MMAP_LABELS
  MMapGroupLabels m_labels(".");
  g = new MMapMemGroup(g_lset, 0, &m_xor, true, &m_labels, -1);
  std::vector<int> slots;
  ASSERT_FALSE(g->append(nullptr, {{{"ts", "1"}, {"metric", "trash"}}}, 0,
                         {1.5}, &slots));
  ASSERT_EQ(1, slots.size());
  ASSERT_EQ(0, slots[0]);

  slots.clear();
  ASSERT_FALSE(g->append(nullptr,
                         {{{"ts", "2"}, {"metric", "garbage"}},
                          {{"ts", "3"}, {"metric", "rubbish"}}},
                         1, {2.5, 3.5}, &slots));
  ASSERT_EQ(2, slots.size());
  ASSERT_EQ(1, slots[0]);
  ASSERT_EQ(2, slots[1]);

  std::string s;
  g->encode_group(&s);
  chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(s.c_str()), s.size());
  auto it = c.iterator(0);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(0, it->at().first);
  ASSERT_EQ(1.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(1, it->at().first);
  ASSERT_EQ(std::numeric_limits<double>::max(), it->at().second);
  ASSERT_FALSE(it->next());

  it = c.iterator(2);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(0, it->at().first);
  ASSERT_EQ(std::numeric_limits<double>::max(), it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(1, it->at().first);
  ASSERT_EQ(3.5, it->at().second);
  ASSERT_FALSE(it->next());
#else
  g = new MMapMemGroup(g_lset, 0, &m_xor, true);
  std::vector<int> slots;
  ASSERT_FALSE(g->append(nullptr, {{{"ts", "1"}, {"metric", "trash"}}}, 0,
                         {1.5}, &slots));
  ASSERT_EQ(1, slots.size());
  ASSERT_EQ(0, slots[0]);

  slots.clear();
  ASSERT_FALSE(g->append(nullptr,
                         {{{"ts", "2"}, {"metric", "garbage"}},
                          {{"ts", "3"}, {"metric", "rubbish"}}},
                         0, {2.5, 3.5}, &slots));
  ASSERT_EQ(2, slots.size());
  ASSERT_EQ(1, slots[0]);
  ASSERT_EQ(2, slots[1]);
#endif
  delete g;
}

TEST_F(MemGroupTest, Test2) {
  clean_files();

  label::Labels g_lset({{"group", "system"}, {"location", "CUHK"}});
  MMapGroupXORChunk m_xor(".", 8, 10000, "g");
  MAX_HEAD_LABELS_FILE_SIZE = 32 * 1024 * 1024;
#if USE_MMAP_LABELS
  MMapGroupLabels m_labels(".");
  g = new MMapMemGroup(g_lset,
                       {{{"ts", "2"}, {"metric", "garbage"}},
                        {{"ts", "3"}, {"metric", "rubbish"}}},
                       0, &m_xor, true, &m_labels, -1);
  std::vector<int> slots;
  ASSERT_FALSE(g->append(nullptr, {{{"ts", "1"}, {"metric", "trash"}}}, 0,
                         {1.5}, &slots));
  ASSERT_EQ(1, slots.size());
  ASSERT_EQ(2, slots[0]);

  slots.clear();
  ASSERT_FALSE(g->append(nullptr,
                         {{{"ts", "2"}, {"metric", "garbage"}},
                          {{"ts", "3"}, {"metric", "rubbish"}}},
                         1, {2.5, 3.5}, &slots));
  ASSERT_EQ(2, slots.size());
  ASSERT_EQ(0, slots[0]);
  ASSERT_EQ(1, slots[1]);

  std::string s;
  g->encode_group(&s);
  chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(s.c_str()), s.size());
  auto it = c.iterator(2);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(0, it->at().first);
  ASSERT_EQ(1.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(1, it->at().first);
  ASSERT_EQ(std::numeric_limits<double>::max(), it->at().second);
  ASSERT_FALSE(it->next());

  it = c.iterator(1);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(0, it->at().first);
  ASSERT_EQ(std::numeric_limits<double>::max(), it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(1, it->at().first);
  ASSERT_EQ(3.5, it->at().second);
  ASSERT_FALSE(it->next());
#else
  g = new MMapMemGroup(g_lset, 0, &m_xor, true);
  std::vector<int> slots;
  ASSERT_FALSE(g->append(nullptr, {{{"ts", "1"}, {"metric", "trash"}}}, 0,
                         {1.5}, &slots));
  ASSERT_EQ(1, slots.size());
  ASSERT_EQ(0, slots[0]);

  slots.clear();
  ASSERT_FALSE(g->append(nullptr,
                         {{{"ts", "2"}, {"metric", "garbage"}},
                          {{"ts", "3"}, {"metric", "rubbish"}}},
                         0, {2.5, 3.5}, &slots));
  ASSERT_EQ(2, slots.size());
  ASSERT_EQ(1, slots[0]);
  ASSERT_EQ(2, slots[1]);
#endif
  delete g;
}

TEST_F(MemGroupTest, Test3) {
  clean_files();

  label::Labels g_lset({{"group", "system"}, {"location", "CUHK"}});
  MMapGroupXORChunk m_xor(".", 8, 10000, "g");
  MAX_HEAD_LABELS_FILE_SIZE = 32 * 1024 * 1024;
#if USE_MMAP_LABELS
  MMapGroupLabels m_labels(".");
  g = new MMapMemGroup(g_lset,
                       {{{"ts", "1"}, {"metric", "trash"}},
                        {{"ts", "2"}, {"metric", "garbage"}},
                        {{"ts", "3"}, {"metric", "rubbish"}}},
                       0, &m_xor, true, &m_labels, -1);
  std::vector<int> slots;
  ASSERT_FALSE(g->append(nullptr, 0, {1.5, 1.54, 1.43}));

  slots.clear();
  ASSERT_FALSE(g->append(nullptr,
                         {{{"ts", "2"}, {"metric", "garbage"}},
                          {{"ts", "3"}, {"metric", "rubbish"}}},
                         1, {2.5, 3.5}, &slots));
  ASSERT_EQ(2, slots.size());
  ASSERT_EQ(1, slots[0]);
  ASSERT_EQ(2, slots[1]);

  ASSERT_FALSE(g->append(nullptr, {0}, 2, {1.8}));

  std::string s;
  g->encode_group(&s);
  chunk::NullSupportedXORGroupChunk c(
      reinterpret_cast<const uint8_t*>(s.c_str()), s.size());
  auto it = c.iterator(2);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(0, it->at().first);
  ASSERT_EQ(1.43, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(1, it->at().first);
  ASSERT_EQ(3.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(2, it->at().first);
  ASSERT_EQ(std::numeric_limits<double>::max(), it->at().second);
  ASSERT_FALSE(it->next());

  it = c.iterator(0);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(0, it->at().first);
  ASSERT_EQ(1.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(1, it->at().first);
  ASSERT_EQ(std::numeric_limits<double>::max(), it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(2, it->at().first);
  ASSERT_EQ(1.8, it->at().second);
  ASSERT_FALSE(it->next());
#else
  g = new MMapMemGroup(g_lset, 0, &m_xor, true);
  std::vector<int> slots;
  ASSERT_FALSE(g->append(nullptr, {{{"ts", "1"}, {"metric", "trash"}}}, 0,
                         {1.5}, &slots));
  ASSERT_EQ(1, slots.size());
  ASSERT_EQ(0, slots[0]);

  slots.clear();
  ASSERT_FALSE(g->append(nullptr,
                         {{{"ts", "2"}, {"metric", "garbage"}},
                          {{"ts", "3"}, {"metric", "rubbish"}}},
                         0, {2.5, 3.5}, &slots));
  ASSERT_EQ(2, slots.size());
  ASSERT_EQ(1, slots[0]);
  ASSERT_EQ(2, slots[1]);
#endif
  delete g;
}

}  // namespace head
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
