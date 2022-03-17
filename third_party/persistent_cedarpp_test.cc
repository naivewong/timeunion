#include <boost/filesystem.hpp>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>

// #include "cedar.h"
#include "gtest/gtest.h"
#include "label/Label.hpp"
#include "third_party/persistent_cedarpp.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"

// int main() {
//   {
//     cedar::da <int> trie(".");
//     trie.update("alec", 4, 1);
//     trie.update("alecwang", 8, 1);
//     trie.update("ckscjc", 6, 1);

//     char line[8192];
//     size_t pos = 0;
//     size_t from = 0;
//     if (trie.find("alecwa", from, pos) != cedar::da<int>::CEDAR_NO_PATH) {
//       size_t len = 6;
//       std::fprintf(stdout, "%d %lu %lu\n", trie.begin(from, len), from, len);
//       trie.suffix(line, len, from);
//       line[len] = '\0';
//       std::fprintf(stdout, "%s\n", line);
//       while (true) {
//         int r = trie.next(from, len);
//         std::fprintf(stdout, "%d %lu %lu\n", r, from, len);
//         if (r == cedar::da<int>::CEDAR_NO_PATH)
//           break;
//         trie.suffix(line, len, from);
//         line[len] = '\0';
//         std::fprintf(stdout, "%s\n", line);
//       }
//     }
//   }

//   // Reopen.
//   {
//     cedar::da <int> trie(".");

//     char line[8192];
//     size_t pos = 0;
//     size_t from = 0;
//     if (trie.find("alecwa", from, pos) != cedar::da<int>::CEDAR_NO_PATH) {
//       size_t len = 6;
//       std::fprintf(stdout, "%d %lu %lu\n", trie.begin(from, len), from, len);
//       trie.suffix(line, len, from);
//       line[len] = '\0';
//       std::fprintf(stdout, "%s\n", line);
//       while (true) {
//         int r = trie.next(from, len);
//         std::fprintf(stdout, "%d %lu %lu\n", r, from, len);
//         if (r == cedar::da<int>::CEDAR_NO_PATH)
//           break;
//         trie.suffix(line, len, from);
//         line[len] = '\0';
//         std::fprintf(stdout, "%s\n", line);
//       }
//     }
//   }

//   return 0;
// }

std::vector<::tsdb::label::Labels> read_labels(int num,
                                               const std::string& name) {
  std::vector<::tsdb::label::Labels> lsets;
  std::ifstream file(name);
  std::string line;
  while (getline(file, line) && lsets.size() < num) {
    rapidjson::Document d;
    d.Parse(line.c_str());
    ::tsdb::label::Labels lset;
    for (auto& m : d.GetObject())
      lset.emplace_back(m.name.GetString(), m.value.GetString());
    std::sort(lset.begin(), lset.end());
    lsets.push_back(lset);
  }
  return lsets;
}

class CedarTest : public testing::Test {
 public:
  std::unique_ptr<pcedar::da<int>> trie_;

  void setup(const std::string& dir) {
    trie_.reset(new pcedar::da<int>(dir, "test_"));
  }

  void release() { trie_.reset(); }

  void clean_files(const std::string& dir) {
    std::vector<std::string> to_remove;
    boost::filesystem::path p(dir);
    boost::filesystem::directory_iterator end_itr;
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      if (boost::filesystem::is_regular_file(itr->path())) {
        std::string current_file = itr->path().filename().string();
        if ((current_file.size() > 10 &&
             memcmp(current_file.c_str() + 5, "array", 5) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str() + 5, "ninfo", 5) == 0) ||
            (current_file.size() > 10 &&
             memcmp(current_file.c_str() + 5, "block", 5) == 0) ||
            (current_file.size() > 9 &&
             memcmp(current_file.c_str() + 5, "tail", 4) == 0)) {
          to_remove.push_back(itr->path().string());
        }
      }
    }

    for (const auto& name : to_remove) {
      std::cout << "clean " << name << std::endl;
      boost::filesystem::remove(name);
    }
  }
};

TEST_F(CedarTest, Test1) {
  clean_files(".");
  setup(".");

  int i = 0;
  std::vector<::tsdb::label::Labels> lsets =
      read_labels(20000, "../test/timeseries.json");
  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      std::string tmp = l.label + "%" + l.value;
      trie_->update(tmp.c_str(), tmp.size(), 0);
    }
    i++;
  }

  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      std::string tmp = l.label + "%" + l.value;
      ASSERT_EQ(0, trie_->exactMatchSearch<int>(tmp.c_str()));
    }
  }

  std::cout << "first round finishes" << std::endl;
  release();
  setup(".");
  std::cout << "second round begins" << std::endl;

  for (const auto& lset : lsets) {
    for (const auto& l : lset) {
      std::string tmp = l.label + "%" + l.value;
      ASSERT_EQ(0, trie_->exactMatchSearch<int>(tmp.c_str()));
    }
  }
}

// TEST_F(CedarTest, Test2) {
//   clean_files();
//   setup(".");

//   release();
//   setup(".");
// }

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}