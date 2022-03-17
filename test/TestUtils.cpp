#include "test/TestUtils.hpp"

#include <stdlib.h>

#include <boost/tokenizer.hpp>
#include <fstream>

namespace tsdb {
namespace test {

std::deque<std::deque<double>> load_sample_data(const std::string &filename) {
  std::deque<std::deque<double>> r;
  std::ifstream file(filename);
  std::string line;
  boost::char_separator<char> tokenSep(",");
  while (getline(file, line)) {
    r.push_back(std::deque<double>());
    boost::tokenizer<boost::char_separator<char>> tokens(line, tokenSep);
    for (auto const &token : tokens) {
      r.back().push_back(atof(token.c_str()));
    }
  }
  return r;
}

std::vector<std::string> split(const std::string &str,
                               const std::string &delim) {
  std::vector<std::string> tokens;
  size_t prev = 0, pos = 0;
  do {
    pos = str.find(delim, prev);
    if (pos == std::string::npos) pos = str.length();
    std::string token = str.substr(prev, pos - prev);
    if (!token.empty()) tokens.push_back(token);
    prev = pos + delim.length();
  } while (pos < str.length() && prev < str.length());
  return tokens;
}

}  // namespace test
}  // namespace tsdb