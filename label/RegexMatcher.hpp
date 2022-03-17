#ifndef REGEXMATCHER_H
#define REGEXMATCHER_H

#include <regex>

#include "label/MatcherInterface.hpp"

namespace tsdb {
namespace label {

// Will return an regex that matches nothing when invalid pattern
class RegexMatcher : public MatcherInterface {
 private:
  std::string name_;
  std::regex re;

 public:
  RegexMatcher(const std::string &name, const std::string &pattern)
      : name_(name) {
    try {
      re.assign(pattern);
    } catch (const std::regex_error &e) {
    }
  }

  const std::string &name() const { return name_; }

  std::string value() const { return ""; }

  // boost::string_ref rname(){
  //     return name_;
  // }

  bool match(const std::string &s) const { return std::regex_match(s, re); }

  std::string class_name() const { return "regex"; }
  // bool rmatch(const boost::string_ref & s){
  //     return std::regex_match(std::string(s.data(), s.length()), re);
  // }
};

}  // namespace label
}  // namespace tsdb

#endif