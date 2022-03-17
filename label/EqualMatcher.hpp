#ifndef EQUALMATCHER_H
#define EQUALMATCHER_H

#include "label/MatcherInterface.hpp"

namespace tsdb {
namespace label {

class EqualMatcher : public MatcherInterface {
 private:
  std::string name_;
  std::string value_;

 public:
  EqualMatcher(const std::string &name, const std::string &value)
      : name_(name), value_(value) {}

  const std::string &name() const { return name_; }

  std::string value() const { return value_; }

  // boost::string_ref rname(){
  //     return name_;
  // }

  bool match(const std::string &s) const { return (s.compare(value_) == 0); }

  std::string class_name() const { return "equal"; }
  // bool rmatch(const boost::string_ref & s){
  //     return (s.compare(value_) == 0);
  // }
};

}  // namespace label
}  // namespace tsdb

#endif