#ifndef NOTMATCH_H
#define NOTMATCH_H

#include <label/MatcherInterface.hpp>

namespace tsdb {
namespace label {

class NotMatcher : public MatcherInterface {
 private:
  std::shared_ptr<MatcherInterface> matcher;

 public:
  NotMatcher(const std::shared_ptr<MatcherInterface> &matcher)
      : matcher(matcher) {}

  const std::string &name() const { return matcher->name(); }

  std::string value() const { return ""; }

  bool match(const std::string &s) const { return !matcher->match(s); }

  std::string class_name() const { return "not"; }
};

}  // namespace label
}  // namespace tsdb

#endif