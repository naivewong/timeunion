#ifndef MATCHERINTERFACE_H
#define MATCHERINTERFACE_H

#include <deque>
// #include <boost/utility/string_ref.hpp>

namespace tsdb {
namespace label {

class MatcherInterface {
 public:
  virtual const std::string &name() const = 0;
  virtual std::string value() const = 0;
  // virtual boost::string_ref rname()=0;
  virtual bool match(const std::string &s) const = 0;

  virtual std::string class_name() const = 0;
  // virtual bool rmatch(const boost::string_ref & s)=0;
  virtual ~MatcherInterface() = default;
};

typedef std::deque<MatcherInterface *> Selector;

}  // namespace label
}  // namespace tsdb

#endif