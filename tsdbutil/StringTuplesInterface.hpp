#ifndef STRINGTUPLESINTERFACE_H
#define STRINGTUPLESINTERFACE_H

namespace tsdb {
namespace tsdbutil {

class StringTuplesInterface {
 public:
  virtual uint32_t len() = 0;
  virtual std::string at(int i) = 0;
  ~StringTuplesInterface() = default;
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif