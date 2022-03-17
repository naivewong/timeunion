#ifndef TSDBEXCEPTION_H
#define TSDBEXCEPTION_H
#include <exception>

namespace tsdb {
namespace base {

class TSDBException : public std::exception {
 public:
  char const *err;

  TSDBException(char const *err) { this->err = err; }

  const char *what() const throw() { return err; }
};

}  // namespace base
}  // namespace tsdb

#endif