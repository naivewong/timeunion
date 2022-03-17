#ifndef SERIESINTERFACE_H
#define SERIESINTERFACE_H

#include "label/Label.hpp"
#include "querier/SeriesIteratorInterface.hpp"

namespace tsdb {
namespace querier {

enum SeriesType { kTypeSeries = 0, kTypeGroup = 1 };

class SeriesInterface {
 public:
  virtual const label::Labels& labels() const = 0;

  virtual std::unique_ptr<SeriesIteratorInterface> iterator() = 0;

  virtual std::unique_ptr<SeriesIteratorInterface> chain_iterator() {
    return nullptr;
  }

  virtual ~SeriesInterface() = default;

  virtual void labels(label::Labels& lset) {}
  virtual SeriesType type() { return kTypeSeries; }
  virtual bool next() { return false; }
};

}  // namespace querier
}  // namespace tsdb

#endif