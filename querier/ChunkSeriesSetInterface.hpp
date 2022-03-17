#ifndef CHUNKSERIESSETINTERFACE_H
#define CHUNKSERIESSETINTERFACE_H

#include "base/Error.hpp"
#include "querier/ChunkSeriesMeta.hpp"

namespace tsdb {
namespace querier {

// Expose actual series' labels and series' data points
class ChunkSeriesSetInterface {
 public:
  virtual bool next() const = 0;
  virtual const std::shared_ptr<ChunkSeriesMeta> &at() const = 0;
  virtual bool error() const = 0;
  virtual error::Error error_detail() const { return error::Error(); }
  virtual ~ChunkSeriesSetInterface() = default;
};

}  // namespace querier
}  // namespace tsdb

#endif