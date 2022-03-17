#ifndef APPENDERINTERFACE_H
#define APPENDERINTERFACE_H

#include "base/Error.hpp"
#include "label/Label.hpp"
#include "leveldb/status.h"

namespace tsdb {
namespace db {

// Appender allows appending a batch of data. It must be completed with a
// call to commit or rollback and must not be reused afterwards.
//
// Operations on the Appender interface are not thread-safe.
class AppenderInterface {
 public:
  // add adds a sample pair for the given series. A reference number is
  // returned which can be used to add further samples in the same or later
  // transactions.
  // Returned reference numbers are ephemeral and may be rejected in calls
  // to AddFast() at any point. Adding the sample via add() returns a new
  // reference number.
  // If the reference is 0 it must not be used for caching.
  virtual std::pair<uint64_t, leveldb::Status> add(const label::Labels& lset,
                                                   int64_t t, double v) = 0;

  // For group.
  virtual leveldb::Status add(
      const label::Labels& group_lset,
      const std::vector<label::Labels>& individual_lsets, int64_t timestamp,
      const std::vector<double>& values, uint64_t* gid,
      std::vector<int>* slots) {
    return leveldb::Status::OK();
  }
  virtual leveldb::Status add(uint64_t ref, int64_t timestamp,
                              const std::vector<double>& values) {
    return leveldb::Status::OK();
  }
  virtual leveldb::Status add(uint64_t ref, const std::vector<int>& slots,
                              int64_t timestamp,
                              const std::vector<double>& values) {
    return leveldb::Status::OK();
  }

  // add_fast adds a sample pair for the referenced series. It is generally
  // faster than adding a sample by providing its full label set.
  virtual leveldb::Status add_fast(uint64_t ref, int64_t t, double v) = 0;

  // commit submits the collected samples and purges the batch.
  virtual leveldb::Status commit() = 0;

  // rollback rolls back all modifications made in the appender so far.
  virtual leveldb::Status rollback() = 0;

  virtual ~AppenderInterface() = default;
};

}  // namespace db
}  // namespace tsdb

#endif