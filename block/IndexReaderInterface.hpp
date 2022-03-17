#ifndef INDEXREADERINTERFACE_H
#define INDEXREADERINTERFACE_H

#include <stdint.h>

#include <boost/function.hpp>
#include <deque>
#include <initializer_list>
#include <set>
// #include <boost/utility/string_ref.hpp>

#include "chunk/ChunkMeta.hpp"
#include "index/PostingsInterface.hpp"
#include "label/Label.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace block {

class SerializedTuples;

class IndexReaderInterface {
 public:
  virtual std::set<std::string> symbols() = 0;
  virtual const std::deque<std::string> &symbols_deque() const = 0;

  // Return empty SerializedTuples when error
  virtual std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(
      const std::initializer_list<std::string> &names) = 0;
  virtual std::unique_ptr<tsdbutil::StringTuplesInterface> label_values(
      const std::string &name) {
    return nullptr;
  }
  virtual std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings(
      const std::string &name, const std::string &value) = 0;

  // 1. Get the corresponding group postings entry.
  // 2. Pass ALL_GROUP_POSTINGS, get offsets of all group postings entries.
  virtual std::pair<std::unique_ptr<index::PostingsInterface>, bool>
  group_postings(uint64_t group_ref) {
    return {nullptr, false};
  }
  virtual bool series(
      uint64_t ref, label::Labels &lset,
      std::deque<std::shared_ptr<chunk::ChunkMeta>> &chunks) = 0;
  virtual bool series(uint64_t ref, label::Labels &lset,
                      const std::shared_ptr<chunk::ChunkMeta> &chunk) {
    return false;
  }
  virtual std::deque<std::string> label_names() = 0;

  virtual bool error() = 0;
  virtual uint64_t size() = 0;

  virtual std::unique_ptr<index::PostingsInterface> sorted_postings(
      std::unique_ptr<index::PostingsInterface> &&p) = 0;
  // NOTE(Alec), this is not to sort the content inside a group postings entry
  // but sort all the groups by their first series' labels.
  virtual std::unique_ptr<index::PostingsInterface> sorted_group_postings(
      std::unique_ptr<index::PostingsInterface> &&p) {
    return nullptr;
  }

  virtual ~IndexReaderInterface() = default;
};

}  // namespace block
}  // namespace tsdb

#endif