#ifndef BLOCKINTERFACE_H
#define BLOCKINTERFACE_H

#include <deque>
#include <initializer_list>
#include <limits>

#include "base/Error.hpp"
#include "block/BlockUtils.hpp"
#include "block/ChunkReaderInterface.hpp"
#include "block/IndexReaderInterface.hpp"
#include "label/MatcherInterface.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"

namespace tsdb {
namespace block {

class BlockInterface {
 public:
  // index returns an IndexReader over the block's data, succeed or not.
  virtual std::pair<std::shared_ptr<IndexReaderInterface>, bool> index()
      const = 0;

  // chunks returns a ChunkReader over the block's data, succeed or not.
  virtual std::pair<std::shared_ptr<ChunkReaderInterface>, bool> chunks()
      const = 0;

  // tombstones returns a TombstoneReader over the block's deleted data, succeed
  // or not.
  virtual std::pair<std::shared_ptr<tombstone::TombstoneReaderInterface>, bool>
  tombstones() const = 0;

  virtual uint8_t type() { return -1; }

  virtual std::string dir() { return ""; }

  virtual bool overlap_closed(int64_t mint, int64_t maxt) const {
    return false;
  }

  virtual BlockMeta meta() const { return BlockMeta(); }

  virtual int64_t MaxTime() const {
    return std::numeric_limits<int64_t>::min();
  }

  virtual int64_t MinTime() const {
    return std::numeric_limits<int64_t>::max();
  }

  // size returns the number of bytes that the block takes up.
  virtual uint64_t size() const { return 0; }

  virtual bool set_compaction_failed() { return true; }

  virtual bool set_deletable() { return true; }

  virtual error::Error del(
      int64_t mint, int64_t maxt,
      const std::deque<std::shared_ptr<label::MatcherInterface>> &matchers) {
    return error::Error();
  }
  virtual std::pair<ulid::ULID, error::Error> clean_tombstones(
      const std::string &dest, void *compactor) {
    return {ulid::ULID(), error::Error()};
  }
  virtual error::Error error() const = 0;

  virtual void close() const {}

  virtual ~BlockInterface() = default;
};

class Blocks {
 private:
  std::deque<std::shared_ptr<BlockInterface>> blocks;

 public:
  Blocks() = default;
  Blocks(const std::deque<std::shared_ptr<BlockInterface>> &blocks)
      : blocks(blocks) {}
  Blocks(const std::initializer_list<std::shared_ptr<BlockInterface>> &blocks)
      : blocks(blocks.begin(), blocks.end()) {}

  void push_back(const std::shared_ptr<BlockInterface> &block) {
    blocks.push_back(block);
  }

  void clear() { blocks.clear(); }

  int size() const { return blocks.size(); }

  bool empty() const { return blocks.empty(); }

  const std::shared_ptr<BlockInterface> &at(int i) const { return blocks[i]; }

  const std::shared_ptr<BlockInterface> &front() const {
    return blocks.front();
  }

  const std::shared_ptr<BlockInterface> &back() const { return blocks.back(); }
};

}  // namespace block
}  // namespace tsdb

#endif