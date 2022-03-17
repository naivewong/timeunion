#ifndef BLOCKMETA_H
#define BLOCKMETA_H

#include <stdint.h>

#include <boost/filesystem.hpp>
#include <boost/function.hpp>
#include <cstdio>
#include <limits>
#include <vector>

#include "base/Endian.hpp"
#include "third_party/ulid.hpp"

namespace tsdb {

namespace block {

extern const std::string INDEX_FILE_NAME;
extern const std::string META_FILE_NAME;

class BlockStats {
 public:
  uint64_t num_samples;
  uint64_t num_series;
  uint64_t num_chunks;
  uint64_t num_tombstones;
  uint64_t num_bytes;

  BlockStats()
      : num_samples(0),
        num_series(0),
        num_chunks(0),
        num_tombstones(0),
        num_bytes(0) {}
  bool operator==(const BlockStats &b) const {
    return (b.num_samples == num_samples) && (b.num_series == num_series) &&
           (b.num_chunks == num_chunks) &&
           (b.num_tombstones == num_tombstones) && (b.num_bytes == num_bytes);
  }
};

class BlockDesc {
 public:
  ulid::ULID ulid_;
  int64_t max_time;
  int64_t min_time;

  BlockDesc()
      : ulid_(0),
        max_time(std::numeric_limits<int64_t>::min()),
        min_time(std::numeric_limits<int64_t>::max()) {}
  BlockDesc(const ulid::ULID &ulid_, int64_t min_time, int64_t max_time)
      : ulid_(ulid_), min_time(min_time), max_time(max_time) {}
};

class BlockMetaCompaction {
 public:
  int level;
  std::vector<ulid::ULID> sources;
  std::vector<BlockDesc> parents;
  bool failed;
  bool deletable;

  BlockMetaCompaction() : level(0), failed(0), deletable(0) {}
};

class BlockMeta {
 public:
  ulid::ULID ulid_;
  int64_t max_time;
  int64_t min_time;
  BlockStats stats;
  BlockMetaCompaction compaction;
  int version;

  BlockMeta()
      : ulid_(0),
        max_time(std::numeric_limits<int64_t>::min()),
        min_time(std::numeric_limits<int64_t>::max()),
        version(1) {}
  BlockMeta(const ulid::ULID &ulid_, int64_t min_time, int64_t max_time)
      : ulid_(ulid_), min_time(min_time), max_time(max_time), version(1) {}
};

class BlockMetas {
 private:
  std::deque<BlockMeta> metas;

 public:
  BlockMetas() = default;
  BlockMetas(const std::deque<BlockMeta> &metas) : metas(metas) {}
  BlockMetas(const std::initializer_list<BlockMeta> &metas)
      : metas(metas.begin(), metas.end()) {}

  void push_back(const BlockMeta &meta) { metas.push_back(meta); }

  void pop_back() { metas.pop_back(); }

  void clear() { metas.clear(); }

  int size() const { return metas.size(); }

  void sort_by_min_time() {
    std::sort(metas.begin(), metas.end(),
              [](const BlockMeta &lhs, const BlockMeta &rhs) {
                return lhs.min_time < rhs.min_time;
              });
  }

  BlockMeta &operator[](int i) { return metas[i]; }

  BlockMeta operator[](int i) const { return metas[i]; }

  const BlockMeta &at(int i) const { return metas[i]; }

  const BlockMeta &back() const { return metas.back(); }

  const BlockMeta &front() const { return metas.front(); }
};

class DirMeta {
 public:
  std::string dir;
  std::shared_ptr<BlockMeta> meta;

  DirMeta() = default;
  DirMeta(const std::string &dir, const std::shared_ptr<BlockMeta> &meta)
      : dir(dir), meta(meta) {}
};

class DirMetas {
 private:
  std::deque<DirMeta> metas;

 public:
  DirMetas() = default;
  DirMetas(const std::deque<DirMeta> &metas) : metas(metas) {}
  DirMetas(const std::initializer_list<DirMeta> &metas)
      : metas(metas.begin(), metas.end()) {}

  void push_back(const DirMeta &meta) { metas.push_back(meta); }

  void pop_back() { metas.pop_back(); }

  void clear() { metas.clear(); }

  int size() const { return metas.size(); }

  bool empty() const { return metas.empty(); }

  void sort_by_min_time() {
    std::sort(metas.begin(), metas.end(),
              [](const DirMeta &lhs, const DirMeta &rhs) {
                return lhs.meta->min_time < rhs.meta->min_time;
              });
  }

  DirMeta &operator[](int i) { return metas[i]; }

  DirMeta operator[](int i) const { return metas[i]; }

  const DirMeta &at(int i) const { return metas[i]; }

  const DirMeta &back() const { return metas.back(); }

  const DirMeta &front() const { return metas.front(); }
};

std::pair<BlockMeta, bool> read_block_meta(const std::string &dir);

bool write_block_meta(const std::string &dir, const BlockMeta &meta);

}  // namespace block

}  // namespace tsdb

#endif