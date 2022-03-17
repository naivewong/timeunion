#ifndef DBUTILS_H
#define DBUTILS_H

#include <boost/functional/hash.hpp>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/Channel.hpp"
#include "base/Error.hpp"
#include "base/WaitGroup.hpp"
#include "block/BlockInterface.hpp"
#include "block/BlockUtils.hpp"
#include "label/MatcherInterface.hpp"

#define USE_PROTOBUF 1

namespace tsdb {
namespace db {

bool is_block_dir(const std::string &dir);

std::deque<std::string> dirs(const std::string &dir);

std::deque<std::string> block_dirs(const std::string &dir);

int64_t range_for_timestamp(int64_t t, int64_t width);

std::vector<int64_t> exponential_block_ranges(int64_t min_size, int step,
                                              int step_size);

class Options {
 public:
  // Segments (wal files) max size.
  // wal_segment_size = 0, segment size is default size.
  // wal_segment_size > 0, segment size is WALSegmentSize.
  // wal_segment_size < 0, wal is disabled.
  int wal_segment_size;

  // Duration of persisted data to keep.
  uint64_t retention_duration;

  // Maximum number of bytes in blocks to be retained.
  // 0 or less means disabled.
  // NOTE: For proper storage calculations need to consider
  // the size of the WAL folder which is not added when calculating
  // the current size of the database.
  int64_t max_bytes;

  // The sizes of the Blocks.
  std::vector<int64_t> block_ranges;

  // no_lock_file disables creation and consideration of a lock file.
  bool no_lock_file;

  // Overlapping blocks are allowed if AllowOverlappingBlocks is true.
  // This in-turn enables vertical compaction and vertical query merge.
  bool allow_overlapping_blocks;

  Options()
      : wal_segment_size(0),
        retention_duration(0),
        max_bytes(0),
        no_lock_file(false),
        allow_overlapping_blocks(false) {}
  Options(int wal_segment_size, uint64_t retention_duration, int64_t max_bytes,
          const std::vector<int64_t> &block_ranges, bool no_lock_file,
          bool allow_overlapping_blocks)
      : wal_segment_size(wal_segment_size),
        retention_duration(retention_duration),
        max_bytes(max_bytes),
        block_ranges(block_ranges),
        no_lock_file(no_lock_file),
        allow_overlapping_blocks(allow_overlapping_blocks) {}
};

extern const Options DefaultOptions;

class TimeRange {
 public:
  int64_t min_time;
  int64_t max_time;
  TimeRange() : min_time(0), max_time(0) {}
  TimeRange(int64_t min_time, int64_t max_time)
      : min_time(min_time), max_time(max_time) {}

  bool operator==(const TimeRange &t) const {
    return t.min_time == min_time && t.max_time == max_time;
  }
};

class TimeRangeOverlaps {
 private:
  struct TimeRangeHasher {
    std::size_t operator()(const TimeRange &t) const {
      // Start with a hash value of 0    .
      std::size_t seed = 0;

      // Modify 'seed' by XORing and bit-shifting in
      // one member of 'Key' after the other:
      boost::hash_combine(seed, boost::hash_value(t.min_time));
      boost::hash_combine(seed, boost::hash_value(t.max_time));

      // Return the result.
      return seed;
    }
  };

 public:
  std::unordered_map<TimeRange, std::deque<block::BlockMeta>, TimeRangeHasher>
      map_;

  // str returns human readable string form of overlapped blocks.
  std::string str() {
    std::string s;

    for (auto const &mp : map_) {
      std::string group;
      for (auto const &bm : mp.second) {
        group += "<ulid: " + ulid::Marshal(bm.ulid_) +
                 ", mint: " + std::to_string(bm.min_time) +
                 ", maxt: " + std::to_string(bm.max_time) + ", range: " +
                 std::to_string((bm.max_time - bm.min_time) / 1000) + "s>, ";
      }
      if (!group.empty()) group = group.substr(0, group.length() - 2);
      s += "[mint: " + std::to_string(mp.first.min_time) +
           ", maxt: " + std::to_string(mp.first.max_time) + ", range: " +
           std::to_string((mp.first.max_time - mp.first.min_time) / 1000) +
           "s, " + "blocks: " + std::to_string(mp.second.size()) +
           "]: " + group + "\n";
    }

    if (!s.empty()) return s.substr(0, s.length() - 1);
    return s;
  }

  int size() { return map_.size(); }

  bool empty() { return map_.empty(); }
};

// overlapping_blocks returns all overlapping blocks from given meta files.
TimeRangeOverlaps overlapping_blocks(const block::BlockMetas &bms);

int exponential(int d, int min, int max);

void backoff_timing(std::shared_ptr<base::Channel<char>> chan, int sleep_secs);

void del_helper(
    std::shared_ptr<block::BlockInterface> b, int64_t mint, int64_t maxt,
    const std::deque<std::shared_ptr<label::MatcherInterface>> *matchers,
    base::WaitGroup *wg, error::MultiError *multi_err);

}  // namespace db
}  // namespace tsdb

#endif