#include "db/DBUtils.hpp"

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>

#include "wal/WALUtils.hpp"

namespace tsdb {
namespace db {

bool is_block_dir(const std::string &dir) {
  if (!boost::filesystem::is_directory(boost::filesystem::path(dir)))
    return false;
  if (ulid::Validate(boost::filesystem::path(dir).filename().string(), true))
    return true;
  return false;
}

std::deque<std::string> dirs(const std::string &dir) {
  std::deque<std::string> r;
  boost::filesystem::path p(dir);
  if (!boost::filesystem::exists(p) || !boost::filesystem::is_directory(p))
    return r;
  for (auto const &entry : boost::make_iterator_range(
           boost::filesystem::directory_iterator(p), {})) {
    if (boost::filesystem::is_directory(entry.path()))
      r.push_back(entry.path().string());
  }
  std::sort(r.begin(), r.end());
  return r;
}

std::deque<std::string> block_dirs(const std::string &dir) {
  std::deque<std::string> d = dirs(dir);
  std::deque<std::string> r;
  for (auto const &name : d) {
    if (is_block_dir(name)) r.push_back(name);
  }
  return r;
}

int64_t range_for_timestamp(int64_t t, int64_t width) {
  return (t / width) * width + width;
}

std::vector<int64_t> exponential_block_ranges(int64_t min_size, int step,
                                              int step_size) {
  std::vector<int64_t> r(step);

  for (int i = 0; i < step; ++i) {
    r[i] = min_size;
    min_size *= static_cast<int64_t>(step_size);
  }
  return r;
}

const Options DefaultOptions = Options(
    wal::SEGMENT_SIZE,
    15 * 24 * 60 * 60 * 1000,  // 15 days in milliseconds
    0,
    exponential_block_ranges(2 * 3600 * 1000, 3, 5),  // 2 hours in milliseconds
    false, false);

// overlapping_blocks returns all overlapping blocks from given meta files.
// blocks are sorted by min_time.
TimeRangeOverlaps overlapping_blocks(const block::BlockMetas &bms) {
  TimeRangeOverlaps overlaps;
  if (bms.size() <= 1) return overlaps;

  std::deque<std::deque<int>> o;
  bool chained = false;

  int64_t maxt = bms.at(0).max_time;
  std::deque<int> overlapping;

  // We have here blocks sorted by min_time. We iterate over each block and
  // treat its minTime as our "current" timestamp.
  for (int i = 1; i < bms.size(); ++i) {
    if (bms[i].min_time < maxt) {
      if (!chained) {
        overlapping.push_back(i - 1);
        chained = true;
      }
      overlapping.push_back(i);
    } else
      chained = false;
    if (!chained && !overlapping.empty()) {
      TimeRange t(bms.at(overlapping.front()).min_time, maxt);
      for (int j : overlapping) overlaps.map_[t].push_back(bms.at(j));
      overlapping.clear();
    }
    if (bms[i].max_time > maxt) maxt = bms[i].max_time;
  }
  if (!overlapping.empty()) {
    TimeRange t(bms.at(overlapping.front()).min_time, maxt);
    for (int j : overlapping) overlaps.map_[t].push_back(bms.at(j));
  }

  return overlaps;
}

int exponential(int d, int min, int max) {
  d *= 2;
  if (d < min) d = min;
  if (d > max) d = max;
  return d;
}

void backoff_timing(std::shared_ptr<base::Channel<char>> chan, int sleep_secs) {
  chan->wait_for_seconds(sleep_secs);
  chan->send(0);
}

void del_helper(
    std::shared_ptr<block::BlockInterface> b, int64_t mint, int64_t maxt,
    const std::deque<std::shared_ptr<label::MatcherInterface>> *matchers,
    base::WaitGroup *wg, error::MultiError *multi_err) {
  error::Error err = b->del(mint, maxt, *matchers);
  if (err) multi_err->add(err);
  wg->done();
}

}  // namespace db
}  // namespace tsdb