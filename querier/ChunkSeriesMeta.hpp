#ifndef CHUNKSERIESMETA_H
#define CHUNKSERIESMETA_H

#include <algorithm>

#include "chunk/ChunkMeta.hpp"
#include "label/Label.hpp"
#include "tombstone/Interval.hpp"

namespace tsdb {
namespace querier {

class ChunkSeriesMeta {
 public:
  label::Labels lset;
  std::deque<std::shared_ptr<chunk::ChunkMeta>> chunks;
  tombstone::Intervals intervals;

  ChunkSeriesMeta() = default;
  ChunkSeriesMeta(label::Labels &&lset,
                  std::deque<std::shared_ptr<chunk::ChunkMeta>> &&chunks,
                  tombstone::Intervals &&intervals)
      : lset(std::move(lset)),
        chunks(std::move(chunks)),
        intervals(std::move(intervals)) {}

  void clear() {
    lset.clear();
    chunks.clear();
    intervals.clear();
  }

  void sort_by_min_time() {
    std::sort(chunks.begin(), chunks.end(),
              [](const std::shared_ptr<chunk::ChunkMeta> &lhs,
                 const std::shared_ptr<chunk::ChunkMeta> &rhs) {
                return lhs->min_time < rhs->min_time;
              });
  }
};

class GroupChunkSeriesMeta {
 public:
  std::deque<label::Labels> lset;
  // Each row represents one series.
  std::deque<std::deque<std::shared_ptr<chunk::ChunkMeta>>> chunks;
  // The intervals are for the whole group.
  tombstone::Intervals intervals;

  GroupChunkSeriesMeta() = default;

  void clear() {
    lset.clear();
    chunks.clear();
    intervals.clear();
  }

  std::deque<std::shared_ptr<chunk::ChunkMeta>> get_column(int i) {
    if (chunks.empty() || i >= chunks[0].size())
      return std::deque<std::shared_ptr<chunk::ChunkMeta>>();
    std::deque<std::shared_ptr<chunk::ChunkMeta>> r;
    for (int j = 0; j < chunks.size(); ++j) r.push_back(chunks[j][i]);
    return r;
  }

  // Pop a column.
  void pop_chunks() {
    for (int i = 0; i < chunks.size(); ++i) chunks[i].pop_front();
  }

  // Push a column.
  void push_chunks(int num) {
    for (int i = 0; i < num; ++i)
      chunks.push_back(std::deque<std::shared_ptr<chunk::ChunkMeta>>());
  }

  // TODO(Alec), more efficient.
  void sort_by_min_time() {
    for (int i = 0; i < chunks.size(); ++i) {
      std::sort(chunks[i].begin(), chunks[i].end(),
                [](const std::shared_ptr<chunk::ChunkMeta> &lhs,
                   const std::shared_ptr<chunk::ChunkMeta> &rhs) {
                  return lhs->min_time < rhs->min_time;
                });
    }
  }
};

}  // namespace querier
}  // namespace tsdb

#endif