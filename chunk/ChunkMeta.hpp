#ifndef CHUNKMETA
#define CHUNKMETA

#include <stdint.h>

#include <limits>

#include "base/Checksum.hpp"
#include "chunk/ChunkInterface.hpp"

namespace tsdb {
namespace chunk {

class ChunkMeta {
 public:
  uint64_t ref;  // sequence number(high 32) | offset(low 32)

  // NOTE(Alec): relative offset to the ref in GDC1, series number in GMC1.
  uint64_t series_ref;
  std::unique_ptr<ChunkInterface> chunk;
  int64_t min_time;
  int64_t max_time;
  uint8_t type;  // 0 for GMC1, 1 for GDC1.
  uint64_t logical_group_ref;

  ChunkMeta()
      : min_time(std::numeric_limits<int64_t>::max()),
        max_time(std::numeric_limits<int64_t>::min()) {}
  ChunkMeta(uint64_t ref, int64_t min_time, int64_t max_time)
      : ref(ref), min_time(min_time), max_time(max_time) {}
  ChunkMeta(uint64_t ref, uint64_t series_ref, int64_t min_time,
            int64_t max_time)
      : ref(ref),
        series_ref(series_ref),
        min_time(min_time),
        max_time(max_time) {}
  ChunkMeta(uint64_t ref, std::unique_ptr<ChunkInterface> &&chunk,
            int64_t min_time, int64_t max_time)
      : ref(ref),
        chunk(std::move(chunk)),
        min_time(min_time),
        max_time(max_time) {}
  ChunkMeta(uint64_t ref, uint64_t series_ref,
            std::unique_ptr<ChunkInterface> &&chunk, int64_t min_time,
            int64_t max_time)
      : ref(ref),
        series_ref(series_ref),
        chunk(std::move(chunk)),
        min_time(min_time),
        max_time(max_time) {}
  ChunkMeta(std::unique_ptr<ChunkInterface> &&chunk, int64_t min_time,
            int64_t max_time)
      : chunk(std::move(chunk)), min_time(min_time), max_time(max_time) {}

  int32_t hash() { return base::GetCrc32(chunk->bytes(), chunk->size()); }

  bool overlap_closed(int64_t min_time, int64_t max_time) {
    return min_time <= this->max_time && max_time >= this->min_time;
  }
};

}  // namespace chunk
}  // namespace tsdb

#endif