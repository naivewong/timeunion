#ifndef CHUNKUTILS_H
#define CHUNKUTILS_H

#include <deque>

#include "base/Error.hpp"
#include "chunk/ChunkInterface.hpp"
#include "chunk/ChunkMeta.hpp"
// #include "querier/ChunkSeriesMeta.hpp"

namespace tsdb {
namespace chunk {

extern const uint32_t MAGIC_CHUNK;
extern const int DEFAULT_CHUNK_SIZE;
extern const int CHUNK_FORMAT_V1;
extern const uint8_t
    DEFAULT_MEDIAN_SEGMENT;  // NOTE(Alec): should be larger than one.
extern const uint8_t DEFAULT_TUPLE_SIZE;

bool is_number(const std::string &s);

std::pair<int, std::string> next_sequence_file(const std::string &dir);

// Will get the path including the dir name
std::deque<std::string> sequence_files(const std::string &dir);

// merge_chunks vertically merges a and b, i.e., if there is any sample
// with same timestamp in both a and b, the sample in a is discarded.
std::pair<std::unique_ptr<chunk::ChunkInterface>, error::Error> merge_chunks(
    chunk::ChunkInterface *c1, chunk::ChunkInterface *c2);

// merge_overlapping_chunks removes the samples whose timestamp is overlapping.
// The last appearing sample is retained in case there is overlapping.
// This assumes that `chunks` are sorted w.r.t. min_time.
std::pair<std::deque<std::shared_ptr<ChunkMeta>>, error::Error>
merge_overlapping_chunks(const std::deque<std::shared_ptr<ChunkMeta>> &chunks);

}  // namespace chunk
}  // namespace tsdb

#endif