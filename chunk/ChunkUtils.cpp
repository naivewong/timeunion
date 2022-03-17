#include "chunk/ChunkUtils.hpp"

#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/range/iterator_range.hpp>
#include <limits>

#include "chunk/XORChunk.hpp"

namespace tsdb {
namespace chunk {

const uint32_t MAGIC_CHUNK = 0x51705259;
const int DEFAULT_CHUNK_SIZE = 512 * 1024 * 1024;
const int CHUNK_FORMAT_V1 = 1;
const uint8_t DEFAULT_MEDIAN_SEGMENT = 2;
const uint8_t DEFAULT_TUPLE_SIZE = 8;

bool is_number(const std::string &s) {
  std::string::const_iterator it = s.begin();
  while (it != s.end() && std::isdigit(*it)) ++it;
  return !s.empty() && it == s.end();
}

std::pair<int, std::string> next_sequence_file(const std::string &dir) {
  // ATTENTION leave one end byte for \0
  int max = -1;
  boost::filesystem::path p(dir);
  for (auto &entry : boost::make_iterator_range(
           boost::filesystem::directory_iterator(p), {})) {
    int temp2 = std::stoi(entry.path().filename().string());
    if (temp2 > max) max = temp2;
  }
  boost::format fmt("%06d");
  fmt % (max + 1);
  return {max + 1, (p / boost::filesystem::path(fmt.str())).string()};
}

// Will get the path including the dir name
std::deque<std::string> sequence_files(const std::string &dir) {
  std::deque<std::string> r;
  boost::filesystem::path p(dir);
  if (!boost::filesystem::exists(p) || !boost::filesystem::is_directory(p))
    return r;
  for (auto const &entry : boost::make_iterator_range(
           boost::filesystem::directory_iterator(p), {})) {
    if (entry.path().filename().string().length() >= 6 &&
        is_number(entry.path().filename().string().substr(
            entry.path().filename().string().length() - 6, 6)))
      r.push_back(entry.path().string());
  }
  std::sort(r.begin(), r.end());
  return r;
}

// merge_chunks vertically merges a and b, i.e., if there is any sample
// with same timestamp in both a and b, the sample in a is discarded.
std::pair<std::unique_ptr<chunk::ChunkInterface>, error::Error> merge_chunks(
    chunk::ChunkInterface *c1, chunk::ChunkInterface *c2) {
  std::unique_ptr<chunk::ChunkInterface> new_chunk =
      std::unique_ptr<chunk::XORChunk>(new chunk::XORChunk());
  std::unique_ptr<chunk::ChunkAppenderInterface> app;
  try {
    app = new_chunk->appender();
  } catch (const base::TSDBException &e) {
    return {std::unique_ptr<chunk::ChunkInterface>(), error::Error(e.what())};
  }

  std::unique_ptr<ChunkIteratorInterface> it1 = c1->iterator();
  std::unique_ptr<ChunkIteratorInterface> it2 = c2->iterator();
  bool ok1 = it1->next();
  bool ok2 = it2->next();
  while (ok1 && ok2) {
    std::pair<int64_t, double> p1 = it1->at();
    std::pair<int64_t, double> p2 = it2->at();
    if (p1.first < p2.first) {
      app->append(p1.first, p1.second);
      ok1 = it1->next();
    } else if (p1.first > p2.first) {
      app->append(p2.first, p2.second);
      ok2 = it2->next();
    } else {
      app->append(p2.first, p2.second);
      ok1 = it1->next();
      ok2 = it2->next();
    }
  }

  while (ok1) {
    std::pair<int64_t, double> p1 = it1->at();
    app->append(p1.first, p1.second);
    ok1 = it1->next();
  }
  while (ok2) {
    std::pair<int64_t, double> p2 = it2->at();
    app->append(p2.first, p2.second);
    ok2 = it2->next();
  }

  if (it1->error())
    return {std::unique_ptr<chunk::ChunkInterface>(),
            error::Error("Error in first chunk iterator")};
  if (it2->error())
    return {std::unique_ptr<chunk::ChunkInterface>(),
            error::Error("Error in second chunk iterator")};
  return {std::move(new_chunk), error::Error()};
}

// merge_overlapping_chunks removes the samples whose timestamp is overlapping.
// The last appearing sample is retained in case there is overlapping.
// This assumes that `chunks` are sorted w.r.t. min_time.
std::pair<std::deque<std::shared_ptr<ChunkMeta>>, error::Error>
merge_overlapping_chunks(const std::deque<std::shared_ptr<ChunkMeta>> &chunks) {
  if (chunks.size() < 2) return {chunks, error::Error()};

  std::deque<std::shared_ptr<ChunkMeta>> new_chunks;
  new_chunks.push_back(chunks[0]);
  int last = 0;
  for (int i = 1; i < chunks.size(); i++) {
    // We need to check only the last chunk in newChks.
    // Reason: (1) newChks[last-1].MaxTime < newChks[last].MinTime (non
    // overlapping).
    //         (2) As chks are sorted w.r.t. MinTime, newChks[last].MinTime <
    //         c.MinTime.
    // So never overlaps with newChks[last-1] or anything before that.
    if (chunks[i]->min_time > new_chunks[last]->max_time) {
      new_chunks.push_back(chunks[i]);
      ++last;
      continue;
    }
    if (new_chunks[last]->max_time < chunks[i]->max_time)
      new_chunks[last]->max_time = chunks[i]->max_time;

    std::pair<std::unique_ptr<chunk::ChunkInterface>, error::Error> chk =
        merge_chunks(new_chunks[last]->chunk.get(), chunks[i]->chunk.get());
    if (chk.second)
      return {std::deque<std::shared_ptr<ChunkMeta>>(), chk.second};
    new_chunks[last]->chunk = std::move(chk.first);
  }

  return {new_chunks, error::Error()};
}

}  // namespace chunk
}  // namespace tsdb