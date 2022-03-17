#ifndef QUERIERUTILS_H
#define QUERIERUTILS_H

#include <initializer_list>

#include "block/IndexReaderInterface.hpp"
#include "label/MatcherInterface.hpp"
#include "querier/ChunkSeriesSetInterface.hpp"
// #include "querier/GroupChunkSeriesSetInterface.hpp"
#include "querier/SeriesInterface.hpp"
#include "querier/SeriesSetInterface.hpp"

namespace tsdb {
namespace querier {

class Series {
 private:
  std::deque<std::shared_ptr<SeriesInterface>> series;

 public:
  Series() = default;
  Series(const std::deque<std::shared_ptr<SeriesInterface>> &series);

  void push_back(const std::shared_ptr<SeriesInterface> &s);
  void clear();
  std::shared_ptr<SeriesInterface> &operator[](int i);
  std::shared_ptr<SeriesInterface> operator[](int i) const;
  std::shared_ptr<SeriesInterface> &at(int i);
  int size();
  bool empty();
};

class SeriesSets {
 private:
  std::deque<std::shared_ptr<SeriesSetInterface>> ss;

 public:
  SeriesSets() = default;
  SeriesSets(const std::deque<std::shared_ptr<SeriesSetInterface>> &ss);

  void push_back(const std::shared_ptr<SeriesSetInterface> &s);
  void clear();
  void next();
  void next(const std::deque<int> &id);
  std::shared_ptr<SeriesSetInterface> &operator[](int i);
  std::shared_ptr<SeriesSetInterface> operator[](int i) const;
  std::shared_ptr<SeriesSetInterface> &at(int i);
  int size();
  bool empty();
};

class ChunkSeriesSets {
 private:
  std::deque<std::shared_ptr<ChunkSeriesSetInterface>> css;

 public:
  ChunkSeriesSets() = default;
  ChunkSeriesSets(
      const std::deque<std::shared_ptr<ChunkSeriesSetInterface>> &css);

  void push_back(const std::shared_ptr<ChunkSeriesSetInterface> &s);
  void clear();
  void next();
  void next(const std::deque<int> &id);
  std::shared_ptr<ChunkSeriesSetInterface> &operator[](int i);
  std::shared_ptr<ChunkSeriesSetInterface> operator[](int i) const;
  std::shared_ptr<ChunkSeriesSetInterface> &at(int i);
  int size();
  bool empty();
};

// class GroupChunkSeriesSets {
//  private:
//   std::deque<std::shared_ptr<GroupChunkSeriesSetInterface>> css;

//  public:
//   GroupChunkSeriesSets() = default;
//   GroupChunkSeriesSets(
//       const std::deque<std::shared_ptr<GroupChunkSeriesSetInterface>> &css);

//   void push_back(const std::shared_ptr<GroupChunkSeriesSetInterface> &s);
//   void clear();
//   void next();
//   void next(const std::deque<int> &id);
//   std::shared_ptr<GroupChunkSeriesSetInterface> &operator[](int i);
//   std::shared_ptr<GroupChunkSeriesSetInterface> operator[](int i) const;
//   std::shared_ptr<GroupChunkSeriesSetInterface> &at(int i);
//   int size();
//   bool empty();
// };

std::pair<std::unique_ptr<index::PostingsInterface>, bool>
postings_for_unset_label_matcher(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::shared_ptr<label::MatcherInterface> &matcher);

std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings_for_matcher(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::shared_ptr<label::MatcherInterface> &matcher);

// PostingsForMatchers assembles a single postings iterator against the index
// reader based on the given matchers. It returns a list of label names that
// must be manually checked to not exist in series the postings list points to.
std::pair<std::unique_ptr<index::PostingsInterface>, bool>
postings_for_matchers(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::initializer_list<std::shared_ptr<label::MatcherInterface>> &l);

std::pair<std::unique_ptr<index::PostingsInterface>, bool>
postings_for_matchers(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::deque<std::shared_ptr<label::MatcherInterface>> &l);

}  // namespace querier
}  // namespace tsdb

#endif