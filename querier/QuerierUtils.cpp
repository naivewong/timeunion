#include "querier/QuerierUtils.hpp"

#include "base/Logging.hpp"
#include "block/BlockUtils.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IntersectPostings.hpp"
#include "index/SubtractionPostings.hpp"
#include "index/UnionPostings.hpp"
#include "tsdbutil/StringTuplesInterface.hpp"

namespace tsdb {
namespace querier {

Series::Series(const std::deque<std::shared_ptr<SeriesInterface>> &series)
    : series(series) {}

void Series::push_back(const std::shared_ptr<SeriesInterface> &s) {
  series.push_back(s);
}

void Series::clear() { series.clear(); }

std::shared_ptr<SeriesInterface> &Series::operator[](int i) {
  return series[i];
}

std::shared_ptr<SeriesInterface> Series::operator[](int i) const {
  return series[i];
}

std::shared_ptr<SeriesInterface> &Series::at(int i) { return series[i]; }

int Series::size() { return series.size(); }

bool Series::empty() { return series.empty(); }

SeriesSets::SeriesSets(
    const std::deque<std::shared_ptr<SeriesSetInterface>> &ss)
    : ss(ss) {}

void SeriesSets::push_back(const std::shared_ptr<SeriesSetInterface> &s) {
  ss.push_back(s);
}

void SeriesSets::clear() { ss.clear(); }

void SeriesSets::next() {
  std::deque<std::shared_ptr<SeriesSetInterface>>::iterator it = ss.begin();
  while (it != ss.end()) {
    if (!(*it)->next())
      it = ss.erase(it);
    else
      ++it;
  }
}

void SeriesSets::next(const std::deque<int> &id) {
  int d = 0;
  for (auto const &i : id) {
    if (i - d < ss.size()) {
      std::deque<std::shared_ptr<SeriesSetInterface>>::iterator it =
          ss.begin() + i - d;
      if (!(*it)->next()) {
        ss.erase(it);
        ++d;
      }
    }
  }
}

std::shared_ptr<SeriesSetInterface> &SeriesSets::operator[](int i) {
  return ss[i];
}

std::shared_ptr<SeriesSetInterface> SeriesSets::operator[](int i) const {
  return ss[i];
}

std::shared_ptr<SeriesSetInterface> &SeriesSets::at(int i) { return ss[i]; }

int SeriesSets::size() { return ss.size(); }

bool SeriesSets::empty() { return ss.empty(); }

ChunkSeriesSets::ChunkSeriesSets(
    const std::deque<std::shared_ptr<ChunkSeriesSetInterface>> &css)
    : css(css) {}

void ChunkSeriesSets::push_back(
    const std::shared_ptr<ChunkSeriesSetInterface> &s) {
  css.push_back(s);
}

void ChunkSeriesSets::clear() { css.clear(); }

void ChunkSeriesSets::next() {
  std::deque<std::shared_ptr<ChunkSeriesSetInterface>>::iterator it =
      css.begin();
  while (it != css.end()) {
    if (!(*it)->next())
      it = css.erase(it);
    else
      ++it;
  }
}

void ChunkSeriesSets::next(const std::deque<int> &id) {
  int d = 0;
  for (auto const &i : id) {
    if (i - d < css.size()) {
      std::deque<std::shared_ptr<ChunkSeriesSetInterface>>::iterator it =
          css.begin() + i - d;
      if (!(*it)->next()) {
        css.erase(it);
        ++d;
      }
    }
  }
}

std::shared_ptr<ChunkSeriesSetInterface> &ChunkSeriesSets::operator[](int i) {
  return css[i];
}

std::shared_ptr<ChunkSeriesSetInterface> ChunkSeriesSets::operator[](
    int i) const {
  return css[i];
}

std::shared_ptr<ChunkSeriesSetInterface> &ChunkSeriesSets::at(int i) {
  return css[i];
}

int ChunkSeriesSets::size() { return css.size(); }

bool ChunkSeriesSets::empty() { return css.empty(); }

// GroupChunkSeriesSets::GroupChunkSeriesSets(
//     const std::deque<std::shared_ptr<GroupChunkSeriesSetInterface>> &css)
//     : css(css) {}

// void GroupChunkSeriesSets::push_back(
//     const std::shared_ptr<GroupChunkSeriesSetInterface> &s) {
//   css.push_back(s);
// }

// void GroupChunkSeriesSets::clear() { css.clear(); }

// void GroupChunkSeriesSets::next() {
//   std::deque<std::shared_ptr<GroupChunkSeriesSetInterface>>::iterator it =
//       css.begin();
//   while (it != css.end()) {
//     if (!(*it)->next())
//       it = css.erase(it);
//     else
//       ++it;
//   }
// }

// void GroupChunkSeriesSets::next(const std::deque<int> &id) {
//   int d = 0;
//   for (auto const &i : id) {
//     if (i - d < css.size()) {
//       std::deque<std::shared_ptr<GroupChunkSeriesSetInterface>>::iterator it
//       =
//           css.begin() + i - d;
//       if (!(*it)->next()) {
//         css.erase(it);
//         ++d;
//       }
//     }
//   }
// }

// std::shared_ptr<GroupChunkSeriesSetInterface>
// &GroupChunkSeriesSets::operator[](
//     int i) {
//   return css[i];
// }

// std::shared_ptr<GroupChunkSeriesSetInterface>
// GroupChunkSeriesSets::operator[](
//     int i) const {
//   return css[i];
// }

// std::shared_ptr<GroupChunkSeriesSetInterface> &GroupChunkSeriesSets::at(int
// i) {
//   return css[i];
// }

// int GroupChunkSeriesSets::size() { return css.size(); }

// bool GroupChunkSeriesSets::empty() { return css.empty(); }

std::pair<std::unique_ptr<index::PostingsInterface>, bool>
postings_for_unset_label_matcher(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::shared_ptr<label::MatcherInterface> &matcher) {
  // All the corresponding label values
  std::unique_ptr<tsdbutil::StringTuplesInterface> vtpls =
      ir->label_values({matcher->name()});
  if (vtpls->len() == 0)
    return std::make_pair(
        std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
        false);

  // First collect all those not matched label values
  std::deque<std::string> not_matches;
  for (int i = 0; i < vtpls->len(); i++) {
    std::string val = vtpls->at(i);
    if (!matcher->match(val)) not_matches.push_back(val);
  }

  // Second collect all those not matched postings
  std::deque<std::shared_ptr<index::PostingsInterface>> not_matched_postings;
  for (auto const &value : not_matches) {
    std::pair<std::unique_ptr<index::PostingsInterface>, bool> p =
        ir->postings(matcher->name(), value);
    if (!p.second)
      return {
          std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
          false};
    not_matched_postings.push_back(std::move(p.first));
  }

  std::pair<std::unique_ptr<index::PostingsInterface>, bool> all = ir->postings(
      label::ALL_POSTINGS_KEYS.label, label::ALL_POSTINGS_KEYS.value);
  if (!all.second)
    return {
        std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
        false};

  return {index::without_u(std::move(all.first),
                           std::move(merge_s(not_matched_postings))),
          true};
}

std::pair<std::unique_ptr<index::PostingsInterface>, bool> postings_for_matcher(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::shared_ptr<label::MatcherInterface> &matcher) {
  // If the matcher selects an empty value, it selects all the series which
  // don't have the label name set too. For NotMatcher.
  if (matcher->match("")) return postings_for_unset_label_matcher(ir, matcher);

  std::deque<std::string> matches;
  if (matcher->class_name() != "equal") {
    // All the corresponding label values
    std::unique_ptr<tsdbutil::StringTuplesInterface> vtpls =
        ir->label_values({matcher->name()});
    if (!vtpls || vtpls->len() == 0) {
      return {
          std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
          false};
    }

    // First collect all those matched label values
    for (int i = 0; i < vtpls->len(); i++) {
      std::string val = vtpls->at(i);
      // LOG_DEBUG << val;
      if (matcher->match(val)) matches.push_back(val);
    }
    if (matches.size() == 0)
      return {
          std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
          true};
  } else {
    matches.push_back(matcher->value());
  }

  // Second collect all those matched postings
  std::deque<std::shared_ptr<index::PostingsInterface>> matched_postings;
  for (auto const &value : matches) {
    // Will check if the matcher->name() exists in the index here.
    // LOG_DEBUG << matcher->name() << " " << value;
    // {
    //     std::pair<std::unique_ptr<index::PostingsInterface>, bool> p =
    //     ir->postings(matcher->name(), value); while(p.first->next())
    //         LOG_DEBUG << p.first->at();
    // }
    std::pair<std::unique_ptr<index::PostingsInterface>, bool> p =
        ir->postings(matcher->name(), value);
    if (!p.second) {
      // LOG_DEBUG << "matcher->name:" << matcher->name() << " value:" << value;
      return {
          std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
          false};
    }
    matched_postings.push_back(std::move(p.first));
  }

  return {index::merge_u(matched_postings), true};
}

// PostingsForMatchers assembles a single postings iterator against the index
// reader based on the given matchers. It returns a list of label names that
// must be manually checked to not exist in series the postings list points to.
std::pair<std::unique_ptr<index::PostingsInterface>, bool>
postings_for_matchers(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::initializer_list<std::shared_ptr<label::MatcherInterface>> &l) {
  // deque of unique_ptr of Postings.
  // NOTICE
  // This is a local variable, should std::move all the elements before return.
  std::deque<std::unique_ptr<index::PostingsInterface>> r;
  for (auto const &matcher : l) {
    // LOG_DEBUG << matcher->name();
    std::pair<std::unique_ptr<index::PostingsInterface>, bool> p =
        postings_for_matcher(ir, matcher);
    if (!p.second)
      return {
          std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
          false};
    r.emplace_back(std::move(p.first));
  }
  // Intersection
  return {ir->sorted_postings(std::move(index::intersect_u(std::move(r)))),
          true};
}
std::pair<std::unique_ptr<index::PostingsInterface>, bool>
postings_for_matchers(
    const std::shared_ptr<block::IndexReaderInterface> &ir,
    const std::deque<std::shared_ptr<label::MatcherInterface>> &l) {
  // deque of unique_ptr of Postings.
  // NOTICE
  // This is a local variable, should std::move all the elements before return.
  std::deque<std::unique_ptr<index::PostingsInterface>> r;
  for (auto const &matcher : l) {
    // LOG_DEBUG << matcher->name();
    std::pair<std::unique_ptr<index::PostingsInterface>, bool> p =
        postings_for_matcher(ir, matcher);
    if (!p.second)
      return {
          std::unique_ptr<index::PostingsInterface>(new index::EmptyPostings()),
          false};
    r.emplace_back(std::move(p.first));
  }
  // Intersection
  return {ir->sorted_postings(std::move(index::intersect_u(std::move(r)))),
          true};
}

}  // namespace querier
}  // namespace tsdb