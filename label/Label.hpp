#ifndef LABEL_H
#define LABEL_H

#include <stdint.h>

#include <boost/functional/hash.hpp>
#include <deque>
#include <initializer_list>
#include <string>
#include <unordered_map>

namespace tsdb {
namespace label {

extern const std::string SEP;
extern const uint64_t XXHASH_SEED;
extern std::string HEAD_LABEL_SEP;

class Label {
 public:
  std::string label;
  std::string value;

  Label() = default;

  Label(const std::initializer_list<std::string> &l);

  Label(const std::string &label, const std::string &value);

  bool operator<(const Label &l2) const;
  bool operator<=(const Label &l2) const;
  bool operator==(const Label &l2) const;
};

struct LabelHasher {
  std::size_t operator()(const Label &l) const {
    // Start with a hash value of 0    .
    std::size_t seed = 0;

    // Modify 'seed' by XORing and bit-shifting in
    // one member of 'Key' after the other:
    boost::hash_combine(seed, boost::hash_value(l.label));
    boost::hash_combine(seed, boost::hash_value(l.value));

    // Return the result.
    return seed;
  }
};

typedef std::deque<Label> Labels;

Labels lbs_add(const Labels &lset, const Label &l);
Labels lbs_del(const Labels &lset, const std::string &name);

std::string lbs_string(const Labels &lbs);

std::unordered_map<std::string, std::string> lbs_map(const Labels &lbs);

uint64_t lbs_hash(const Labels &lbs);
uint64_t lbs_hash_group(const std::deque<Labels> &labels);
uint64_t lbs_hash_for_labels(const Labels &lbs,
                             const std::deque<std::string> &names);
uint64_t lbs_hash_without_labels(const Labels &lbs,
                                 const std::deque<std::string> &names);

Labels lbs_from_string(const std::initializer_list<std::string> &list);

Labels lbs_from_map(const std::unordered_map<std::string, std::string> &m);

int lbs_compare(const Labels &lbs1, const Labels &lbs2);

Labels random_labels(int num);

extern const Label ALL_POSTINGS_KEYS;

}  // namespace label
}  // namespace tsdb

#endif