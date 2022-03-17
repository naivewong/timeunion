#include "label/Label.hpp"

#include <stdlib.h> /* srand, rand */

#include <algorithm>

#include "third_party/xxhash.h"

namespace tsdb {
namespace label {

const std::string SEP = "\xff";
const uint64_t XXHASH_SEED = 0;
// TODO(Alec).
std::string HEAD_LABEL_SEP = "?";

Label::Label(const std::initializer_list<std::string> &l) {
  if (l.size() == 2) {
    label = *l.begin();
    value = *(l.begin() + 1);
  }
}

Label::Label(const std::string &label, const std::string &value)
    : label(label), value(value) {}

bool Label::operator<(const Label &l2) const {
  int c = label.compare(l2.label);
  if (c > 0)
    return false;
  else if (c < 0)
    return true;
  else {
    int c1 = value.compare(l2.value);
    if (c1 < 0)
      return true;
    else
      return false;
  }
}

bool Label::operator<=(const Label &l2) const {
  int c = label.compare(l2.label);
  if (c > 0)
    return false;
  else if (c < 0)
    return true;
  else {
    int c1 = value.compare(l2.value);
    if (c1 <= 0)
      return true;
    else
      return false;
  }
}

bool Label::operator==(const Label &l2) const {
  return (label.compare(l2.label) == 0) && (value.compare(l2.value) == 0);
}

Labels lbs_add(const Labels &lset, const Label &l) {
  for (int i = 0; i < lset.size(); ++i) {
    if (lset[i].label == l.label) return lset;
  }
  Labels r(lset);
  r.push_back(l);
  std::sort(r.begin(), r.end());
  return r;
}
Labels lbs_del(const Labels &lset, const std::string &name) {
  Labels r;
  for (int i = 0; i < lset.size(); ++i) {
    if (lset[i].label != name) r.emplace_back(lset[i].label, lset[i].value);
  }
  return r;
}

std::string lbs_string(const Labels &lbs) {
  std::string r = "{";
  for (int i = 0; i < lbs.size(); i++) {
    if (i > 0) r += ",";
    r += lbs[i].label + "=" + lbs[i].value;
  }
  r += "}";
  return r;
}

std::unordered_map<std::string, std::string> lbs_map(const Labels &lbs) {
  std::unordered_map<std::string, std::string> r;
  for (Label l : lbs) r.insert({l.label, l.value});
  return r;
}

uint64_t lbs_hash(const Labels &lbs) {
  std::string input;
  for (const Label &l : lbs) {
    input += l.label;
    input += SEP;
    input += l.value;
    input += SEP;
  }
  return ::XXH64(input.c_str(), input.size(), XXHASH_SEED);
}
uint64_t lbs_hash_group(const std::deque<Labels> &labels) {
  std::string input;
  for (const Labels &lbs : labels) {
    for (const Label &l : lbs) {
      input += l.label;
      input += SEP;
      input += l.value;
      input += SEP;
    }
  }
  return ::XXH64(input.c_str(), input.size(), XXHASH_SEED);
}
uint64_t lbs_hash_for_labels(const Labels &lbs,
                             const std::deque<std::string> &names) {
  std::string input;
  for (auto const &l : lbs) {
    for (auto const &name : names) {
      if (l.label == name) {
        input += l.label;
        input += SEP;
        input += l.value;
        input += SEP;
        break;
      }
    }
  }
  return ::XXH64(input.c_str(), input.size(), XXHASH_SEED);
}
uint64_t lbs_hash_without_labels(const Labels &lbs,
                                 const std::deque<std::string> &names) {
  std::string input;
  for (auto const &l : lbs) {
    if (l.label == "__name__") continue;
    bool found = false;
    for (auto const &name : names) {
      if (l.label == name) {
        found = true;
        break;
      }
    }
    if (found) continue;
    input += l.label;
    input += SEP;
    input += l.value;
    input += SEP;
  }
  return ::XXH64(input.c_str(), input.size(), XXHASH_SEED);
}

Labels lbs_from_string(const std::initializer_list<std::string> &list) {
  Labels r;
  if (list.size() % 2 != 0) return r;
  std::initializer_list<std::string>::iterator it = list.begin();
  while (it != list.end()) {
    r.push_back(Label(*(it), *(it + 1)));
    ++it;
    ++it;
  }
  std::sort(r.begin(), r.end());
  return r;
}

Labels lbs_from_map(const std::unordered_map<std::string, std::string> &m) {
  Labels r;
  for (std::unordered_map<std::string, std::string>::const_iterator it =
           m.cbegin();
       it != m.cend(); it++) {
    r.push_back(Label(it->first, it->second));
  }
  std::sort(r.begin(), r.end());
  return r;
}

int lbs_compare(const Labels &lbs1, const Labels &lbs2) {
  int len = (lbs1.size() < lbs2.size() ? lbs1.size() : lbs2.size());
  for (int i = 0; i < len; i++) {
    int c = lbs1[i].label.compare(lbs2[i].label);
    if (c != 0) return c;
    c = lbs1[i].value.compare(lbs2[i].value);
    if (c != 0) return c;
  }
  return lbs1.size() - lbs2.size();
}

Labels random_labels(int num) {
  std::unordered_map<std::string, std::string> m;
  for (int i = 0; i < num; ++i) {
    int len1 = rand() % 25 + 1;
    int len2 = rand() % 25 + 1;
    std::string label(len1, 'a');
    std::string value(len2, 'a');
    for (int j = 0; j < len1; ++j) label[j] = 'a' + (rand() % 26);
    for (int j = 0; j < len2; ++j) value[j] = 'a' + (rand() % 26);
    m[label] = value;
  }
  return lbs_from_map(m);
}

const Label ALL_POSTINGS_KEYS = Label("ALL_POSTINGS", "ALL_POSTINGS");

}  // namespace label
}  // namespace tsdb