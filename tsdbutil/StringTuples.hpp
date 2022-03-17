#ifndef STRING_TUPLES_H
#define STRING_TUPLES_H

#include <deque>
#include <string>

namespace tsdb {
namespace tsdbutil {

class StringTuples {
 public:
  std::deque<std::string> entries;
  int length;

  StringTuples() {}
  StringTuples(const std::deque<std::string> &entries, int length)
      : entries(entries), length(length) {}

  int len() { return entries.size() / length; }

  // (Index of the tuple -- not the entry) Should be used inside the length
  std::deque<std::string> at(int i) {
    if (i >= len()) return std::deque<std::string>();
    return std::deque<std::string>(entries.begin() + i * length,
                                   entries.begin() + (i + 1) * length);
  }

  // (Index of the tuple -- not the entry) Should be used inside the length
  void swap(int i, int j) {
    if (i >= len() || j >= len()) return;
    for (int k = 0; k < length; k++) {
      std::swap(entries[i * length + k], entries[j * length + k]);
    }
  }

  // (Index of the tuple -- not the entry) Should be used inside the length
  bool less(int i, int j) {
    for (int k = 0; k < length; k++) {
      int c = entries[i * length + k].compare(entries[j * length + k]);
      if (c < 0)
        return true;
      else if (c > 0)
        return false;
    }
    return false;
  }

  int partition(int low, int high) {
    int pivot = high;   // pivot
    int i = (low - 1);  // Index of smaller element

    for (int j = low; j <= high - 1; j++) {
      // If current element is smaller than or
      // equal to pivot
      if (less(j, pivot)) {
        i++;  // increment index of smaller element
        swap(i, j);
      }
    }
    swap(i + 1, high);
    return (i + 1);
  }

  void quicksort(int low, int high) {
    if (low < high) {
      /* pi is partitioning index, arr[p] is now
               at right place */
      int pi = partition(low, high);

      // Separately sort elements before
      // partition and after partition
      quicksort(low, pi - 1);
      quicksort(pi + 1, high);
    }
  }

  void sort() { quicksort(0, len() - 1); }
};

}  // namespace tsdbutil
}  // namespace tsdb

#endif