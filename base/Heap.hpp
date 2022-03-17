#ifndef BASE_HEAP_H
#define BASE_HEAP_H

#include <deque>

namespace tsdb {
namespace base {

template <typename T>
class MinHeap {
 private:
  std::deque<T> samples;
  int size;

  int parent(int i) { return (i - 1) / 2; }
  // to get index of left child of node at index i
  int left(int i) { return (2 * i + 1); }
  // to get index of right child of node at index i
  int right(int i) { return (2 * i + 2); }
  // A recursive method to heapify a subtree with the root at given index
  // This method assumes that the subtrees are already heapified
  void min_heapify(int i) {
    int l = left(i);
    int r = right(i);
    int smallest = i;
    if (l < samples.size() && samples[l] < samples[i]) smallest = l;
    if (r < samples.size() && samples[r] < samples[smallest]) smallest = r;
    if (smallest != i) {
      std::swap(samples[i], samples[smallest]);
      min_heapify(smallest);
    }
  }

 public:
  MinHeap(int k) : size(k) {}
  // This is to find the kth smallest element.
  MinHeap(const std::deque<T> &data, int k) : size(k) {
    if (k > data.size()) return;
    for (int j = 0; j < k; ++j) {
      samples.push_back(data[j]);
      int i = samples.size() - 1;

      // Fix the min heap property if it is violated
      while (i != 0 && samples[parent(i)] > samples[i]) {
        std::swap(samples[i], samples[parent(i)]);
        i = parent(i);
      }
    }
    for (int i = k; i < data.size(); ++i) {
      if (data[i] > samples.front()) {
        samples[0] = data[i];
        min_heapify(0);
      }
    }
  }
  void push(const T &sample) {
    samples.push_back(sample);
    int i = samples.size() - 1;

    // Fix the min heap property if it is violated
    while (i != 0 && samples[parent(i)] > samples[i]) {
      std::swap(samples[i], samples[parent(i)]);
      i = parent(i);
    }
    if (samples.size() > size) samples.pop_back();
  }
  // NOTE(Alec): Only call when not empty.
  T pop() {
    if (samples.size() == 1) {
      T s = samples.front();
      samples.pop_front();
      return s;
    }

    // Store the minimum value, and remove it from heap
    T root = samples[0];
    samples[0] = samples.back();
    samples.pop_back();
    min_heapify(0);
    return root;
  }
  T front() { return samples.front(); }
};

template <typename T>
class MaxHeap {
 private:
  std::deque<T> samples;
  int size;

  int parent(int i) { return (i - 1) / 2; }
  // to get index of left child of node at index i
  int left(int i) { return (2 * i + 1); }
  // to get index of right child of node at index i
  int right(int i) { return (2 * i + 2); }
  // A recursive method to heapify a subtree with the root at given index
  // This method assumes that the subtrees are already heapified
  void max_heapify(int i) {
    int l = left(i);
    int r = right(i);
    int smallest = i;
    if (l < samples.size() && samples[l] > samples[i]) smallest = l;
    if (r < samples.size() && samples[r] > samples[smallest]) smallest = r;
    if (smallest != i) {
      std::swap(samples[i], samples[smallest]);
      max_heapify(smallest);
    }
  }

 public:
  MaxHeap(int k) : size(k) {}
  // This is to find the kth largest element.
  MaxHeap(const std::deque<T> &data, int k) : size(k) {
    if (k > data.size()) return;
    for (int j = 0; j < k; ++j) {
      samples.push_back(data[j]);
      int i = samples.size() - 1;

      // Fix the min heap property if it is violated
      while (i != 0 && samples[parent(i)] < samples[i]) {
        std::swap(samples[i], samples[parent(i)]);
        i = parent(i);
      }
    }
    for (int i = k; i < data.size(); ++i) {
      if (data[i] < samples.front()) {
        samples[0] = data[i];
        max_heapify(0);
      }
    }
  }
  void push(const T &sample) {
    samples.push_back(sample);
    int i = samples.size() - 1;

    // Fix the min heap property if it is violated
    while (i != 0 && samples[parent(i)] < samples[i]) {
      std::swap(samples[i], samples[parent(i)]);
      i = parent(i);
    }
    if (samples.size() > size) samples.pop_back();
  }
  // NOTE(Alec): Only call when not empty.
  T pop() {
    if (samples.size() == 1) {
      T s = samples.front();
      samples.pop_front();
      return s;
    }

    // Store the minimum value, and remove it from heap
    T root = samples[0];
    samples[0] = samples.back();
    samples.pop_back();
    max_heapify(0);
    return root;
  }
  T front() { return samples.front(); }
};

}  // namespace base
}  // namespace tsdb

#endif