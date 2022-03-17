#ifndef SERIESITERATORINTERFACE_H
#define SERIESITERATORINTERFACE_H

#include <stdint.h>

#include <utility>
#include <vector>

namespace tsdb {
namespace querier {

class SeriesIteratorInterface {
 public:
  virtual bool seek(int64_t t) const = 0;
  virtual std::pair<int64_t, double> at() const = 0;
  virtual bool next() const = 0;
  virtual bool error() const = 0;
  virtual ~SeriesIteratorInterface() = default;
};

class EmptySeriesIterator : public SeriesIteratorInterface {
 public:
  bool seek(int64_t t) const { return false; }
  std::pair<int64_t, double> at() const { return {0, 0}; }
  bool next() const { return false; }
  bool error() const { return false; }
};

class MergeSeriesIterator : public SeriesIteratorInterface {
 private:
  mutable std::vector<SeriesIteratorInterface*> iters_;
  mutable std::vector<int> id_;
  bool err_;
  mutable int64_t t_;
  mutable double v_;
  mutable bool init_;

  void init() const {
    std::vector<SeriesIteratorInterface*>::iterator it = iters_.begin();
    while (it != iters_.end()) {
      if (!(*it)->next()) {
        delete *it;
        it = iters_.erase(it);
      } else
        ++it;
    }
  }

  void _next() const {
    int d = 0;
    for (auto const& i : id_) {
      if (i - d < iters_.size()) {
        std::vector<SeriesIteratorInterface*>::iterator it =
            iters_.begin() + i - d;
        if (!(*it)->next()) {
          delete *it;
          iters_.erase(it);
          ++d;
        }
      }
    }
  }

 public:
  MergeSeriesIterator() : err_(false), init_(false) {}
  MergeSeriesIterator(const std::vector<SeriesIteratorInterface*>& iters)
      : iters_(iters), err_(false), init_(false) {}
  ~MergeSeriesIterator() {
    for (size_t i = 0; i < iters_.size(); i++) delete iters_[i];
  }

  void push_back(SeriesIteratorInterface* it) { iters_.push_back(it); }

  bool seek(int64_t t) const {
    if (iters_.empty()) return false;

    init_ = true;
    std::vector<SeriesIteratorInterface*>::iterator it = iters_.begin();
    while (it != iters_.end()) {
      if (!(*it)->seek(t)) {
        delete *it;
        it = iters_.erase(it);
      } else
        ++it;
    }

    if (iters_.empty()) return false;

    id_.clear();
    id_.push_back(0);
    t_ = iters_[0]->at().first;
    v_ = iters_[0]->at().second;
    for (int i = 1; i < iters_.size(); i++) {
      if (iters_[i]->at().first < t_) {
        id_.clear();
        id_.push_back(i);
        t_ = iters_[i]->at().first;
        v_ = iters_[i]->at().second;
      } else if (iters_[i]->at().first == t_) {
        id_.push_back(i);
        v_ = iters_[i]->at().second;
      }
    }
    return true;
  }

  bool next() const {
    if (iters_.empty()) return false;

    if (!init_) {
      init();
      init_ = true;
    }

    _next();
    if (iters_.empty()) return false;

    id_.clear();
    id_.push_back(0);
    t_ = iters_[0]->at().first;
    v_ = iters_[0]->at().second;
    for (int i = 1; i < iters_.size(); i++) {
      if (iters_[i]->at().first < t_) {
        id_.clear();
        id_.push_back(i);
        t_ = iters_[i]->at().first;
        v_ = iters_[i]->at().second;
      } else if (iters_[i]->at().first == t_) {
        id_.push_back(i);
        v_ = iters_[i]->at().second;
      }
    }
    return true;
  }

  std::pair<int64_t, double> at() const { return {t_, v_}; }

  bool error() const { return err_; }
};

}  // namespace querier
}  // namespace tsdb

#endif