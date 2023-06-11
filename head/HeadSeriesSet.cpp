#include "head/HeadSeriesSet.hpp"

#include "chunk/XORIterator.hpp"
#include "index/EmptyPostings.hpp"
#include "index/IntersectPostings.hpp"

namespace tsdb {
namespace head {

/**********************************************
 *                 HeadIterator               *
 **********************************************/
HeadIterator::HeadIterator(std::string* s, int64_t mint, int64_t maxt)
    : chunk_(reinterpret_cast<const uint8_t*>(s->data()), s->size()),
      end_(false),
      min_time_(mint),
      max_time_(maxt),
      init_(false) {
  iter_ = chunk_.xor_iterator();
}

bool HeadIterator::seek(int64_t t) const {
  if (t > max_time_) return false;
  if (!init_) {
    init_ = true;
    while (true) {
      if (!iter_->next()) {
        end_ = true;
        break;
      }
      if (iter_->at().first > max_time_) {
        end_ = true;
        break;
      } else if (iter_->at().first >= min_time_)
        break;
    }
  }
  if (end_) return false;
  while (iter_->next()) {
    if (iter_->at().first > max_time_) {
      end_ = true;
      return false;
    } else if (iter_->at().first >= t)
      return true;
  }
  end_ = true;
  return false;
}

std::pair<int64_t, double> HeadIterator::at() const {
  if (end_) return {0, 0};
  return iter_->at();
}

bool HeadIterator::next() const {
  if (!init_) {
    init_ = true;
    while (true) {
      if (!iter_->next()) {
        end_ = true;
        break;
      }
      if (iter_->at().first > max_time_) {
        end_ = true;
        break;
      } else if (iter_->at().first >= min_time_)
        break;
    }
    return !end_;
  }
  if (end_) return false;
  end_ = !iter_->next();
  if (!end_ && iter_->at().first > max_time_) end_ = true;
  return !end_;
}

}  // namespace head
}  // namespace tsdb