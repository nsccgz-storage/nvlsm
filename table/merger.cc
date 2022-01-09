// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// #include <unique_ptr.h>
#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "util/heap.h"


namespace leveldb {

namespace {
  class MaxIteratorComparator {
 public:
  MaxIteratorComparator(const Comparator* comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) const {
    return comparator_->Compare(a->key(), b->key()) < 0;
  }
 private:
  const Comparator* comparator_;
};

// When used with std::priority_queue, this comparison functor puts the
// iterator with the min/smallest key on top.
class MinIteratorComparator {
 public:
  MinIteratorComparator(const Comparator* comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) const {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }
 private:
  const Comparator* comparator_;
};
typedef BinaryHeap<IteratorWrapper*, MaxIteratorComparator> MergerMaxIterHeap;
typedef BinaryHeap<IteratorWrapper*, MinIteratorComparator> MergerMinIterHeap;

class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward),
        minHeap_(comparator) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    assert(comparator_ != nullptr);
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    ClearHeaps();
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
      if(children_[i].Valid()) {
        minHeap_.push(&children_[i]);
      }
    }
    // FindSmallest();
    // current_ = minHeap_.top();
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
      if(children_[i].Valid()) {
        maxHeap_->push(&children_[i]);
      }
    }
    // FindLargest();
    
    direction_ = kReverse;
    current_ =  CurrentReverse();
  }

  void Seek(const Slice& target) override {
    ClearHeaps();
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);

      if(children_[i].Valid()) {
        minHeap_.push(&children_[i]);
      }
    }
    // FindSmallest();
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      SwitchToForward();
      assert(current_ == CurrentForward());
      // for (int i = 0; i < n_; i++) {
      //   IteratorWrapper* child = &children_[i];
      //   if (child != current_) {
      //     child->Seek(key());
      //     if (child->Valid() &&
      //         comparator_->Compare(key(), child->key()) == 0) {
      //       child->Next();
      //     }
      //   }
      // }
      // direction_ = kForward;
    }

    assert(current_ = CurrentForward());
    current_->Next();
    if(current_->Valid()) {
      minHeap_.replace_top(current_);
    } else {
      minHeap_.pop();
    }
    current_ = CurrentForward();
    // current_->Next();
    // FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      ClearHeaps();
      InitMaxHeap();
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
        if(child->Valid()) {
          maxHeap_->push(child);
        }
      }
      direction_ = kReverse;
      assert(current_ == CurrentReverse());
    }

    assert(current_ == CurrentReverse());
    current_->Prev();
    if(current_->Valid()) {
      maxHeap_->replace_top(current_);
    } else {
      maxHeap_->pop();
    }
    current_ = CurrentReverse();
    // FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();
  MergerMinIterHeap minHeap_;
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
  void InitMaxHeap() {
    if(!maxHeap_) {
      maxHeap_.reset(new MergerMaxIterHeap(comparator_));
    }
  }

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    return !maxHeap_->empty() ? maxHeap_->top() : nullptr;
  }

  void ClearHeaps() {
    minHeap_.clear();
    if (maxHeap_) {
      maxHeap_->clear();
    }
  }
  void SwitchToForward() {
  // Otherwise, advance the non-current children.  We advance current_
  // just after the if-block.
    ClearHeaps();
    Slice target = key();
    // for (auto& child : children_) {
    for(int i=0; i < n_; i++) {

      IteratorWrapper* child = &children_[i];
      if (child != current_) {
        child->Seek(target);
        // considerStatus(child.status());
        // if (child.Valid() && comparator_->Equal(target, child.key())) {
          // child.Next();
          // considerStatus(child.status());
        // }
      }
      if (child->Valid()) {
        minHeap_.push(child);
      }
    }
    direction_ = kForward;
  }

};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
