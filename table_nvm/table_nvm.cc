#include <cstdio>
#include <stdint.h>
#include <string>
#include <vector>
// #include <libpmemobj++/make_persistent.hpp>
// #include <libpmemobj++/p.hpp>
// #include <libpmemobj++/persistent_ptr.hpp>
// #include <libpmemobj++/pool.hpp>
// #include <libpmemobj++/transaction.hpp>
// #include <libpmemobj++/utils.hpp>

#include "db/filename.h"
#include "db/version_edit.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"

#include "table/merger.h"
#include "util/coding.h"

#include "table_nvm.h"

// using namespace pmem::obj;

namespace leveldb {

struct Segment::SegRep {
  SegRep() {}
  ~SegRep() {
    // delete file;
    for (int i = 0; i < key_metas.size(); i++) {
      delete key_metas[i];
      key_metas[i] = nullptr;
    }
    key_metas.clear();
    if (key_meta != nullptr) {
      delete key_meta;
      key_meta = nullptr;
    }
    // delete key_meta;
    // key_meta = nullptr;
  }
  // file content for data part;
  // RandomAccessFile* file;
  // std::vector<KeyMetaData*> key_metas;
  Options options;
  // const char* data;
  uint64_t data_offset;
  uint64_t data_size;
  const char* key_meta;
  uint64_t key_offset;
  uint64_t key_size;
  RandomAccessFile* file;
  std::vector<KeyMetaData*> key_metas;
};

TableNVM::~TableNVM() {
  // delete segs
  // for(int i=0; i < segs_.size(); i++) {
  //     delete segs_[i];
  // }
}

class SegIterator : public Iterator {
 public:
  explicit SegIterator(const Comparator* comparator,
                       const std::vector<KeyMetaData*>* key_metas,
                       RandomAccessFile* file, uint64_t base_offset)
      : key_idx_(key_metas->size()),
        key_metas_(key_metas),
        file_(file),
        base_offset_(base_offset),
        data_(nullptr),
        comparator_(comparator) {
    // assert(seg_rep->key_metas.size() > 0);
  }

  ~SegIterator() override {
    if (data_ != nullptr) {
      delete data_;
    }
  }

  bool Valid() const override {
    return key_idx_ >= 0 && key_idx_ < (*key_metas_).size();
  }

  void Seek(const Slice& k) override {
    int left = 0;
    int right = (*key_metas_).size();

    // lower bound ?
    while (left < right) {
      int mid = (left + right) / 2;
      if (comparator_->Compare((*key_metas_)[mid]->key, k) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    key_idx_ = left;
    if (Valid()) {
      getCurKeyVal();
    }
  }

  void SeekToFirst() override {
    key_idx_ = 0;
    if (Valid()) {
      getCurKeyVal();
    }
  }

  void SeekToLast() override {
    key_idx_ = (*key_metas_).size() - 1;
    if (Valid()) {
      getCurKeyVal();
    }
  }

  void Next() override {
    assert(Valid());
    key_idx_++;
    if (Valid()) {
      getCurKeyVal();
    }
  }
  void Prev() override {
    assert(Valid());
    key_idx_--;
    if (Valid()) {
      getCurKeyVal();
    }
  }

  Slice key() const override {
    return cur_key_;
    // return seg_rep_->key_metas[key_idx_]->key;
  }

  Slice value() const override {
    // return data_+cur_val_offset_;
    return cur_val_;
    // return seg_rep_->key_metas[key_idx_]->data_offset;
  }

  Status status() const override { return Status::OK(); }

 private:
  void getCurKeyVal() {
    cur_key_ = (*key_metas_)[key_idx_]->key;
    cur_val_offset_ =
        DecodeFixed64((*key_metas_)[key_idx_]->data_offset.data());
    // uint64_t relative_data_offset = cur_val_offset_ - base_offset_;

    Slice val_size_slice;
    char buf[8];
    Status s;
    s = file_->Read(cur_val_offset_, sizeof(buf), &val_size_slice, buf);
    if (!s.ok()) {
      // printf("read data from file failed\n");
      printf("%s\n", s.ToString().data());
    }
    uint64_t val_size = DecodeFixed64(val_size_slice.data());
    // char *data = new char[val_size];
    if (data_ != nullptr) {
      delete data_;
      data_ = nullptr;
    }
    cur_val_ = Slice();
    if (val_size > 0) {
      data_ = new char[val_size];
      s = file_->Read(cur_val_offset_ + 8, val_size, &cur_val_, data_);
      if (!s.ok()) {
        // printf( "read data from file failed\n");
        printf("%s\n", s.ToString().data());
      }
    }
  }
  int key_idx_;
  // const SegRep* const seg_rep_;
  // const char* data_;
  char* data_;
  uint64_t base_offset_;
  RandomAccessFile* file_;
  uint64_t cur_val_offset_;
  Slice cur_key_;
  Slice cur_val_;
  const Comparator* comparator_;
  const std::vector<KeyMetaData*>* key_metas_;
};

class SegKeyoffsetIterator : public Iterator {
 public:
  explicit SegKeyoffsetIterator(const Comparator* comprator,
                                const std::vector<KeyMetaData*>* key_metas)
      : key_metas_(key_metas),
        key_idx_(key_metas->size()),
        comparator_(comprator) {}

  bool Valid() const override {
    return key_idx_ >= 0 && key_idx_ < (*key_metas_).size();
  }

  void Seek(const Slice& k) override {
    int left = 0;
    int right = (*key_metas_).size();

    // lower bound ?
    while (left < right) {
      int mid = (left + right) / 2;
      if (comparator_->Compare((*key_metas_)[mid]->key, k) < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    key_idx_ = left;
  }

  void SeekToFirst() override { key_idx_ = 0; }

  void SeekToLast() override {
    if (key_metas_->empty()) {
      key_idx_ = 0;
    } else {
      key_idx_ = (*key_metas_).size() - 1;
    }
  }

  void Next() override {
    assert(Valid());
    key_idx_++;
  }

  void Prev() override {
    assert(Valid());
    key_idx_--;
  }

  Slice key() const override { return (*key_metas_)[key_idx_]->key; }

  Slice value() const override {
    return (*key_metas_)[key_idx_]->key_data_offset;
  }

  Status status() const override { return Status::OK(); }

 private:
  int key_idx_;
  // const SegRep* const seg_rep_;
  const std::vector<KeyMetaData*>* key_metas_;
  const Comparator* comparator_;
  // const TableNVM* table_;
  // Iterator* key_meta_iter;
};

std::vector<Iterator*> TableNVM::NewIndexIterator() const {
  std::vector<Iterator*> res;
  ReadOptions options;
  for (int i = 0; i < segs_.size(); i++) {
    res.push_back(segs_[i]->NewIndexIterator(options));
  }

  return res;
}

// class TableNVMIterator: public Iterator {
// public:
//     explicit TableNVMIterator(const TableNVM*tablenvm):
//         tablenvm_(tablenvm) {
//         // merging all prev segment table iterator
//     }

//     TableNVMIterator(const TableNVMIterator&) = delete;
//     TableNVMIterator& operator=(const TableNVMIterator&) = delete;

//     ~TableNVMIterator() override {

//     }

//     bool Valid() const override {
//         return key_index_iter_->Valid();
//     }
//     void Seek(const Slice& k) override {

//     }
//     void SeekToFirst() override {
//         key_index_iter_->SeekToFirst();

//     }
//     void SeekToLast() override {
//         key_index_iter_->SeekToLast();
//     }
//     void Next() override {
//         assert(Valid());
//         key_index_iter_->Next();

//     }
//     void Prev() override {}
//     Slice key() const override{
//         return  key_;
//     }
//     Slice value() const override {

//     }

//     Status status() const override {return Status::OK();}

// private:
//     void binary_search(const Slice& k) {

//     }

//     /**
//      * return pos of the bucket;
//      * */
//     int find_greater_than_or_equal_to(const Slice&k) {

//     }

//     void ParseNextKey() {
//         key_.assign(key_index_iter_->key().data(),
//         key_index_iter_->key().size()); Slice v_pos_val =
//         key_index_iter_->value(); uint64_t offset =
//         DecodeFixed64(v_pos_val.data());

//         Slice key_len_result;
//         Status s = tablenvm_->rep_->file->Read(offset, sizeof(uint64_t),
//         &key_len_result, nullptr); if(!s.ok()) {
//             printf("parseNextKey read key len failed\n");
//             exit(1);
//         }
//         uint64_t key_len = DecodeFixed64(key_len_result.data());
//         offset += sizeof(uint64_t);
//         Slice key;
//         s = tablenvm_->rep_->file->Read(offset, key_len, &key, nullptr);
//         if(!s.ok()) {
//         }
//     }

//     const TableNVM *tablenvm_;
// };

static inline void DecodeMetaVals(Slice& val, uint64_t* key_offset,
                                  uint64_t* key_data_offset) {
  *key_offset = DecodeFixed64(val.data());
  *key_data_offset = DecodeFixed64(val.data() + 8);
}

static bool compareKey(const Slice& akey, const Slice& bkey) {
  auto* byte_comp = BytewiseComparator();
  int r = byte_comp->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  if (r < 0) {
    return true;
  } else
    return false;
}

void TableNVM::GetMidKeyDataOffset(std::vector<SegSepKeyData>& left,
                                   std::vector<SegSepKeyData>& right) const {
  std::vector<Slice> mid_keys(left.size());

  for (int i = 0; i < left.size(); i++) {
    Slice ret = segs_[i]->GetMidKey();
    mid_keys[i] = ret;
  }

  sort(mid_keys.begin(), mid_keys.end(), compareKey);
  Slice mid_key = mid_keys[(mid_keys.size() - 1) / 2];
  // printf("mid key is %s\n", mid_key.data());

  // segs_[0]->GetMidKeys(left[0], right[0]);
  // Slice first_left_key = left[0].key;
  for (int i = 0; i < segs_.size(); i++) {
    segs_[i]->SearchForMidKeys(mid_key, left[i], right[i]);
  }
}

void Segment::SearchForMidKeys(Slice& key, SegSepKeyData& left,
                               SegSepKeyData& right) const {
  ReadOptions options;
  Iterator* seg_iter = NewIndexIterator(options);
  seg_iter->Seek(key);
  // seg_key is the one that is lower bound of key

  uint64_t end_key_offset = seg_rep_->key_offset + seg_rep_->key_size;
  uint64_t end_data_offset = seg_rep_->data_offset + seg_rep_->data_size;
  if (!seg_iter->Valid()) {
    // this means all keys of this segment are smaller than first_left_key
    seg_iter->SeekToLast();
    Slice seg_key = seg_iter->key();
    Slice seg_key_data_offset = seg_iter->value();
    // put to left
    left.key = seg_key;
    left.key_data_offset = seg_key_data_offset;
    left.next_key_offset = end_key_offset;
    left.next_data_offset = end_data_offset;
    // there is not right part
  } else {
    // there may be no left part,
    // maybe there is no left part or we can set the first key of the segment to
    // be in the left
    Slice right_key = seg_iter->key();
    Slice right_key_data_offset = seg_iter->value();
    right.key = right_key;
    right.key_data_offset = right_key_data_offset;
    seg_iter->Next();
    if (seg_iter->Valid()) {
      Slice right_next_key_data_offset = seg_iter->value();
      DecodeMetaVals(right_next_key_data_offset, &right.next_key_offset,
                     &right.next_data_offset);
      seg_iter->Prev();
    } else {
      right.next_key_offset = end_key_offset;
      right.next_data_offset = end_data_offset;
      seg_iter->SeekToLast();
    }

    seg_iter->Prev();
    if (!seg_iter->Valid()) {
      // left has no key
    } else {
      left.key = seg_iter->key();
      left.key_data_offset = seg_iter->value();
      DecodeMetaVals(right_key_data_offset, &left.next_key_offset,
                     &left.next_data_offset);
    }
  }
}

Slice Segment::GetMidKey() const {
  int len = seg_rep_->key_metas.size();

  int mid_idx = (len - 1) / 2;
  return seg_rep_->key_metas[mid_idx]->key;

  // return seg_rep_->key_metas[mid_idx]->key;
}

void Segment::GetMidKeys(SegSepKeyData& left, SegSepKeyData& right) const {
  int len = seg_rep_->key_metas.size();
  int left_mid = (len - 1) / 2;
  int right_mid = left_mid + 1;

  uint64_t end_key_offset = seg_rep_->key_offset + seg_rep_->key_size;
  uint64_t end_data_offset = seg_rep_->data_offset + seg_rep_->data_size;
  KeyMetaData* left_key_meta = seg_rep_->key_metas[left_mid];
  left.key = left_key_meta->key;
  left.key_data_offset = left_key_meta->key_data_offset;
  if (right_mid >= seg_rep_->key_metas.size()) {
    left.next_key_offset = end_key_offset;
    left.next_data_offset = end_data_offset;
  } else {
    right.key = seg_rep_->key_metas[right_mid]->key;
    right.key_data_offset = seg_rep_->key_metas[right_mid]->key_data_offset;
    DecodeMetaVals(seg_rep_->key_metas[right_mid]->key_data_offset,
                   &left.next_key_offset, &left.next_data_offset);

    int right_mid_next = right_mid + 1;
    if (right_mid_next >= seg_rep_->key_metas.size()) {
      right.next_key_offset = end_key_offset;
      right.next_data_offset = end_data_offset;
    } else {
      DecodeMetaVals(seg_rep_->key_metas[right_mid_next]->key_data_offset,
                     &right.next_key_offset, &right.next_data_offset);
    }
  }
}

Iterator* TableNVM::NewIterator(const ReadOptions& read_options) const {
  std::vector<Iterator*> iters;
  for (int i = segs_.size() - 1; i >= 0; i--) {
    iters.push_back(segs_[i]->NewIterator(read_options));
  }
  assert(!iters.empty());
  return NewMergingIterator(options_.comparator, &iters[0], iters.size());
}

Status Segment::InternalGet(const ReadOptions& options, const Slice& k,
                            void* arg,
                            void (*handle_result)(void*, const Slice&,
                                                  const Slice&)) {
  Iterator* iter = NewIterator(options);
  iter->Seek(k);
  if (iter->Valid()) {
    (*handle_result)(arg, iter->key(), iter->value());
    return Status::OK();
  } else {
    return Status::NotFound("");
  }
}

Status TableNVM::InternalGet(const ReadOptions& options, const Slice& k,
                             void* arg,
                             void (*handle_result)(void*, const Slice&,
                                                   const Slice&)) {
  Status s;
  // for(int i=segs_.size()-1; i >= 0; i--) {
  //     if()
  // }
  // Iterator* iter = NewIterator(options) ;
  // iter->Seek(k);
  // if(iter->Valid()) {
  //     (*handle_result)(arg, iter->key(), iter->value());
  // }

  // s = iter->status();
  // delete iter;
  return s;
}

Status TableNVM::Open(const Options& options, std::vector<Segment*>& segs,
                      TableNVM** table) {
  // assert(file_meta->is_nvm);
  *table = nullptr;

  // TableNVM::Rep* rep = new TableNVM::Rep();
  // rep->options = options;
  // rep->file = file;

  // rep->keys_meta = new NVMTableMeta();
  // rep->meta = new MetaIndexEntry();

  // Slice* result_meta = nullptr;
  // char buf[footer->meta_index_size];
  // we are assuming that we will definitely using pmem
  // Status s;
  // rep->segs.resize(file_meta->segments.size());
  // for(int i=0; i < file_meta->segments.size(); i++) {
  //     Segment* segment;
  //     s = Segment::Open(options, file_meta->segments[i], db_name, &segment);
  //     if(!s.ok()) {
  //         return s;
  //     }
  //     rep->segs.push_back(segment);

  // }

  *table = new TableNVM(options, segs);

  return Status::OK();
}

TableNVM::TableNVM(const Options& options, const std::vector<Segment*>& segs) {
  options_ = options;
  segs_ = std::move(segs);
}

Status Segment::Open(const Options& options, RandomAccessFile* file,
                     uint64_t data_offset, uint64_t data_size,
                     uint64_t key_offset, uint64_t key_size,
                     Segment** segment) {
  SegRep* seg_rep = new SegRep();
  seg_rep->key_meta = nullptr;
  seg_rep->options = options;
  seg_rep->data_offset = data_offset;
  seg_rep->data_size = data_size;
  seg_rep->key_offset = key_offset;
  seg_rep->key_size = key_size;
  seg_rep->file = file;

  Status s;
  uint64_t cur_meta_size = 0;
  char* key_buf = new char[key_size];
  Slice key_contens;
  // s = key_file->Read(0, key_size, &key_contens, key_buf);
  s = file->Read(key_offset, key_size, &key_contens, key_buf);
  const char* key_data = key_contens.data();
  if (!s.ok()) {
    printf("file read not ok %s\n", s.ToString().c_str());
  }
  assert(key_contens.size() == key_size);
  if (key_data != key_buf) {
    delete[] key_buf;
  } else {
    seg_rep->key_meta = key_buf;
  }

  while (cur_meta_size < key_size) {
    Slice key_size_slice;
    key_size_slice = Slice(key_data + cur_meta_size, 8);
    // key_size
    uint64_t cur_key_size = DecodeFixed64(key_size_slice.data());
    // printf("decode cur key size: %lu\n", cur_key_size);
    if (cur_key_size > key_size) {
      printf("decode err");
    }
    Slice key;
    key = Slice(key_data + cur_meta_size + 8, cur_key_size);
    // InternalKey

    Slice key_offset_slice;
    key_offset_slice = Slice(key_data + cur_meta_size + 8 + cur_key_size, 8);

    Slice data_offset_slice;
    data_offset_slice =
        Slice(key_data + cur_meta_size + 8 + cur_key_size + 8, 8);

    Slice key_data_offset_slice =
        Slice(key_data + cur_meta_size + 8 + cur_key_size, 16);

    KeyMetaData* key_meta_ptr = new KeyMetaData(
        key, key_offset_slice, data_offset_slice, key_data_offset_slice);
    assert(key_meta_ptr != nullptr);
    // seg_rep->key_metas.push_back(new KeyMetaData(key, key_offset_slice,
    // data_offset_slice, key_data_offset_slice));
    seg_rep->key_metas.push_back(key_meta_ptr);

    uint64_t cur_key_total_size = 8 + cur_key_size + 8 + 8;
    cur_meta_size += cur_key_total_size;
  }
  if (cur_meta_size != key_size) {
    printf("segment open cur meta size: %lu != key size:%lu\n", cur_meta_size,
           key_size);
  }
  assert(cur_meta_size == key_size);
  Segment* new_segment = new Segment(seg_rep);
  *segment = new_segment;
  return s;
}

Iterator* Segment::NewIterator(const ReadOptions& read_options) const {
  return new SegIterator(seg_rep_->options.comparator, &seg_rep_->key_metas,
                         seg_rep_->file, seg_rep_->data_offset);
}

Iterator* Segment::NewIndexIterator(const ReadOptions& read_options) const {
  return new SegKeyoffsetIterator(seg_rep_->options.comparator,
                                  &seg_rep_->key_metas);
}

Segment::~Segment() {
  // for(int i=0; i < seg_rep_->key_metas.size(); i++) {
  //     delete seg_rep_->key_metas[i];
  // }
  // delete seg_rep_->data_file;
  delete seg_rep_;
}
}  // namespace leveldb
