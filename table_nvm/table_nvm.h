

#ifndef TABLE_NVM_H
#define TABLE_NVM_H

#include <string>

#include "db/dbformat.h"
#include "leveldb/slice.h"
#include "leveldb/table.h"
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>

//#include <libpmemobj++/experimental/


// using namespace pmem::obj;

namespace leveldb{

struct KeyMetaData {
  InternalKey key;
  uint64_t offset;
  // uint64_t total_size; // key_size + sizeof(key) + val_size + sizeof(val)
};

// struct SegmentMeta {
//   KeyMetaData *key_metas;
//   uint64_t num;

//   SegmentMeta() {

//   }

//   ~SegmentMeta() {
//     delete []key_metas;
//     key_metas = nullptr;
//   }
// };


struct Footer{
  uint64_t meta_index_offset;
  uint64_t meta_index_size;
  uint64_t key_meta_offset;
  uint64_t key_meta_size;
};


// each NVM table consists of multiple segment 
// and the segments need not be continuous on NVM
// since when we do level compaction, we will just 
// create new segments and have a copy of the original nvm meta
// and then append the new meta of the new segment to the 
// copy meta.
class NVMTableMeta {
public:
  // SegmentMeta* seg_metas;
  KeyMetaData *key_metas;
  uint64_t num;

  NVMTableMeta() {
    key_metas = nullptr;
    num = 0;
  }

  void DecondeFrom(const Slice* input) {

  }

  Iterator* NewIterator(const Comparator* comparator);

  ~NVMTableMeta() {
    delete []key_metas;
    key_metas = nullptr;
    // delete []seg_metas;
    // seg_metas = nullptr;
  }


private:
  class Iter;

};

/*
  MetaIndexEntry represents the meta data of the SegmentMeta
*/
class MetaIndexEntry {
public:
  MetaIndexEntry(const Slice& minkey, const Slice& maxkey, uint64_t  offset, uint64_t size, uint64_t prevFileNum) {
    min_key_.DecodeFrom(minkey);
    max_key_.DecodeFrom(maxkey);
    offset_ = offset;
    size_ = size;
    // optimize: record the pointer to the prev SSTable
    // we can also just record the pointer to the prev SSTable
    prev_file_num = prevFileNum;
    // filename_ = fname;
  }

  MetaIndexEntry() {
    
  }

  void EncodeTo(std::string* dst) const {

  }

  void DecodeFrom(const Slice* input)  {

  }

  InternalKey GetMinKey() {

  }

  InternalKey GetMaxKey() {

  }

  uint64_t GetPrevFileNum() {

  }

  uint64_t GetMetaSize() {

  }

  uint64_t GetOffset() {
    
  }

private:
  uint64_t prev_file_num;
  InternalKey min_key_;  // min key of the segment

  InternalKey max_key_; // max key of the segment
  uint64_t offset_; // offset of the segment in the sstable file
  uint64_t size_; // size of the segmentMeta in the sstable file
  // std::string filename_; //  file name of the sstable in which the segment exists.
  
};



// class OptionsNvm {
// public:
//     size_t Item_size = 1024; //1kB
//     uint Num_item_table = 1000;
//     float rate_limit = 0.2;
// };



class TableNVMIterator;

/**
 * the item to be inserted into the tablenvm should not exceed 
 * the preset limit
 ** 
**/
class TableNVM: public Table{
public:

  static Status Open(const Options& options, RandomAccessFile* file,
                    uint64_t file_size, TableNVM** table, Footer* footer);

  TableNVM(const TableNVM&) = delete;
  TableNVM& operator=(const TableNVM&) = delete;

  ~TableNVM();

  Iterator *NewIterator(const ReadOptions&) const;
  uint64_t ApproximateOffsetOf(const Slice& key) const;


private:
    friend class TableBuilderNVMLevel;

    friend class TableCache;

    friend class TableNVMIterator;
    
    struct Rep;

    /*
    initialize the table size 
    */
    // TableNVM(pool_base &pop, const OptionsNvm &nvmoption, const InternalKeyComparator *comp);
    explicit TableNVM(Rep* rep) : rep_(rep) {
    }


    struct KeyComparator {
      const InternalKeyComparator comparator;
      explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
      int operator()(const char* a, const char* b) const;
    };

   int find_less_or_equal_idx(const InternalKey &ikey);
   // used by TableBuilderNVMLevel
   void GetMetaIndexData(std::string *dst);

    Rep * const rep_;

    
    const InternalKeyComparator *comp_;
    // const OptionsNvm option_nvm_;
};


}




#endif