

#ifndef TABLE_NVM_H
#define TABLE_NVM_H

#include <string>

#include "db/dbformat.h"
#include "leveldb/slice.h"
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
  uint64_t total_size; // key_size + sizeof(key) + val_size + sizeof(val)
};

struct SegmentMeta {
  KeyMetaData *key_metas;
  uint64_t num;

  SegmentMeta() {

  }

  ~SegmentMeta() {
    delete []key_metas;
    key_metas = nullptr;
  }
};


// each NVM table consists of multiple segment 
// and the segments need not be continuous on NVM
// since when we do level compaction, we will just 
// create new segments and have a copy of the original nvm meta
// and then append the new meta of the new segment to the 
// copy meta.
struct NVMTableMeta {
  SegmentMeta* seg_metas;
  // KeyMetaData *key_metas;
  uint64_t num;

  NVMTableMeta() {
    
  }

  ~NVMTableMeta() {
    delete []seg_metas;
    seg_metas = nullptr;
  }

};

/*
  MetaIndexEntry represents the meta data of the SegmentMeta
*/
class MetaIndexEntry {
public:
  MetaIndexEntry(const Slice& minkey, const Slice& maxkey, uint64_t  offset, uint64_t size, std::string fname) {
    min_key_.DecodeFrom(minkey);
    max_key_.DecodeFrom(maxkey);
    offset_ = offset;
    size_ = size;
    filename_ = fname;
  }

  void EncodeTo(std::string* dst) const {

  }

private:
  InternalKey min_key_;  // min key of the segment

  InternalKey max_key_; // max key of the segment
  uint64_t offset_; // offset of the segment in the sstable file
  uint64_t size_; // size of the segmentMeta in the sstable file
  std::string filename_; //  file name of the sstable in which the segment exists.
};



class OptionsNvm {
public:
    size_t Item_size = 1024; //1kB
    uint Num_item_table = 1000;
    float rate_limit = 0.2;
};



class TableNVMIterator;

/**
 * the item to be inserted into the tablenvm should not exceed 
 * the preset limit
 ** 
**/
class TableNVM {
public:
    /*
        initialize the table size 
    */
    TableNVM(pool_base &pop, const OptionsNvm &nvmoption, const InternalKeyComparator *comp);

    Status Get(pool_base &pop, const Slice &key, void *arg,
              void (*handle_result)(void*, const Slice&, const Slice&));

    //Status Add(pool_base&pop, const InternalKey &ikey, const SliceNVM& val);
    //Status Add(const Slice& key, const Slice &val);

    Iterator *NewIterator() const;

    size_t TableItemSizeLimit(void) const{
      return 0;
    }

    bool HitLimit() {
      //transaction::
      return  1;
    }

private:
    friend class TableNVMIterator;
    friend class TableBuilderNVM;
    // friend class  TableSorter;
    
    friend class TableNVMCache;
    struct Rep;


    struct KeyComparator {
      const InternalKeyComparator comparator;
      explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
      int operator()(const char* a, const char* b) const;
    };

   int find_less_or_equal_idx(const InternalKey &ikey);
    

    Rep * const rep_;

    
    const InternalKeyComparator *comp_;
    const OptionsNvm option_nvm_;
};


}




#endif