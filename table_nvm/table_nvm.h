

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



class OptionsNvm {
public:
    size_t Item_size = 1024; //1kB
    uint Num_item_table = 1000;
    float rate_limit = 0.2;
};

/**
 * 
 * immutable datatype
 * */
// class LEVELDB_EXPORT SliceNVM {
//  public:
//   // Create an empty slice.
//   SliceNVM() : data_(""), size_(0) {}

//   // Create a slice that refers to d[0,n-1].
//   SliceNVM (persistent_ptr<const char> d, size_t n) : data_(d), size_(n) {}

//   // Create a slice that refers to the contents of "s"
//   SliceNVM(const std::string& s) : data_(s.data()), size_(s.size()) {}

//   // Create a slice that refers to s[0,strlen(s)-1]
//   SliceNVM(const char* s) : data_(s), size_(strlen(s)) {}
  
//   // Intentionally copyable.
//   SliceNVM(const SliceNVM&) = default;
//   SliceNVM& operator=(const SliceNVM&) = default;

//   // Return a pointer to the beginning of the referenced data
//     persistent_ptr<const char> data() const { return data_; }

//   // Return the length (in bytes) of the referenced data
//   size_t size() const { return size_; }

//   // Return true iff the length of the referenced data is zero
//   bool empty() const { return size_ == 0; }

//   // Return the ith byte in the referenced data.
//   // REQUIRES: n < size()
//   char operator[](size_t n) const {
//     assert(n < size());
//     return data_[n];
//   }

//   // Change this slice to refer to an empty array
//   void clear() {
//     data_ = "";
//     size_ = 0;
//   }

//   // Drop the first "n" bytes from this slice.
//   /*
//   void remove_prefix(size_t n) {
//     assert(n <= size());
//     data_ += n;
//     size_ -= n;
//   }
//   */

//   // Return a string that contains the copy of the referenced data.
//   std::string ToString() const { return std::string(data_.get(), size_); }

//   // Three-way comparison.  Returns value:
//   //   <  0 iff "*this" <  "b",
//   //   == 0 iff "*this" == "b",
//   //   >  0 iff "*this" >  "b"
//   int compare(const Slice& b) const;

//   // Return true iff "x" is a prefix of "*this"
//   /*
//   bool starts_with(const Slice& x) const {
//     return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
//   }
//   */
 
//  private:
//   persistent_ptr<const char> data_;
//   //const char* data_;
//   p<size_t> size_;
//   //size_t size_;
// };



  struct KeysMetadata {
    //persistent_ptr<char[]> key;
    pmem::obj::string key;
    p<uint64_t> offset;
    p<uint64_t> size;
    p<uint64_t> bucket_num;
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