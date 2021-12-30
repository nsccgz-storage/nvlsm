

#ifndef TABLE_NVM_H
#define TABLE_NVM_H

#include <string>
#include <vector>
#include "db/dbformat.h"
#include "leveldb/slice.h"
// #include "leveldb/table.h"
// #include <libpmemobj++/persistent_ptr.hpp>
// #include <libpmemobj++/p.hpp>
// #include <libpmemobj++/make_persistent.hpp>
// #include <libpmemobj++/make_persistent_array.hpp>
// #include <libpmemobj++/container/string.hpp>
// #include <libpmemobj++/container/vector.hpp>


namespace leveldb{

class RandomAccessFile;

struct KeyMetaData {
  // KeyMetaData(Slice &k, uint64_t koffset, uint64_t doffset)
  //   :key_offset(koffset), data_offset(doffset) {
  //   key.DecodeFrom(k);
  //   // offs = ofset;
  // }
  KeyMetaData(Slice &k, Slice& k_offset, Slice& v_offset, Slice& k_d_offset) 
  :key(k), key_offset(k_offset), data_offset(v_offset),
    key_data_offset(k_d_offset) {

  }
  // InternalKey key;
  // uint64_t key_offset;
  // uint64_t data_offset;
  Slice key;
  Slice key_offset;
  Slice data_offset;
  Slice key_data_offset;
  // uint64_t total_size; // key_size + sizeof(key) + val_size + sizeof(val)
};

class Segment {
public:
  

  static Status Open(const Options& options, RandomAccessFile* file, uint64_t data_offset, uint64_t data_size, uint64_t key_offset, uint64_t key_size, Segment** segment); 

  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;

  ~Segment();

  Iterator* NewIterator(const ReadOptions&) const;
  Iterator* NewIndexIterator(const ReadOptions&) const;

  
private:
  struct SegRep;
  friend class TableCacheNVM;
  explicit Segment(SegRep* seg_rep): seg_rep_(seg_rep) {
    //seg_rep_ = seg_rep;
  }

  SegRep* seg_rep_;
};

class TableNVMIterator;

/**
 * the item to be inserted into the tablenvm should not exceed 
 * the preset limit
 ** 
**/
class TableNVM{
public:

  // static Status TableNVM::Open(const Options& options, 
  //               FileMetaData* file_meta, const std::string& db_name, 
  //               TableNVM** table);

  static Status Open(const Options& options,
                              std::vector<Segment*>& segments,  
                              TableNVM** table);

  TableNVM(const TableNVM&) = delete;
  TableNVM& operator=(const TableNVM&) = delete;

  ~TableNVM();

  Iterator *NewIterator(const ReadOptions&) const;

  std::vector<Iterator*> NewIndexIterator() const;
  uint64_t ApproximateOffsetOf(const Slice& key) const;


private:
    friend class TableBuilderNVMLevel;

    friend class TableCacheNVM;

    // friend class TableNVMIterator;
    
    struct Rep;

    Status InternalGet(const ReadOptions& options, const Slice& k, void *arg,
                        void (*handle_result)(void*, const Slice&,
                                                      const Slice&)); 
    /*
    initialize the table size 
    */
    // TableNVM(pool_base &pop, const OptionsNvm &nvmoption, const InternalKeyComparator *comp);
    // explicit TableNVM(Rep* rep) : rep_(rep) {
    // }
    explicit TableNVM(const Options& options, const std::vector<Segment*>& segs);

    struct KeyComparator {
      const InternalKeyComparator comparator;
      explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
      int operator()(const char* a, const char* b) const;
    };

  //  int find_less_or_equal_idx(const InternalKey &ikey);
   // used by TableBuilderNVMLevel
  //  void GetMetaIndexData(std::string *dst);

    // Rep * const rep_;
    std::vector<Segment*> segs_;
    Options options_;
    // const Comparator *comp_;
    // const OptionsNvm option_nvm_;
};


}




#endif