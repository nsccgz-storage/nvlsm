// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_NVM_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_NVM_H_
#include <vector>

#include "leveldb/export.h"
#include "table_nvm/table_nvm.h"
namespace leveldb {

class TableNVM;

class LEVELDB_EXPORT TableBuilderNVM {
public:
  TableBuilderNVM(const Options &option, char *nvm_mem_);
  TableBuilderNVM(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value);

    
  Status Finish();

  uint64_t FileSize() {return offset_;}

  uint64_t RawDataSize() { return raw_data_size_;}

  uint64_t MetaDataSize() { return meta_data_size_;}

private:
    char *raw_;
    uint64_t offset_;
    uint64_t raw_data_size_;
    uint64_t meta_data_size_;
    // int offset;

    Options option_;

    std::vector<KeyMetaData*> meta_data_;
};
}

#endif
