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
  virtual ~TableBuilderNVM() {}
  // TableBuilderNVM(const Options &option, char *nvm_mem_);
  // TableBuilderNVM(const TableBuilder&) = delete;
  // TableBuilder& operator=(const TableBuilder&) = delete;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Add(const Slice& key, const Slice& value) = 0;

  virtual Status status() const = 0;

  virtual Status Finish() = 0;

  virtual uint64_t FileSize() const = 0;

  virtual uint64_t NumEntries() const = 0;

  // virtual uint64_t 

  virtual uint64_t RawDataSize() const = 0;

  virtual uint64_t MetaDataSize() const = 0;


};


class LEVELDB_EXPORT TableBuilderNVMSplit : public TableBuilderNVM{
public:
  TableBuilderNVMSplit(const Options &option, std::string fname);
  TableBuilderNVMSplit(const TableBuilderNVMSplit&) = delete;
  TableBuilderNVMSplit& operator=(const TableBuilderNVMSplit&) = delete;
  ~TableBuilderNVMSplit() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

    
  Status Finish() override;

  uint64_t FileSize() const override;// {return offset_;} 

  uint64_t RawDataSize() const override; //{ return raw_data_size_;}

  uint64_t MetaDataSize() const override; //{ return meta_data_size_;}

private:

  // char *raw_;
  std::string buffer_;
  uint64_t offset_;
  uint64_t raw_data_size_;
  uint64_t meta_data_size_;
  // int offset;

  std::string fname_;

  Options option_;

  std::vector<KeyMetaData*> meta_data_;
};


class LEVELDB_EXPORT TableBuilderNVMLevel :public TableBuilderNVM{
public:

};
}

#endif
