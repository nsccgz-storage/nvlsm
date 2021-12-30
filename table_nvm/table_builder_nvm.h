// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_NVM_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_NVM_H_
#include <vector>

#include "leveldb/export.h"
#include "table_nvm/table_nvm.h"
#include "table_nvm/table_cache_nvm.h"
namespace leveldb {

Status BuildTableNVM(const std::string& dbname, Env* env, const Options& options, 
                     TableCacheNVM* table_cache, Iterator* iter, FileMetaData* meta,  uint64_t new_seg_number);

class TableNVM;
class TableBuilderNVM {
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

  virtual Status Finish(WritableFile* file) = 0;

  virtual uint64_t FileSize() const = 0;

  virtual uint64_t NumEntries() const = 0;

  // virtual uint64_t 

  virtual uint64_t RawDataSize() const = 0;

  virtual uint64_t MetaDataSize() const = 0;

  virtual Slice Data() const = 0;

  virtual Slice KeyData() const = 0;
};
// TableBuilderNVM::~TableBuilderNVM() {}

class TableBuilderNVMSplit : public TableBuilderNVM{
public:
  TableBuilderNVMSplit(const Options &option );
  TableBuilderNVMSplit(const TableBuilderNVMSplit&) = delete;
  TableBuilderNVMSplit& operator=(const TableBuilderNVMSplit&) = delete;
  ~TableBuilderNVMSplit();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

    
  Status Finish(WritableFile* file) override;

  uint64_t FileSize() const override;// {return offset_;} 

  uint64_t RawDataSize() const override; //{ return raw_data_size_;}

  uint64_t MetaDataSize() const override; //{ return meta_data_size_;}

  uint64_t NumEntries() const override;

  Status status() const override;

  Slice Data() const override;

  Slice KeyData() const override;

private:

  struct KeyMeta {
    // InternalKey key;
    std::string key;
    uint64_t key_offset_meta;
    uint64_t data_offset;
  };
  // char *raw_;
  std::string buffer_;
  std::string meta_buffer_;
  uint64_t data_offset_;
  uint64_t meta_offset_;
  // uint64_t raw_data_size_;
  uint64_t meta_data_size_;
  // int offset;

  std::string fname_;

  Options option_;

  std::vector<KeyMeta*> meta_data_;
  bool closed_;
};


// class LEVELDB_EXPORT TableBuilderNVMLevel :public TableBuilderNVM{
// public:
//   TableBuilderNVMLevel(const Options &option, std::string fname, TableCache* table_cache, FileMetaData* prev_file_meta);
//   TableBuilderNVMLevel(const TableBuilderNVMLevel&) = delete;
//   TableBuilderNVMLevel& operator=(const TableBuilderNVMLevel&) = delete;
//   ~TableBuilderNVMLevel() {}
//   // Add key,value to the table being constructed.
//   // REQUIRES: key is after any previously added key according to comparator.
//   // REQUIRES: Finish(), Abandon() have not been called
//   void Add(const Slice& key, const Slice& value) override;

    
//   Status Finish() override;

//   uint64_t FileSize() const override;// {return offset_;} 

//   uint64_t RawDataSize() const override; //{ return raw_data_size_;}

//   uint64_t MetaDataSize() const override; //{ return meta_data_size_;}



// private:
//   std::string buffer_;

//   FileMetaData* prev_file_meta_;
//   TableCache* table_cache_;

//   std::string fname_;
//   std::vector<KeyMetaData*> meta_data_;
//   Options option_;
// };
}

#endif
