
#include <string>
#include <libpmem.h>

#include "table_nvm/table_builder_nvm.h"
#include "db/version_edit.h"
#include "db/filename.h"
#include "leveldb/env.h"


#include "table_nvm/table_nvm.h"
#include "util/coding.h"

namespace leveldb {

Status BuildTableNVM(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {

    Status s;
    meta->file_size = 0;
    iter->SeekToFirst();

    std::string fname = TableFileName(dbname, meta->name);
    if(iter->Valid()) {
        char *nvm_mem;
        s = NewNVMFile(fname, &nvm_mem);
        if(!s.ok()) {
            return s;
        }

        TableBuilderNVM *builder = new TableBuilderNVMSplit(options, nvm_mem);
        meta->smallest.DecodeFrom(iter->key());
        Slice key;
        for(; iter->Valid(); iter->Next()) {
            key = iter->key();
            builder->Add(key, iter->value());
        }

        if(!key.empty()) {
            meta->largest.DecodeFrom(key);
        }

        s = builder->Finish();
        if(s.ok()) {
            meta->file_size = builder->FileSize();
            meta->raw_data_size = builder->RawDataSize();
            meta->meta_size = builder->MetaDataSize();
            assert(meta->file_size > 0);
        }
        delete builder;
        
    }

    if(s.ok() && meta->file_size > 0) {

    } else {
        RemoveNVMFile(fname);
        
    }

    return s;
}

/**
 * tablenvm should be empty
 * */
TableBuilderNVMSplit::TableBuilderNVMSplit(const Options &option, std::string fname): 
                option_(option),
                fname_(fname),
                offset_(0) {

    // offset_ = 0;
}


/* ------------
 * | key_size | // 64bit
 * ------------
 * |    key   |
 * ------------
 * |value_size| // 64bit
 * ------------
 * |   value  |
 * ------------
 * */
void TableBuilderNVMSplit::Add(const Slice& key, const Slice& value){

    int total_size = key.size() + value.size() + 8 + 8;
    if(offset_ + total_size > option_.max_nvm_table_size) {
        std::cout << "size of adding kv execeeds limit" << std::endl;
        return;
    }


    // std::string key_value;
    PutFixed64(&buffer_, key.size());
    buffer_.append(key.data(), key.size());

    PutFixed64(&buffer_, value.size());
    buffer_.append(value.data(), value.size());

    // mempcpy(raw_ + offset_, key_value.c_str(), key_value.length());

    KeyMetaData *key_meta = new KeyMetaData();
    key_meta->key.DecodeFrom(key);
    key_meta->offset = offset_;
    key_meta->total_size = total_size;

    meta_data_.push_back(key_meta);

    offset_ += total_size;

}

Status TableBuilderNVMSplit::Finish(){
    std::string meta_str;

    for(int i=0; i < meta_data_.size(); i++) {
        Slice key = meta_data_[i]->key.Encode();
        PutFixed64(&meta_str, key.size());

        meta_str.append(key.data(), key.size());

        PutFixed64(&meta_str, meta_data_[i]->offset);
        PutFixed64(&meta_str, meta_data_[i]->total_size);

    }
    meta_data_size_ = meta_str.size();
    raw_data_size_ = buffer_.size();


    if(offset_ + meta_str.size() > option_.max_nvm_table_size) {
        std::string msg =  "size exceeds while appending meta data size:" + std::to_string(meta_str.size()) +  "table limit:" + std::to_string(option_.max_nvm_table_size) ;
        return Status::IOError(msg);
    } 


    // try to open a file in pmem and pmem_copy all the data in the 
    // buffer to the pmem file.
    
    // memcpy(raw_ + offset_, meta_str.c_str(), meta_str.size());

    // pmem_persist(raw_, offset_ + meta_str.size());

    WritableFile* file;
    Status s = option_.env->NewFixedSizeWritableFile(fname_, &file, buffer_.size() + meta_str.size());

    if(s.ok()) {
        s = file->Append(buffer_);

    }
    
    if(s.ok()) {
        s =  file->Append(meta_str);

    }

    if(!s.ok()) {
        delete file;
        return s;
    }

    s = file->Close();
    delete file;
    file = nullptr;


    return Status::OK();
}

  uint64_t TableBuilderNVMSplit::FileSize() const {return raw_data_size_ + meta_data_size_;} 

  uint64_t TableBuilderNVMSplit::RawDataSize() const { return raw_data_size_;}

  uint64_t TableBuilderNVMSplit::MetaDataSize() const{ return meta_data_size_;}




}