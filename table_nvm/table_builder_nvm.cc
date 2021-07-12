
#include <string>
#include <libpmem.h>

#include "table_nvm/table_builder_nvm.h"
#include "db/version_edit.h"
#include "db/filename.h"


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

        TableBuilderNVM *builder = new TableBuilderNVM(options, nvm_mem);
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
TableBuilderNVM::TableBuilderNVM(const Options &option, char *raw) {
    assert(raw != nullptr); 
    raw_ = raw;
    offset_ = 0;
    option_ = option;
    // assert(tablenvm_->buckets_num_ == 0);
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
void TableBuilderNVM::Add(const Slice& key, const Slice& value){

    int total_size = key.size() + value.size() + 8 + 8;
    if(offset_ + total_size > option_.max_nvm_table_size) {
        std::cout << "size of adding kv execeeds limit" << std::endl;
        return;
    }


    std::string key_value;
    PutFixed64(&key_value, key.size());
    key_value.append(key.data(), key.size());

    PutFixed64(&key_value, value.size());
    key_value.append(value.data(), value.size());

    mempcpy(raw_ + offset_, key_value.c_str(), key_value.length());

    KeyMetaData *key_meta = new KeyMetaData();
    key_meta->key.DecodeFrom(key);
    key_meta->offset = offset_;
    key_meta->total_size = total_size;

    meta_data_.push_back(key_meta);

    offset_ += total_size;

    // we are at an empty bucket, so it is safe to just assign kv 
    // to bucket content
    // persistent_ptr<pmem::obj::string> buffer = &(tablenvm->buckets_rep_[bucket_num]);
    // PutVarint32NVM(buffer.get(), key.size());
    // PutVarint32NVM(buffer.get(), value.size());

    // buffer->append(key.data(), key.size());
    // buffer->append(value.data(), value.size());

    // KeysMetadata meta = {key.data(), 0, key.size(), bucket_num};
    // tablenvm->buckets_keys.push_back(meta);
    
    //tablenvm->buckets_num_++;
    
    //tablenvm->buckets_rep_[tablenvm->buckets_num_++].append();
}

Status TableBuilderNVM::Finish(){
    std::string meta_str;

    for(int i=0; i < meta_data_.size(); i++) {
        Slice key = meta_data_[i]->key.Encode();
        PutFixed64(&meta_str, key.size());

        meta_str.append(key.data(), key.size());

        PutFixed64(&meta_str, meta_data_[i]->offset);
        PutFixed64(&meta_str, meta_data_[i]->total_size);

    }

    if(offset_ + meta_str.size() > option_.max_nvm_table_size) {
        std::string msg =  "size exceeds while appending meta data size:" + std::to_string(meta_str.size()) +  "table limit:" + std::to_string(option_.max_nvm_table_size) ;
        return Status::IOError(msg);
    } 


    memcpy(raw_ + offset_, meta_str.c_str(), meta_str.size());

    pmem_persist(raw_, offset_ + meta_str.size());

    return Status::OK();
}


}