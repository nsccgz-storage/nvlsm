
#include <string>
#include <libpmem.h>

#include "table_nvm/table_builder_nvm.h"
#include "db/version_edit.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table.h"


#include "table_nvm/table_nvm.h"
#include "util/coding.h"

namespace leveldb {

Status BuildTableNVM(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {

    Status s;
    meta->file_size = 0;
    iter->SeekToFirst();

    std::string fname = TableFileName(dbname, meta->number);
    if(iter->Valid()) {
        char *nvm_mem;
        // s = NewNVMFile(fname, &nvm_mem);
        if(!s.ok()) {
            return s;
        }

        TableBuilderNVM *builder = new TableBuilderNVMSplit(options, fname);
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
        // RemoveNVMFile(fname);
        
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


// how about we just add value to the data part 
// and put all the keys to the key meta part
// this is my original thought. it' ok
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
    // if(offset_ + total_size > option_.max_nvm_table_size) {
    //     std::cout << "size of adding kv execeeds limit" << std::endl;
    //     return;
    // }


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

    assert(meta_data_.size() > 0);
    // build metadata , actually how to build appropriate meta data 
    // format is worth thinking 
    for(int i=0; i < meta_data_.size(); i++) {
        Slice key = meta_data_[i]->key.Encode();
        PutFixed64(&meta_str, key.size());

        meta_str.append(key.data(), key.size());

        PutFixed64(&meta_str, meta_data_[i]->offset);
        PutFixed64(&meta_str, meta_data_[i]->total_size);

    }
    meta_data_size_ = meta_str.size();
    raw_data_size_ = buffer_.size();


    // if(offset_ + meta_str.size() > option_.max_nvm_table_size) {
    //     std::string msg =  "size exceeds while appending meta data size:" + std::to_string(meta_str.size()) +  "table limit:" + std::to_string(option_.max_nvm_table_size) ;
    //     return Status::IOError(msg);
    // } 

    std::string meta_index_str;
    int midx_offset = raw_data_size_ + meta_data_size_;
    MetaIndexEntry* midx_entry = new MetaIndexEntry(meta_data_[0]->key.Encode(), meta_data_.back()->key.Encode(), raw_data_size_, meta_data_size_, 0);
    midx_entry->EncodeTo(&meta_index_str);

    // try to open a file in pmem and pmem_copy all the data in the 
    // buffer to the pmem file.
    
    // memcpy(raw_ + offset_, meta_str.c_str(), meta_str.size());

    // pmem_persist(raw_, offset_ + meta_str.size());

    WritableFile* file;
    // add footer size to file_total_size
    uint64_t file_total_size = buffer_.size() + meta_str.size() + meta_index_str.size();
    Status s = option_.env->NewFixedSizeWritableFile(fname_, &file, file_total_size);

    if(s.ok()) {
        s = file->Append(buffer_);
    }
    
    if(s.ok()) {
        s =  file->Append(meta_str);
    }

    if(s.ok()) {
        s = file->Append(meta_index_str);
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

uint64_t TableBuilderNVMSplit::MetaDataSize () const{ return meta_data_size_;}






TableBuilderNVMLevel::TableBuilderNVMLevel(const Options &option, std::string fname, TableCache* table_cache, FileMetaData* prev_file_meta){
    table_cache_ = table_cache;
    prev_file_meta_ = prev_file_meta;
}

// actually for TableBuilderNVMLevel, we could set a table size limit for it,
// if the size of the keys that is to be created for current segment exceed the limit
// we could finish current tableBuilderNVM and create a new TableBuilderNVM
// it may happen that new TableNVM is relatively small since the latter keys 
// no, I will not implement this idea.
void TableBuilderNVMLevel::Add(const Slice& key, const Slice& value){

}

Status TableBuilderNVMLevel::Finish(){
    // this repeated code can be encapsulated 
    // as a function
    std::string meta_str;

    assert(meta_data_.size() > 0);
    // build metadata , actually how to build appropriate meta data 
    // format is worth thinking 
    for(int i=0; i < meta_data_.size(); i++) {
        Slice key = meta_data_[i]->key.Encode();
        PutFixed64(&meta_str, key.size());

        meta_str.append(key.data(), key.size());

        PutFixed64(&meta_str, meta_data_[i]->offset);
        PutFixed64(&meta_str, meta_data_[i]->total_size);

    }
    // meta_data_size_ = meta_str.size();
    // raw_data_size_ = buffer_.size();


    // ReadOptions read_option;
    // read_option.fill_cache = false;
    // Iterator* meta_index_iter = table_cache_->NewMetaKeyIterator(read_option, prev_file_meta_->number);

    // Iterator* index_key_iter = table_cache_->NewKeyIterator(read_option, prev_file_meta_->number);
    // std::string prev_table_meta_content;

    // fetch entries through Cache class, we have fill_cache property to
    // tell the Cache not to cache the entries we read 

    // meta_index_iter->SeekToFirst();


    int meta_size = meta_str.size();
    int meta_offset = buffer_.size();

    MetaIndexEntry* meta_idx_entry = new MetaIndexEntry(meta_data_[0]->key.Encode(), meta_data_.back()->key.Encode(),
                                                        meta_offset, meta_size, prev_file_meta_->number);
    // meta_idx_entry->EncodeTo(&)
    // actually we could just append the final file, since size could be large

    std::string cur_meta_idx_str;
    meta_idx_entry->EncodeTo(&cur_meta_idx_str);

    WritableFile* file;
    uint64_t file_total_size = buffer_.size() + meta_str.size() + cur_meta_idx_str.size();
    Status s  = option_.env->NewFixedSizeWritableFile(fname_, &file, file_total_size);

    if(s.ok()) {
        s = file->Append(buffer_);
    }

    if(s.ok()) {
        s = file->Append(meta_str);
    }

    if(s.ok()) {
        s = file->Append(cur_meta_idx_str);
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
    
// we can cast the table fetched from tablecached to TableNVM, is it ok?

uint64_t TableBuilderNVMLevel::FileSize() const {
    return 0;
}

    

}




