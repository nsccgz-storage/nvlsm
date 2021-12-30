
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
                  TableCacheNVM* table_cache, Iterator* iter, FileMetaData* meta,  uint64_t new_seg_num) {

    Status s;
    meta->file_size = 0;
    iter->SeekToFirst();

    // std::string fname = TableFileName(dbname, meta->number);
    std::string fname = TableFileName(dbname, meta->number);

    WritableFile* file = nullptr; 
    if(iter->Valid()) {
        char *nvm_mem;
        // s = NewNVMFile(fname, &nvm_mem);
        if(!s.ok()) {
            return s;
        }

        TableBuilderNVM *builder = new TableBuilderNVMSplit(options );
        meta->smallest.DecodeFrom(iter->key());
        Slice key;
        for(; iter->Valid(); iter->Next()) {
            key = iter->key();
            builder->Add(key, iter->value());
        }

        if(!key.empty()) {
            meta->largest.DecodeFrom(key);
        }
        uint64_t file_size = builder->FileSize();
        // s = env->NewFixedSizeWritableFile(fname, &file, file_size);
        // s = env->NewWritableFile(fname, &file);
        s = env->NewAppendableFile(fname, &file);
        if(!s.ok()) {
            return s;
        }
        s = builder->Finish(file);
        if(s.ok()) {
            meta->file_size = builder->FileSize();
            // printf("build table nvm file size:%lu, data size:%lu, meta size:%lu \n", meta->file_size, builder->RawDataSize(), builder->MetaDataSize());
            // meta->raw_data_size = builder->RawDataSize();
            // meta->meta_size = builder->MetaDataSize();
            SegmentMeta *new_segment = new SegmentMeta(0, builder->RawDataSize(), builder->RawDataSize(), builder->MetaDataSize(),
                                    meta->number, new_seg_num, meta->smallest, meta->largest);
            
            meta->segments.push_back(new_segment);
            assert(meta->file_size > 0);
        }
        delete builder;
        
    }

    if(s.ok()) {
        assert(file != nullptr);
        s = file->Sync();
    }
    if(s.ok()) {
        s = file->Close();
    }
    delete file;
    file = nullptr;

    if(s.ok() && meta->file_size > 0) {
        Log(options.info_log, "Build L0 Table success, file_num:%ld, file size:%ld ",meta->number ,meta->file_size);
        Iterator* it = table_cache->NewIterator(ReadOptions(), meta, nullptr);
        delete it;
    } else {
        printf("build l0 table failed\n");
        // RemoveNVMFile(fname);
        env->RemoveFile(fname);
        
    }

    return s;
}

/**
 * tablenvm should be empty
 * */
TableBuilderNVMSplit::TableBuilderNVMSplit(const Options &option ): 
                option_(option),
                data_offset_(0),
                meta_offset_(0),
                closed_(false) {

    // offset_ = 0;
}

TableBuilderNVMSplit::~TableBuilderNVMSplit()  {
    for(int i=0; i < meta_data_.size(); i++) {
        delete meta_data_[i];
    }
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

    // int total_size = key.size() + value.size() + 8 + 8;
    int data_size = 8 + value.size();
    int key_meta_size = 8 + key.size() + 8 + 8;

    PutFixed64(&buffer_, (uint64_t)value.size());
    buffer_.append(value.data(), value.size());
    
    KeyMeta* key_meta = new KeyMeta();
    // bool success = key_meta->key.DecodeFrom(key);
    key_meta->key.assign(key.data(), key.size());
    key_meta->data_offset = data_offset_;
    key_meta->key_offset_meta = meta_offset_;

    meta_data_.push_back(key_meta);

    data_offset_+= data_size; 
    meta_offset_ += key_meta_size;
}

Status TableBuilderNVMSplit::Finish(WritableFile* file){
    closed_ = true;
    meta_buffer_.clear();
    assert(meta_data_.size() > 0);
    uint64_t data_size = buffer_.size();
    assert(data_size == data_offset_);
    uint64_t meta_start_offset = data_size;
    uint64_t meta_size = 0;
    // assert(data_size == offset_);
    // build metadata , actually how to build appropriate meta data 
    // format is worth thinking 
    for(int i=0; i < meta_data_.size(); i++) {
        Slice key = meta_data_[i]->key;
        PutFixed64(&meta_buffer_, key.size());
        // printf("put key size is: %ld\n", key.size());
        meta_buffer_.append(key.data(), key.size());

        PutFixed64(&meta_buffer_, meta_start_offset + meta_data_[i]->key_offset_meta);
        // PutFixed64(&meta_str, meta_data_[i]->total_size);
        PutFixed64(&meta_buffer_, meta_data_[i]->data_offset);

        // key_size + key + kv_offset + meta_key_offset
        meta_size += 8 + key.size() + 8 + 8; 
    }
    assert(meta_size == MetaDataSize());
    Status s;
    s = file->Append(buffer_);
    
    if(s.ok()) {
        s =  file->Append(meta_buffer_);
    } else {
        Log(option_.info_log, "table append data failed");
        Log(option_.info_log, s.ToString().data());
    }

    if(s.ok()) {
        s = file->Sync();
        // s = file->Append(meta_index_str);
    } else {
        Log(option_.info_log, "table append meta data failed");
        Log(option_.info_log, s.ToString().data());
    }

    if(!s.ok()) {
        Log(option_.info_log, "table build split failed");
        Log(option_.info_log, s.ToString().data());
        // delete file;
        return s;
    }

    return Status::OK();
}

uint64_t TableBuilderNVMSplit::FileSize() const {return RawDataSize() + MetaDataSize();} 

uint64_t TableBuilderNVMSplit::RawDataSize() const { return buffer_.size();}

uint64_t TableBuilderNVMSplit::MetaDataSize () const{ return meta_offset_;}

uint64_t TableBuilderNVMSplit::NumEntries() const { return meta_data_.size(); }

Slice TableBuilderNVMSplit::Data() const { 
    assert(closed_);
    return Slice(buffer_.data(), RawDataSize());
}

Slice TableBuilderNVMSplit::KeyData() const {
    assert(closed_);
    return Slice(meta_buffer_.data(), MetaDataSize());
}

Status TableBuilderNVMSplit::status() const {
    return Status::OK();
}
}




