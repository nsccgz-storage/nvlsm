#include <string>
// #include <libpmemobj++/make_persistent.hpp>
// #include <libpmemobj++/p.hpp>
// #include <libpmemobj++/persistent_ptr.hpp>
// #include <libpmemobj++/pool.hpp>
// #include <libpmemobj++/transaction.hpp>
// #include <libpmemobj++/utils.hpp>

#include "leveldb/env.h"
#include "table/merger.h"
#include "table_nvm.h"



using namespace pmem::obj;

namespace leveldb {

struct TableNVM::Rep{
    ~Rep() {
        // delete 
        delete keys_meta;
    }

    Options options;
    Status status;
    RandomAccessFile* file;
    uint64_t cache_id;

    // when open 
    NVMTableMeta* keys_meta;
    MetaIndexEntry* meta;
};

class TableNVMSegIterator: public Iterator {
public:
    explicit TableNVMSegIterator(const TableNVM* table_nvm) {

    }

    bool Valid() const override {

    }

    void Seek(const Slice& k) override {

    }

    void SeekToFirst() override {

    }

    void SeekToLast() override {

    }

    void Next() override {

    }

    void Prev() override {}

    Slice key() const override {

    }
    
    Slice value() const override {

    }

    Status status() const override { return Status::OK();}

private:
    const TableNVM* table_;
    Iterator* key_meta_iter;
};

class TableNVMIterator: public Iterator {
public:
    explicit TableNVMIterator(const TableNVM*tablenvm): 
        tablenvm_(tablenvm) {
        // merging all prev segment table iterator

        
    }

    TableNVMIterator(const TableNVMIterator&) = delete;
    TableNVMIterator& operator=(const TableNVMIterator&) = delete;

    ~TableNVMIterator() override = default;

    bool Valid() const override {
        return key_index_iter_->Valid();            
    }
    void Seek(const Slice& k) override {
        
    }
    void SeekToFirst() override {
        key_index_iter_->SeekToFirst();

    }
    void SeekToLast() override {
        key_index_iter_->SeekToLast();
    }
    void Next() override {
        assert(Valid());
        key_index_iter_->Next();
        

    }
    void Prev() override {}
    Slice key() const override{
        return  key_;
    }
    Slice value() const override {

        
    }

    Status status() const override {return Status::OK();}


private:
    void binary_search(const Slice& k) {

    }

    /**
     * return pos of the bucket;
     * */
    int find_greater_than_or_equal_to(const Slice&k) {
        
    }

    void ParseNextKey() {
        key_.assign(key_index_iter_->key().data(), key_index_iter_->key().size());
        Slice v_pos_val = key_index_iter_->value();
        uint64_t offset =  DecodeFixed64(v_pos_val.data());

        Slice key_len_result;
        Status s = tablenvm_->rep_->file->Read(offset, sizeof(uint64_t), &key_len_result, nullptr);
        if(!s.ok()) {
            printf("parseNextKey read key len failed\n");
            exit(1);
        }
        uint64_t key_len = DecodeFixed64(key_len_result.data());
        offset += sizeof(uint64_t);
        Slice key;
        s = tablenvm_->rep_->file->Read(offset, key_len, &key, nullptr);
        if(!s.ok()) {

        }
        
    }

    const TableNVM *tablenvm_;

    Iterator* key_index_iter_;
    uint64_t val_offset;
    std::string key_;
    Slice val_;
};

    






Iterator *TableNVM::NewIterator(const ReadOptions& read_options) const {

    Iterator* seg_iter = new TableNVMSegIterator(this);
    if(rep_->meta->GetPrevFileNum() != 0) {
        Open()
    }
    return new TableNVMIterator(this);
}

Status TableNVM::Open(const Options& options, RandomAccessFile* file,
                    uint64_t file_size, TableNVM** table, Footer* footer) {
    // read meta data 

    *table = nullptr;

    Rep* rep = new Rep();
    rep->options = options;
    rep->file = file;
    rep->keys_meta = new NVMTableMeta();
    rep->meta = new MetaIndexEntry();

    Slice* result_meta = nullptr;
    char buf[footer->meta_index_size];
    // we are assuming that we will definitely using pmem 
    Status s = file->Read(footer->meta_index_offset, footer->meta_index_size, result_meta, buf);
    if(!s.ok()) {
        delete rep;
        return s;
    }

    Slice* result_key_meta = nullptr;
    char *key_meta_buf = new char[footer->key_meta_size];
    s= file->Read(footer->key_meta_offset, footer->key_meta_size, result_key_meta, key_meta_buf);

    if(!s.ok()) {
        return s;
    }

    rep->keys_meta->DecondeFrom(result_key_meta);
    rep->meta->DecodeFrom(result_meta);

    *table = new TableNVM(rep);
    
    return Status::OK();
}
}