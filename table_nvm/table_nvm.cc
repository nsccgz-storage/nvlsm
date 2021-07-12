#include <string>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include "table_nvm.h"


using namespace pmem::obj;

namespace leveldb {

class TableNVMIterator: public Iterator {
public:
    explicit TableNVMIterator(const TableNVM*tablenvm): tablenvm_(tablenvm) {
        bucket_pos_ = -1;
    }

    TableNVMIterator(const TableNVMIterator&) = delete;
    TableNVMIterator& operator=(const TableNVMIterator&) = delete;

    ~TableNVMIterator() override = default;

    bool Valid() const override {
        return bucket_pos_ >=0 && bucket_pos_ <= tablenvm_->buckets_.size()-1 
                && bucket_index_ >=0 && bucket_index_ <= tablenvm_->buckets_[bucket_pos_].size()-1;
        
    }
    void Seek(const Slice& k) override {
        
    }
    void SeekToFirst() override {
        bucket_pos_ = 0;
        bucket_index_ = 0;
    }
    void SeekToLast() override {
        bucket_pos_ = tablenvm_->buckets_.size() - 1;
        bucket_index_ = tablenvm_->buckets_[bucket_pos_].size()-1;
    }//{pos_ = tablenvm_->cur_item_num_;}
    void Next() override {
        assert(Valid());
        //cur_item_ = cur_item_->next_item.get();

    }
    void Prev() override {}
    Slice key() const override{
        //assert(Valid());
        return tablenvm_->buckets_[bucket_pos_][bucket_index_].key;
    }
    Slice Value() const override {

        return Slice(tablenvm_->buckets_rep_[bucket_pos_].data(), tablenvm_->buckets_[bucket_pos_])
        
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
    const TableNVM *tablenvm_;
    int bucket_pos_;
    int bucket_index_;
    string key_;
    Slice val_;
    const char * const data_;
    //item *cur_item_; // ths item pointer in the bucket list

};

TableNVM::TableNVM(pool_base &pop, const OptionsNvm &nvmoption, const InternalKeyComparator *comp )
    :option_nvm_(nvmoption), comp_(comp) {
    
    //buckets_num_ = 0;
    transaction::exec_tx(pop, [&] {
        //cur_item_num_ = 0;
        //num_item_limit_ = nvmoption.Num_item_table;
        //const uint item_num = nvmoption.Num_item_table;
        //items_ = make_persistent<item>();
        //item_size_limit_ = nvmoption.Item_size;
        

    });
    

}

Status TableNVM::Get(pool_base &pop, const Slice &key, void *arg,
            void (*handle_result)(void*, const Slice&, const Slice&)) {
    //auto pool = pmem::obj::pool_by_vptr(this);
    Iterator *iter = NewIterator();
    assert(!iter->Valid());
    iter->Seek(key);
    if(iter->Valid()) {

    }
    return Status::OK();

}



Status TableNVM::Add(const Slice& key, const Slice& value) {
    // auto pool = pmem::obj::pool_by_vptr(this);

    // transaction::exec_tx(pool, [&]{
    //     assert()
    // });
    // assert(cur_item_num_ < num_item_limit_); 
    InternalKey ikey;
    bool decode_status = ikey.DecodeFrom(key);
    if(!decode_status) return Status::Corruption("tablenvm", "add");

    // locate the bucket that this key appended
    int bucket_idx = find_less_or_equal_idx(ikey);

    persistent_ptr<pmem::obj::string> buffer = &(buckets_rep_[bucket_idx]);
    PutVarint32NVM(buffer.get(), key.size());
    PutVarint32NVM(buffer.get(), value.size());

    buffer->append(key.data(), key.size());
    buffer->append(value.data(), value.size());
    return Status::OK();

}

int TableNVM::find_less_or_equal_idx(const InternalKey &ikey) {
    int left_idx = 0;
    int right_idx = buckets_keys.size();
    while(left_idx < right_idx) {
        int mid_idx = (left_idx + right_idx) / 2;
        const pmem::obj::string &head_ikey = buckets_keys[mid_idx].key;
        Slice internal_key(head_ikey);
        InternalKey mid_internal_key;
        mid_internal_key.DecodeFrom(internal_key);
        if(comp_->Compare(mid_internal_key, ikey) > 0) {
            right_idx = mid - 1;
        } else {
            left_idx = mid;
        }
    }

    return left_idx;
}



Iterator *TableNVM::NewIterator() const {
    return nullptr;
}
}