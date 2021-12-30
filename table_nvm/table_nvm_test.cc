#include "table_nvm.h"

#include <map>
#include <string>
#include "gtest/gtest.h"
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include "leveldb/options.h"
#include "db/dbformat.h"
#include "nvm_util/table_builder_nvm.h"



#include "leveldb/slice.h"
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>

#define LAYOUT "table_nvm"
/**
 * test strategy
 * test get:
 *          get the key that exist in the table
 *          not exist in the table
 *          get one then insert a new one, then get
 * 
 * test insert:
 *           test if insert will hit the num_item_limit
 *           insert then get check we have insert 
 *           successfully
 *    
 **/


namespace leveldb{


class StringSink : public WritableFile {
 public:
  ~StringSink() override = default;

  const std::string& contents() const { return contents_; }

  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }

  Status Append(const Slice& data) override {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};


class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice& contents)
      : contents_(contents.data(), contents.size()) {}

  ~StringSource() override = default;

  uint64_t Size() const { return contents_.size(); }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset >= contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - offset;
    }
    std::memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
};

// An STL comparator that uses a Comparator
namespace {
struct STLLessThan {
  const Comparator* cmp;

  STLLessThan() : cmp(BytewiseComparator()) {}
  STLLessThan(const Comparator* c) : cmp(c) {}
  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
};
}  // namespace

typedef std::map<std::string, std::string, STLLessThan> KVMap;

// struct root {
//   persistent_ptr<TableNVM> table;
// };



  // Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp) : data_(STLLessThan(cmp)) {}
  virtual ~Constructor() = default;

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, std::vector<std::string>* keys,
              KVMap* kvmap) {
    *kvmap = data_;
    keys->clear();
    for (const auto& kvp : data_) {
      keys->push_back(kvp.first);
    }
    data_.clear();
    Status s = FinishImpl(options, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options, const KVMap& data) = 0;

  virtual Iterator* NewIterator() const = 0;

  const KVMap& data() const { return data_; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

 private:
  KVMap data_;
};


class TableConstructor : public Constructor {
public:
  TableConstructor(const Comparator *cmp)
    : Constructor(cmp), comp_(cmp)
  {

  }

  ~TableConstructor() {
    //delete table_nvm_;
  }

  Status FinishImpl(const Options& options, const KVMap& data) override {
    // turn to internal key
    // cast to slicenvm
    int seq = 1;
    TableBuilderNVM builder(options, root_->table);

    for(auto kvp: data){
      InternalKey k{kvp.first, seq++, kTypeValue};
      //SliceNVM val{kvp.second};
      Slice val{kvp.second};
      builder.Add(k.Encode(), val);
      //root_->table->Add(pop, k, val);
    }
    
    std::cout << "finish impl\n";
    return Status::OK();

  }

  Iterator* NewIterator() const override {
    return root_->table->NewIterator();
  }


private:
  const InternalKeyComparator comp_;
  StringSource *source;
  TableNVM *table_;
  bool fileExists(const char *path) {
    return access(path, F_OK) != -1;
  }
  // pool<root>  openPool(const char *path, std::string layout) {
    
  //   //persistent_ptr<TableNVM> q;
  //   pool<root> pop;
  //   try{
  //       if(!fileExists(path)) {
  //         pop = pool<root>::create(path, LAYOUT, PMEMOBJ_MIN_POOL);
  //       } else {
  //         pop = pool<root>::open(path, LAYOUT);
  //       }
  //   } catch (const pmem::pool_error &e){
  //       std::cerr << "exception:" << e.what() << std::endl;
  //       exit(1);
  //   } catch(const pmem::transaction_error &e) {
  //       std::cerr << "exception:" << e.what() <<std::endl;
  //       exit(1);
  //   }

  //   //q = pop.get_root();
  //   return pop;
  // }
  // pool<root> pop_;
  // persistent_ptr<root> root_;
  //persistent_ptr<TableNVM> table_nvm_;
  //TableNVM *table_nvm_;

};




class Harness: public testing::Test{
public:

  Harness(){
    options_ = Options();
    constructor_ = new TableConstructor(options_.comparator);
  }

  ~Harness(){
    delete constructor_;
  }
  void Add(const std::string& key, const std::string& value) {
    constructor_->Add(key, value);
  }

  void Test(void) {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);

    TestForwardScan(keys, data);
  }

  void TestForwardScan(const std::vector<std::string> &keys, const KVMap &data){
    Iterator *iter = constructor_->NewIterator();
    if(iter == nullptr) {
      std::cout<< "return iter is nullptr\n";
    }
    ASSERT_TRUE(!iter->Valid());
    std::cout << "test \n";
    iter->SeekToFirst();
    for(KVMap::const_iterator model_iter = data.begin(); 
        model_iter != data.end(); model_iter++){
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          iter++;
    }
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  }

  std::string ToString(const KVMap &data,
                      const KVMap::const_iterator &model_iter){

      if(model_iter == data.end()) {
        return "end";
      } else {
        return "'" + model_iter->first + "->" + model_iter->second + "'";
      }
  }


  std::string ToString(const Iterator *it){
    if(!it->Valid()){
      return "end";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

private:
  Options options_;
  Constructor *constructor_;

};



//  InternalKeyComparator cmp(BytewiseComparator());


// Test empty table.
TEST_F(Harness, Empty) {
  Test();
}


TEST_F(Harness, forwarTest){
  
}


}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
