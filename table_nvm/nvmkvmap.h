

#include <map>

// #include "leveldb/iterator.h"
// #include "leveldb/slice.h"

// #include "table_nvm/table_nvm.h"
#include "db/dbformat.h"
namespace leveldb {

struct cmpSlice {
  bool operator()(const Slice& akey, const Slice& bkey) {
    auto* byte_comp = BytewiseComparator();
    int r = byte_comp->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
    // int r = byte_comp->Compare(akey, bkey);
    // return r > 0;
    if (r == 0) {
      const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
      const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
      if (anum > bnum) {
        r = -1;
      } else if (anum < bnum) {
        r = +1;
      }
    }
    return r < 0;
    // if (r < 0) {
    //   return true;
    // } else {
    //   return false;
    // }
  }
};
class NVMKeyValueMap {
 public:
  NVMKeyValueMap() {}

  virtual ~NVMKeyValueMap() = 0;

  virtual void Insert(Slice key, uint64_t file_id, uint64_t idx) = 0;

  virtual void Remove(Slice key) = 0;

  virtual Iterator* GetIterator() = 0;

  virtual bool Get(Slice key, uint64_t* file_id, uint64_t* idx) = 0;
};
NVMKeyValueMap::~NVMKeyValueMap() {}

class NVMKeyValueMapNaive : public NVMKeyValueMap {
 public:
  NVMKeyValueMapNaive() {}
  ~NVMKeyValueMapNaive() override {}
  // should be user key instead of internal key;
  // store delete action .

  void Insert(Slice key, uint64_t file_id, uint64_t idx) override {
    map_[key] = {file_id, idx};
  }

  void Remove(Slice key) override { map_.erase(key); }

  Iterator* GetIterator() override { return nullptr; }

  bool Get(Slice key, uint64_t* file_id, uint64_t* idx) override {
    if (map_.find(key) != map_.end()) {
      *file_id = map_[key].file_id;
      *idx = map_[key].idx;
      return true;
    }
    return false;
  }

 private:
  struct ValueAndFileId {
    uint64_t file_id;
    uint64_t idx;
  };
  std::map<Slice, struct ValueAndFileId, cmpSlice> map_;
};

// NVMKeyValueMapNaive::NVMKeyValueMapNaive() {}

// NVMKeyValueMapNaive::~NVMKeyValueMapNaive() {}

// void NVMKeyValueMapNaive::Insert(Slice key, uint64_t file_id, uint64_t idx)
// {}

// void NVMKeyValueMapNaive::Remove(Slice key) {}

// Iterator* NVMKeyValueMapNaive::GetIterator() { return nullptr; }

// bool NVMKeyValueMapNaive::Get(Slice key, uint64_t* file_id, uint64_t* idx) {}
}  // namespace leveldb
