#include "table_nvm/nvmkvmap.h"

namespace leveldb {

NVMKeyValueMapNaive::NVMKeyValueMapNaive() {}

NVMKeyValueMapNaive::~NVMKeyValueMapNaive() {}

void NVMKeyValueMapNaive::Insert(Slice key, uint64_t file_id, uint64_t idx) {
  map_[key] = {file_id, idx};
}

void NVMKeyValueMapNaive::Remove(Slice key) { map_.erase(key); }

Iterator* NVMKeyValueMapNaive::GetIterator() { return nullptr; }

bool NVMKeyValueMapNaive::Get(Slice key, uint64_t* file_id, uint64_t* idx) {
  if (map_.find(key) != map_.end()) {
    *file_id = map_[key].file_id;
    *idx = map_[key].idx;
    return true;
  }
  return false;
}

}  // namespace leveldb
