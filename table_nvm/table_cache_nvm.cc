#include <db/filename.h>
#include <table_nvm/table_cache_nvm.h>
#include <db/version_edit.h>
#include <leveldb/env.h>


namespace leveldb {
struct SegmentAndFile{
  Segment* seg;
  RandomAccessFile* file;
};
static void DeleteSegmentAndFile(const Slice& key, void* value) {
  // SegmentAndFile* sf = reinterpret_cast<SegmentAndFile*>(value);
  // delete sf->file;
  // delete sf->seg;
  Segment* seg = reinterpret_cast<Segment*>(value);
  delete seg;

}

static void DeleteEntryNVM(const Slice& key, void *value) {
  TableNVM* t = reinterpret_cast<TableNVM*>(value);
  delete t;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache *cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(arg2);

  cache->Release(handle);
}

TableCacheNVM::TableCacheNVM(const std::string &dbname, const Options& option, int entries)
: env_(option.env),
  dbname_(dbname),
  options_(option),
  cache_(NewLRUCache(entries)) {}

Status TableCacheNVM::Get(const ReadOptions& options,const FileMetaData* file_meta, 
                 const Slice& k, void* arg,
                void(*handle_result)(void*, const Slice&, const Slice&)){
  // find table 

  // for each segment of the table seek given key
  // the naive implmentation may seek in iterator of the table,
  // but that will be a little bit slower 
  Cache::Handle* handle = nullptr;
  Status s = FindTableNVM(file_meta, &handle);
  if(s.ok()) {
    TableNVM* t = reinterpret_cast<TableNVM*>(cache_->Value(handle));
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  } 
  return s; 
}

std::vector<Iterator*> TableCacheNVM::NewIndexIterator(const FileMetaData* file_meta){
    // call find table 
    // call newIndexIterator of table
  Cache::Handle* handle = nullptr;
  Status s = FindTableNVM(file_meta, &handle);
  if(!s.ok()) {
    return {NewErrorIterator(s)};
  }
  TableNVM* table = reinterpret_cast<TableNVM*>(cache_->Value(handle));
  std::vector<Iterator*> idx_iters = table->NewIndexIterator();
  assert(!idx_iters.empty());
  idx_iters[0]->RegisterCleanup(&UnrefEntry, cache_, handle);
  // index_iters =  std::move(table->NewIndexIterator());
  // cache_->Release(handle);

  // return s;
  return idx_iters;
}

Iterator* TableCacheNVM::NewIterator(const ReadOptions& options,const FileMetaData* file,
                             TableNVM** tableptr){
    // call find table 
    // then call newIterator() method of table
    // char buf[9];
    // buf[0] = FileCacheType;
    // uint64_t file_num = file->number;
    // EncodeFixed64(buf+1, file_num);
    // Slice file_key(buf, sizeof(buf));
    // Cache::Handle* handle = cache_->Lookup(buf);
    if(tableptr != nullptr) {
      *tableptr = nullptr;
    }
    Status s;
    Cache::Handle* handle = nullptr;    
    s = FindTableNVM(file, &handle);
    if(!s.ok()) {
      printf("Find table: %d failed, return err iter\n", file->number);
      return NewErrorIterator(s);
    }

    TableNVM* t = reinterpret_cast<TableNVM*>(cache_->Value(handle));
    Iterator* result = t->NewIterator(options);
    result->RegisterCleanup(UnrefEntry, cache_, handle);
    if(tableptr != nullptr) {
      *tableptr = t;
    }
  
    return result;
    // TableNVM* table_nvm = reinterpret_cast<TableNVM*>(handle);

    // return table_nvm->NewIterator(options);     
}

void TableCacheNVM::Evict( CacheType type, uint64_t number){
    char buf[9];
    buf[0] = type;
    EncodeFixed64(buf+1, number);
    Slice key(buf, sizeof(buf));
    cache_->Erase(key);
}


// Status TableCacheNVM::FindTable(CacheType type, uint64_t file_number,  Cache::Handle** handle) {
//    // if found just return 

//    // else, we may need to open the table or open the segments  
// }

Status TableCacheNVM::FindTableNVM(const FileMetaData* file_meta, Cache::Handle** handle) {
  Status s;
  char buf[9];
  buf[0] = TableCacheType;
  EncodeFixed64(buf+1, file_meta->number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if(*handle == nullptr) {
    TableNVM* table = nullptr;
    // s = TableNVM::Open(options_, file_meta, env_, &table);
    std::vector<Segment*> segs;
    for(int i=0; i < file_meta->segments.size(); i++) {
      Cache::Handle* seg_handle = nullptr;
      s = FindSegment(file_meta->segments[i], &seg_handle);
      if(!s.ok()) {
        // if not ok, then we need to delete all segments in the vector.


        return s;
      }
      
      segs.push_back(reinterpret_cast<Segment*>(cache_->Value(seg_handle)));
    } 

    s = TableNVM::Open(options_, segs, &table);
    if(!s.ok()) {
      assert(table == nullptr);
    } else {
      *handle = cache_->Insert(key, table, 1, DeleteEntryNVM);
    }
  }

  return s;
}
static void DeleteFile(const Slice& key, void* value) {
  RandomAccessFile* file = reinterpret_cast<RandomAccessFile*>(value);
  delete file;
}
Status TableCacheNVM::FindFile(uint64_t file_number, Cache::Handle** handle) {
    uint64_t file_num = file_number;
    char file_num_buf[1 + sizeof(file_num)];
    file_num_buf[0] = FileCacheType;
    EncodeFixed64(file_num_buf+1, file_num);
    Slice file_key(file_num_buf, sizeof(file_num_buf));
    // printf("find file key: %s\n", file_key.data());
    Cache::Handle* file_handle = cache_->Lookup(file_key);
    Status s;
    if(file_handle == nullptr) {
      std::string fname = TableFileName(dbname_, file_number);
      RandomAccessFile* result = nullptr;
      // printf("open file: %s\n", fname.data());
      // s = env_->NewRandomAccessFile(fname, &result);
      s = env_->NewRandomAccessFile2(fname, &result);
      if(!s.ok()) {
        printf("find file failed\n");
      }
      file_handle = cache_->Insert(file_key, result, 1, &DeleteFile);
    }
    *handle = file_handle;
    return s;

}

Status TableCacheNVM::FindSegment(const SegmentMeta* segment_meta, Cache::Handle** handle) {

  char seg_buf[9];
  seg_buf[0] = SegCacheType;
  uint64_t seg_num = segment_meta->seg_number;
  EncodeFixed64(seg_buf+1, seg_num);
  Slice key(seg_buf, sizeof(seg_buf));
  
  Cache::Handle* seg_handle = cache_->Lookup(key);
  Status s;
  if(seg_handle == nullptr) {

    Segment* segment = nullptr;
    Cache::Handle* file_handle = nullptr;
    s = FindFile(segment_meta->file_number, &file_handle);
    if(!s.ok()) {
      return s;
    }
    // RandomAccessFile* data_file = nullptr;
    // RandomAccessFile* key_file = nullptr;
    // s = env_->NewRandomAccessFile2(fname, segment_meta->data_offset, segment_meta->data_size, &data_file);
    // s = env_->NewRandomAccessFile2(fname, segment_meta->key_offset, segment_meta->key_size, &key_file);
    // if(!s.ok()) {
    //   printf("segment open file failed\n");
    //   return s;
    // }
    RandomAccessFile* file = reinterpret_cast<RandomAccessFile*>(cache_->Value(file_handle));
    // s = Segment::Open(options_, data_file, segment_meta->data_offset, key_file, segment_meta->key_size, &segment);
    // printf("try to open seg: %lu, corresponding file num: %lu\n", segment_meta->seg_number, segment_meta->file_number);
    s = Segment::Open(options_, file, segment_meta->data_offset, segment_meta->data_size, segment_meta->key_offset, segment_meta->key_size, &segment);
    if(!s.ok()) {
      assert(segment == nullptr);
      delete file;
      // delete data_file;
      // delete key_file;
      return s;
    }

    // SegmentAndFile* sf = new SegmentAndFile;
    // sf->file = data_file;
    // sf->seg = segment;
    seg_handle = cache_->Insert(key, segment, 1, &DeleteSegmentAndFile); 

  }


  *handle = seg_handle;

  return Status::OK();
}


TableCacheNVM::~TableCacheNVM() {
  delete cache_;  
}

}