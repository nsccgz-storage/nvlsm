
#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_NVM_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_NVM_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "leveldb/cache.h"
#include "table_nvm/table_nvm.h"
#include "port/port.h"
#include <vector>

namespace leveldb {
class Env;
typedef char CacheType;
const CacheType TableCacheType = 'T';
const CacheType FileCacheType = 'F';
const CacheType SegCacheType = 'S';

class TableCacheNVM {
public:
    TableCacheNVM(const std::string &dbname, const Options& option, int entries);
    ~TableCacheNVM();


    Iterator* NewIterator(const ReadOptions& options, const FileMetaData* file_meta,
                            TableNVM** tableptr = nullptr);

    Status Get(const ReadOptions& options, const FileMetaData* file_meta,
                  const Slice& k, void* arg,
                void(*handle_result)(void*, const Slice&, const Slice&));

    std::vector<Iterator*> NewIndexIterator(const FileMetaData* file_meta );

    void Evict(CacheType type, uint64_t file_number);

    Status GetMidKeys(const FileMetaData* f, std::vector<SegSepKeyData> & left, std::vector<SegSepKeyData>& right);
private:
    // maybe we can move findTable to public, since tablebulder need to index data 
    // of the table.
    // Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
    
    Status FindTableNVM(const FileMetaData* file_meta, Cache::Handle** handle);
    Status FindSegment(const SegmentMeta* segment, Cache::Handle**);

    Status FindFile(uint64_t file_num, Cache::Handle** handle);

    Env* const env_;
    const std::string dbname_;
    const Options& options_;
    // Cache* cache_;
    Cache* table_cache_;
    Cache* seg_cache_;
    Cache* file_cache_;
    const std::string nvm_path_;


};


}


#endif