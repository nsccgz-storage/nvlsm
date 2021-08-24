
#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_NVM_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_NVM_H_

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "table_nvm/table_nvm.h"
#include "port/port.h"

namespace leveldb {
class Env;

class TableCacheNVM {
public:
    TableCacheNVM(const std::string &dbname, const Options& option, int entries);
    ~TableCacheNVM();


    Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                            uint64_t file_size, TableNVM** tablepotr = nullptr);

    Status Get(const ReadOptions& options, uint64_t file_number,
                uint64_t file_size, const Slice& k, void* arg,
                void(*handle_result)(void*, const Slice&, const Slice&));

    void Evict(uint64_t file_number);


private:
    // maybe we can move findTable to public, since tablebulder need to index data 
    // of the table.
    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

    Env* const env_;
    const std::string dbname_;
    const Options& options_;
    Cache* cache_;


};


}


#endif