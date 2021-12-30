
#include "leveldb/options.h"
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "table_nvm/table_nvm.h"
#include <string>



int main() {
    const leveldb::Comparator* comp = leveldb::BytewiseComparator();
    std::string a = "B2";
    std::string b = "B1";

    if(comp->Compare(a, b) > 0) {
        printf("%s > %s\n", a.data(), b.data());
    } else {
        printf("%s <= %s\n", a, b);
    }

    
    return 0;
}