//#include "leveldb/slice.h"
#include <libpmemobj.h>
//#include <libpmem.h>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/container/string.hpp>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <string>

using namespace pmem::obj;
//using namespace leveldb;

#define LAYOUT "testarr"
#define PATH "/pmem/test2"

struct pstruct {
    pstruct() {
        a = 0;
        b = 0;
    }
    p<int> a;
    p<int> b;
};

struct root{
    persistent_ptr<pstruct[]> ps;
    persistent_ptr<pmem::obj::string> s;
    p<pmem::obj::vector<int>> pv;
};

class mystring {

    mystring(pool_base &pop) {

        capacity_ = 1024;
        cursize_ = 0;
        transaction::run(pop, [&]{
            rep_ = make_persistent<char[]>(capacity_);
        }); 
    }

    void append( const char* in) {
        auto pop = pmem::obj::pool_by_vptr(this);
        uint len = strlen(in);
        if(len + cursize_ >= capacity_) {
            transaction::run(pop, [&] {
                //delete_persistent();
                capacity_ *= 2;
                persistent_ptr<char[]> new_one = make_persistent<char[]>(capacity_);
                pmemobj_memcpy_persist(pop.get_handle(), new_one.get(), rep_.get(), cursize_);
            });
        } 

        pmemobj_memcpy_persist(pop.get_handle(), rep_.get()+cursize_, in, len);
    }

    //void append() 

    const char* c_str() {
        return rep_.get();
    }

    mystring sub_str(int start, int len) {
        if(start >= cursize_) throw("out of range");
        if(start + len > cursize_) len = cursize_  - start;

    }

    char at(int pos) {

    }


private:
    p<uint> capacity_;
    p<uint> cursize_;
    persistent_ptr<char[]> rep_;
};

void show(pstruct *p) {
    std::cout << p->a << " " << p->b << std::endl;
}

int file_exists(const char*path) {
    return access(path, F_OK) != -1;
}

// test if original slice can be ppersisted on pmem  =======>>>> it can't
int main() {
    //const char* path = "/pmem/test";
    std::string s1 = "/pmem/test2";
    std::string layout = "testarr";

    pool<root> pop;

    try{
        if(access(PATH, F_OK) == -1) {
            pop = pool<root>::create(s1, layout, PMEMOBJ_MIN_POOL, S_IRWXU);
        } else {
            pop = pool<root>::open(s1, layout);

        }

    } catch (const pmem::pool_error &e){
        std::cerr << "exception:" << e.what() << std::endl;
        return 1;
    } catch(const pmem::transaction_error &e) {
        std::cerr << "exception:" << e.what() <<std::endl;
        return 1;
    }
    auto root = pop.root();
    transaction::run(pop, [&] {
        if(root->ps == nullptr)  root->ps = make_persistent<pstruct[]>(3);
        if(root->s == nullptr) root->s = make_persistent<pmem::obj::string>();
        
        if(root->s == nullptr) {
            std::cout << "string is null\n";
        }
        //root->ps[0].a = 1;
        root->ps[0].a.get_rw()++;
        root->ps[0].b.get_rw()++;
        root->s->append("a");
        root->pv.get_rw().push_back(3);
    });
    show(&(root->ps[0]));
    std::cout << root->s->c_str() <<"\n";

    for(auto iter: root->pv.get_ro()) {
        std::cout << iter << " ";
    }
    std::cout << "\n";
    //std::cout << root->s.get() << std::endl;
    return 0;
}