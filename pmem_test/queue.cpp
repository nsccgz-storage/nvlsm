

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/shared_mutex.hpp>
#include <libpmemobj++/transaction.hpp>

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>


#include <string>


//using pmem::obj::transaction;

using namespace pmem::obj;

#define LAYOUT "queue"

enum queue_op {
    PUSH,
    POP,
    FRONT
};

struct OPpair {
    queue_op op;
    int val;
};



class queue {
public:
    int enqueue(pool_base &pop, int in) {
        transaction::exec_tx(pop, [&] {
            auto n = make_persistent<entry>();
            n->next = nullptr;
            n->val = in;

            if(head == nullptr) {
                head = tail = n;
            }else {
                tail->next  = n;
                tail = n;
            }

        });
    }
    int front() {
        if(head == nullptr) return -1;
        return head->val;
    }
    void dequeue(pool_base &pop) {
        transaction::exec_tx(pop, [&] {
            if(head == nullptr) transaction::abort(EINVAL);

            auto n = head->next;

            delete_persistent<entry>(head);
            head = n;

            if(head == nullptr) tail = nullptr;
            
        });

    }

private:

struct entry {
    p<int> val;
    persistent_ptr<entry> next;
};

    persistent_ptr<entry> head;
    persistent_ptr<entry> tail;

};

int main(int argc, char*argv[]) {
    pool<queue> pop;
    persistent_ptr<queue> q;

    const char *path = "/pmem/pmemqueue";
    try{
        if(access(path, F_OK) == -1) {
            pop = pool<queue>::create(path, LAYOUT, PMEMOBJ_MIN_POOL);
        } else {
            pop = pool<queue>::open(path, LAYOUT);
        }
        q = pop.get_root();
    } catch (const pmem::pool_error &e){
        std::cerr << "exception:" << e.what() << std::endl;
        return 1;
    } catch(const pmem::transaction_error &e) {
        std::cerr << "exception:" << e.what() <<std::endl;
        return 1;
    }


    const int testnum = 6;
    OPpair ops[testnum] = {{PUSH, 3}, {PUSH, 4}, {FRONT, 0},  {POP, 0}, {PUSH, 8}, {FRONT, 0}};
    int ans[2] = {3, 4};
    for(int i=0; i<testnum; i++) {
        if(ops[i].op == PUSH) {
            q->enqueue(pop, ops[i].val);
        } else if(ops[i].op == POP) {
            q->dequeue(pop);
        } else if(ops[i].op == FRONT) {
            int front = q->front();
            std::cout << "front of the queue is " << front << std::endl;
        }
    }

    return 0;

}