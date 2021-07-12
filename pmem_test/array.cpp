
#include <iostream>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/utils.hpp>

#include <string>

#include <sys/stat.h>
#include <unistd.h>
#include <fstream>

using namespace std;
using namespace pmem::obj;

#define LAYOUT "array"

enum OP{
    INIT,
    PUSH,
    POP,
    FRONT,
    GET_MAX_LEN
};

class queue {
public:
    queue() {
        cout << "the queue constrcutor is called\n";
    }
    queue(int max_len) {
        cout << "the constrcutor is called\n";
        auto pool = pmem::obj::pool_by_vptr(this);
        transaction::exec_tx(pool, [this, &max_len] {
            max_len_ = max_len;
            arr_ = make_persistent<int[]>(max_len_);
            this->head_ = -1;
            tail_ = -1;
            arr_[0] = 50;
            cout << arr_[0] <<endl;
        });

    }

    /**
     * 0 success
     * -1 failed
     * */
    int push(int val) {
        // if full then push failed
        // else push the element and move the tail pointer
	    auto pool = pmem::obj::pool_by_vptr(this);
        transaction::exec_tx(pool, [this, &val] {
            cout << "the front element is " << this->head_ <<"\n";

            if(full()) return -1;
            if(head_ == -1){
                head_ = tail_ = 0;
            } else if(tail_ == max_len_-1 && head_ != 0) {
                tail_ = 0;
            } else {
                tail_ = tail_ + 1;
            }
            
            arr_[tail_] = val;

            return 0;

        });

    }
    void pop() {
        if(empty()) {
            cout << " the queue is empty\n";
            return;
        }
        if(head_ == tail_) {
            head_ = tail_ = -1;
        } else if(head_ == max_len_ - 1) {
            head_ = 0;
        } else {
            head_ = head_ + 1;
        }
    }
    int front() {
        cout << "the front pos is " << head_ << endl;
        if(empty()) {
            cout << "the queue is empty\n";
            return -1;
        }
        return arr_[head_.get_ro()];
    }

    int get_max_len(void ) const {
        cout << "queue max len is " << sizeof(arr_)  << endl;
        //assert(sizeof(arr_) == max_len_);
        return max_len_;
    }
    void test(void) {
        cout << "test\n";
    }

private:
    int empty() {
        if(head_ == -1) return 1;
        return 0;
    }

    int full() {
        if( (head_ == 0 && tail_ == max_len_-1) || tail_ == (head_ - 1) % (max_len_-1)) return 1;
        return 0;
    }
    persistent_ptr<int[]> arr_;
    p<int> max_len_;
    p<int> head_;
    p<int> tail_;
};

pool<queue> open_pool(string path, string layout){
    pool<queue> pop;
    if(access(path.c_str(), F_OK) == -1) {
         pop = pool<queue>::create(path, LAYOUT, PMEMOBJ_MIN_POOL);
    } else {
         pop = pool<queue>::open(path, LAYOUT);
    }
    return pop;
};

struct op_val_pair {
    OP op;
    string val;
};

/*
    init -len
    push -val
    pop 
    front
*/
OP get_op(const char *arg) {
    int numops = 5;

    string ops[numops] = {"init", "push", "pop", "front", "getmaxlen"};

    for(int i=0; i < numops; i++) {
        if(strcmp(ops[i].c_str(), arg) == 0) {
            return OP(i);
        }
    }
    return OP(-1);

}


struct root {
    persistent_ptr<queue> pqueue;
};

int main(int argc, char *argv[]) {
    if(argc < 2) {
        cout << "usage: "<< argv[0] << "[ init [len] | push [val] | \
                                pop | front ]" <<endl;
        return 1;
    }

    string path = "/pmem/arrqueue";

    pool<root> pop;
    if(access(path.c_str(), F_OK) == -1) {
         pop = pool<root>::create(path, LAYOUT, PMEMOBJ_MIN_POOL);

    } else {
         pop = pool<root>::open(path, LAYOUT);
    }
    
    auto root = pop.get_root();
    if(root->pqueue == nullptr) {
        cout << "the pqueue is not yet initialized\n";
    }
    //pop = open_pool(path, LAYOUT);
    const char *argv1 = argv[1];
    if(get_op(argv1) == INIT || get_op(argv1) == PUSH) {
        if(argc < 3){
            cout << "you should input the third param\n";
            return 1;
        }
        const char *val = argv[2];
        
        int ival = atoi(val);
        if(get_op(argv1) == INIT) {
            cout << "get op init\n";
            if(ival < 0) {
                cout << "queue length can not be less than 0\n";
                return 1;
            }
            transaction::exec_tx(pop, [&]{
                root->pqueue = make_persistent<queue>(ival);
                //cout << root->get_max_len() << endl;
                //root->test();
            });


        }

        if(get_op(argv1) == PUSH) {
            transaction::exec_tx(pop, [&]{
                root->pqueue->push(ival);
                //cout << root->get_max_len() << endl;
                //root->test();
            });    
        }

    }

    if(get_op(argv1) == POP) {
        root->pqueue->pop();
    }

    if(get_op(argv1) == FRONT) {
        cout << root->pqueue->front() << endl;
    }

    if(get_op(argv1) == GET_MAX_LEN) {
        cout << root->pqueue->get_max_len() <<endl;
    }
    return 0;
}