#include <string>
#include <iostream>
#include <thread>
#include <utility>

using namespace std;

string test = "leveldb";

void extend_string(string &s, int extend_length) {
    for(int i=0; i < extend_length; i++) {
        s.append("3");
    }
}

void  read_string(const string &s) {
    string subs = s.substr(0, 3);
    cout << "length of s is" << s.length() << endl;
    cout << subs <<endl;
}

int main() {
    int extend_length = 100000000;
    thread t1(extend_string, std::ref(test), extend_length);
    thread t2(read_string, std::ref(test));
    t1.join();
    t2.join();


    return 0;
}