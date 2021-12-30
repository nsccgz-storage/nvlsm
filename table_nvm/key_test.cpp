#include <string>
#include <iostream>

using namespace std;


class MyKey {
public:
    MyKey() {

    }
    MyKey(std::string s, int *num): rep_(s) {
        num_ = new int;
        *num_ = *num;
    }

    std::string getStr() {
        return rep_;
    }

    int getNum() {
        return *num_;
    }
private:
    int *num_;
    std::string rep_;
};


int main() {
    int val = 3;
    MyKey k1("123", &val);

    MyKey k2(k1);

    cout << k2.getStr() << endl;
    cout << k2.getNum() << endl;
    return 0;
}