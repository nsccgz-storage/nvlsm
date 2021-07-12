#include <string>
#include <iostream>
#include <cstdint>
#include <stdio.h>


using namespace std;


int main() {
    string s;
    char a[5] = "1234";
    s.assign(a, 3);
    //a[0] = '9';

    printf("string addr:%p, char addr:%p\n", s.c_str(), a);
//     unsigned int addr =  *((unsigned int*)s.c_str());
//     unsigned int addr2 = *(unsigned int*)a;
//     cout << addr <<endl;

//     cout << addr2 <<endl;
}