#include "table_nvm/nvmkvmap.h"

#include "db/dbformat.h"

#include "gtest/gtest.h"
namespace leveldb {
class FuckTest : public testing::Test {
 public:
  FuckTest() { map_ = new NVMKeyValueMapNaive(); }
  ~FuckTest() { delete map_; }

  void Test() {
    uint64_t fidx = 32;
    uint64_t idx = 65;
    // NVMKeyValueMapNaive* map = new NVMKeyValueMapNaive();
    std::string a = "test";
    Slice key(a);
    InternalKey ikey(key, 32, kTypeValue);
    map_->Insert(ikey.Encode(), 32, 65);

    uint64_t t_fidx;
    uint64_t tidx;

    ASSERT_TRUE(map_->Get(ikey.Encode(), &t_fidx, &tidx));
    ASSERT_EQ(fidx, t_fidx);
    ASSERT_EQ(idx, tidx);
  }

 private:
  NVMKeyValueMap* map_;
};
TEST_F(FuckTest, Insert) { Test(); }

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
