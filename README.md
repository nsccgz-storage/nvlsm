**NVM-LevelDB is an experimental project intended to utilize the byte-addressability of non-volatile memory.**

## key features 
1. DRAM-NVM-SSD hybrid storage architecture to combine the high-bandwidth of NVM and large capacity of SSD.
2. fine-grained compaction to keep long running compaction happening so as not to downgrade write throughput under intensive write workload.
3. adaptive compaction file selection policy to schedule the compaction. 
4. multi-threaded compaction on L1 level to utilize the high-bandwidth and parallelism of Non-volatile memory.


# Documentation

  [LevelDB library documentation](https://github.com/google/leveldb/blob/master/doc/index.md) is online and bundled with the source code.


# Getting the Source

```bash
git clone --recurse-submodules git@github.com:nsccgz-storage/nvlsm.git
```

# Building

This project supports [CMake](https://cmake.org/) out of the box.

### Build for POSIX

Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```
