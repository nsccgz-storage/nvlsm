**NVM-LevelDB is an experimental project intended to utilize the byte-addressability of non-volatile memory.**

## key features 
1. fine-grained compaction to keep long running compaction happening so as not to downgrade write throughput under intensive write workload.
2. compaction selection policy to schedule the compaction. 



# Documentation

  [LevelDB library documentation](https://github.com/google/leveldb/blob/master/doc/index.md) is online and bundled with the source code.



# Getting the Source

```bash
git clone --recurse-submodules https://github.com/google/leveldb.git
```

# Building

This project supports [CMake](https://cmake.org/) out of the box.

### Build for POSIX

Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```

### Building for Windows

First generate the Visual Studio 2017 project/solution files:

```cmd
mkdir build
cd build
cmake -G "Visual Studio 15" ..
```
The default default will build for x86. For 64-bit run:

```cmd
cmake -G "Visual Studio 15 Win64" ..
```

To compile the Windows solution from the command-line:

```cmd
devenv /build Debug leveldb.sln
```

or open leveldb.sln in Visual Studio and build from within.

Please see the CMake documentation and `CMakeLists.txt` for more advanced usage.
