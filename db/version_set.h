// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include <map>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;
class TableCacheNVM;
struct SuperCompaction;
// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {
 public:
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }
  // int NumTables(int level) const {return tables_[level].size();}

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  class InputsIndexIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;
  Iterator* NewConcatIteratorNVM(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  // void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
  //                         bool (*func)(void*, int, FileMetaData*));
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];
  // std::vector<FileMetaData*> tables_[config::kNumLevels];

  std::unordered_set<uint64_t> live_files_set;

  // Next file to compact based on seek stats.
  // FileMetaData* file_to_compact_;
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;
};

struct LevelOutput {
  // bool is_new;
  LevelOutput(uint64_t num, uint64_t f_size, InternalKey& small,
              InternalKey& large, std::vector<SegmentMeta*>& new_segs)
      : number(num), file_size(f_size), new_segments(std::move(new_segs)) {
    smallest.DecodeFrom(small.Encode());
    largest.DecodeFrom(large.Encode());
  }
  // uint64_t orig_number;
  uint64_t number;
  uint64_t file_size;  // virtual sstable size
  InternalKey smallest;
  InternalKey largest;
  std::vector<SegmentMeta*> new_segments;
};

// (], (], ... , (]
struct TableInterval {
  TableInterval(const Slice* left, const Slice* right,
                const Slice* real_left = nullptr,
                const Slice* real_right = nullptr)
      : total_size(0) {
    left_bound = right_bound = nullptr;
    real_left_bound = nullptr;
    real_right_bound = nullptr;
    if (left && !left->empty()) {
      left_bound = new InternalKey();
      left_bound->DecodeFrom(*left);
    }

    if (right && !right->empty()) {
      right_bound = new InternalKey();
      right_bound->DecodeFrom(*right);
    }
    if (real_left != nullptr) {
      real_left_bound = new InternalKey();
      real_left_bound->DecodeFrom(*real_left);
    }
    if (real_right != nullptr) {
      real_right_bound = new InternalKey();
      real_right_bound->DecodeFrom(*real_right);
    }
  }
  ~TableInterval() {
    if (left_bound != nullptr) {
      delete left_bound;
    }
    if (right_bound != nullptr) {
      delete right_bound;
    }
    if (real_left_bound != nullptr) {
      delete real_left_bound;
    }
    if (real_right_bound != nullptr) {
      delete real_right_bound;
    }
  }

  // bool new_interval;
  // uint64_t orig_number;   // only valid when new_interval is false;
  InternalKey* left_bound;   // not included
  InternalKey* right_bound;  // included in the range

  InternalKey* real_left_bound;
  // std::shared_ptr<InternalKey> real_left_bound;
  InternalKey* real_right_bound;
  // std::shared_ptr<InternalKey> real_right_bound;

  std::vector<SegmentMeta*> segments;
  uint64_t total_size;
};

struct TableCacheNVMAndInputs {
  TableCacheNVM* table_cache;
  const std::vector<FileMetaData*>* inputs;
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, TableCacheNVM* table_cache_nvm,
             const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() {
    // printf("next file numner : %lu\n", next_file_number_);
    return next_file_number_++;
  }

  // uint64_t NewSSTableNumber() { return next_sstable_number_++; }
  uint64_t NewSegNumber() { return next_seg_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  bool L1NeedCompaction();
  SuperCompaction* PickSubCompaction(int level);

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

  const char* LevelSegSummary(std::string& s) const;

  Status MakeNewFiles(Compaction* compact, std::vector<LevelOutput>& outputs,
                      port::Mutex* mu);
  // void EvictSegment(uint64_t seg_num) ;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);
  void SubSetupOtherInputs(Compaction* c);
  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Status SplitFileForInputs(const SegmentMeta* seg, Iterator* seg_iter,
                            std::vector<TableInterval*>& table_intervals,
                            port::Mutex* mu);

  void SetupTableIntervals(std::vector<TableInterval*>& intervals,
                           const std::vector<FileMetaData*>& input,
                           port::Mutex* mu, bool is_split,
                           int remain_interval_num);

  void SetupTableIntervalsUniform(std::vector<TableInterval*>& intervals,
                                  const std::vector<FileMetaData*>& input,
                                  port::Mutex* mu);
  // Iterator* NewSegKeyIterator(SegmentMeta* seg);

  Status DecodeMetaVals(Slice& val, uint64_t* key_offset,
                        uint64_t* key_data_offset);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  TableCacheNVM* const table_cache_nvm_;
  TableCacheNVMAndInputs cache_nvm_inputs_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;

  uint64_t next_seg_number_;

  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];
};

class SubCompaction {
 public:
  explicit SubCompaction(int level) : level(level) {}

 private:
  std::vector<FileMetaData*> inputs[2];
  std::vector<FileMetaData*> grandparent;
  int level;
  VersionEdit* edit;
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // FileMetaData* inputSSTable(int which, int i) const {return
  // inputs_[which][i];}

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

  bool IsSplitCompaction() const;

  bool IsLevelCompaction() const;

  uint64_t NumSubCompaction() const;

  std::vector<SubCompaction*> GetAllSubCompaction();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  // for level compaction we could put all files in "level_ + 1"
  // to inputs_[1], but it may be possible that
  // std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs
  std::vector<FileMetaData*> inputs_[2];

  std::vector<SubCompaction*> sub_compactions_;
  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  // std::vector<FileMetaData*> grandparents_;
  std::vector<FileMetaData*> grandparents_;

  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];

  bool is_split_;
  bool is_subcompaction;
};
struct SuperCompaction {
  ~SuperCompaction() {
    for (int i = 0; i < sub_compactions.size(); i++) {
      delete sub_compactions[i];
    }
  }

  void ReleaseInputs() {
    if (input_version != nullptr) {
      input_version->Unref();
    }
  }
  std::vector<Compaction*> sub_compactions;
  Version* input_version;
};
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
