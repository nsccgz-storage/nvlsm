// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <string>

#include "leveldb/env.h"
#include "leveldb/table_builder.h"

#include "port/port.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

#include "table_nvm/table_cache_nvm.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static uint64_t MaxFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t max_file_size = 0;
  for (int i = 0; i < files.size(); i++) {
    if (files[i]->file_size > max_file_size) {
      max_file_size = files[i]->file_size;
    }
  }
  return max_file_size;
}

static uint64_t MaxSegNum(const std::vector<FileMetaData*>& files) {
  uint64_t max_seg_num = 0;
  for (int i = 0; i < files.size(); i++) {
    uint64_t cur_seg_num = files[i]->segments.size();
    if (cur_seg_num > max_seg_num) {
      max_seg_num = cur_seg_num;
    }
  }
  return max_seg_num;
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = 10. * 1048576.0;
  if (level == 1) {
    return result * 10.;
  }

  // Result for both level-0 and level-1
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

class Version::InputsIndexIterator : public Iterator {
 public:
  InputsIndexIterator(const InternalKeyComparator& icmp,
                      const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {}

  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }

  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }

  void Next() override {
    assert(Valid());
    index_++;
  }

  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();
    } else {
      index_--;
    }
  }

  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }

  Slice value() const override {
    assert(Valid());
    EncodeFixed32(buf_, index_);
    return Slice(buf_, sizeof(buf_));
  }

  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;
  mutable char buf_[4];
};

// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

static Iterator* GetTableIterator(void* arg, const ReadOptions& options,
                                  const Slice& table_idx) {
  TableCacheNVMAndInputs* cache_inputs =
      reinterpret_cast<TableCacheNVMAndInputs*>(arg);
  if (table_idx.size() != 4) {
    printf("get table iter failed, table idx size not 4\n");
    return NewErrorIterator(
        Status::Corruption("TableReader NVM invoked with unexpected value"));
  } else {
    uint32_t index = DecodeFixed32(table_idx.data());
    return cache_inputs->table_cache->NewIterator(
        options, (*(cache_inputs->inputs))[index], nullptr);
  }
}
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

static void CleanupDeleteCacheAndInputs(void* arg1, void* arg2) {
  TableCacheNVMAndInputs* cache_inputs =
      reinterpret_cast<TableCacheNVMAndInputs*>(arg1);
  delete cache_inputs;
}
Iterator* Version::NewConcatIteratorNVM(const ReadOptions& options,
                                        int level) const {
  TableCacheNVMAndInputs* cache_inputs = new TableCacheNVMAndInputs;
  cache_inputs->inputs = &files_[level];
  cache_inputs->table_cache = vset_->table_cache_nvm_;
  Iterator* iter =
      NewTwoLevelIterator(new InputsIndexIterator(vset_->icmp_, &files_[level]),
                          &GetTableIterator, cache_inputs, options);
  iter->RegisterCleanup(CleanupDeleteCacheAndInputs, cache_inputs, nullptr);
  return iter;
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  // for (size_t i = 0; i < files_[0].size(); i++) {
  //   iters->push_back(vset_->table_cache_->NewIterator(
  //       options, files_[0][i]->number, files_[0][i]->file_size));
  // }
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_nvm_->NewIterator(options, files_[0][i], nullptr));
  }
  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  if (!files_[1].empty()) {
    iters->push_back(NewConcatIteratorNVM(options, 1));
  }
  for (int level = 2; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      if (level == 0 || level == 1) {
        state->s = state->vset->table_cache_nvm_->Get(
            *state->options, f, state->ikey, &state->saver, SaveValue);
      } else {
        state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                  f->file_size, state->ikey,
                                                  &state->saver, SaveValue);
      }

      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          // for(int i=0; i < f->segments.size(); i++) {
          //   f->segments[i]->refs--;
          //   if(f->segments[i]->refs <= 0) {
          //     vset->
          //   }
          // }

          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      // todo: update constructor function,
      // this does not copy segments of orginal FileMetaData
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;
      if (level == 0 || level == 1) {
        assert(f->segments.size() > 0);
        for (int i = 0; i < f->segments.size(); i++) {
          f->segments[i]->refs++;
        }
      }

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      // if(level == 0 || level == 1) {
      //   for(int i=0; i < f->segments.size(); i++) {
      //     f->segments[i]->refs++;
      //   }
      // }

      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache, TableCacheNVM* table_cache_nvm,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      table_cache_nvm_(table_cache_nvm),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr),
      next_seg_number_(2) {
  AppendVersion(new Version(this));
  cache_nvm_inputs_.inputs = nullptr;
  cache_nvm_inputs_.table_cache = table_cache_nvm_;
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  // maybe we won't generate new file number
  edit->SetNextFile(next_file_number_);
  // edit->SetNextSSTable(next_sstable_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }
    if (level == 1) {
      const uint64_t max_file_bytes = MaxFileSize(v->files_[level]);
      const uint64_t file_max_seg_num = MaxSegNum(v->files_[level]);
      double level1_score =
          static_cast<double>(max_file_bytes) / config::kL1_MaxSegBytes;
      double level1_seg_num_score =
          static_cast<double>(file_max_seg_num) / config::kL1_MaxSegNum;
      if (level1_score > score) {
        score = level1_score;
      }

      if (level1_seg_num_score > score) {
        score = level1_seg_num_score;
        // printf("seg num too high: %ld\n", file_max_seg_num);
      }
    }
    // #ifdef NDEBUG
    // printf("level:%d, score:%f\n", level, score);
    // #endif
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSegSummary(std::string& s) const {
  // std::snprintf(scratch->buffer, sizeof(scratch->buffer), "")
  // printf("LevelSegSumary: level1 file num:%d \n",
  // current_->files_[1].size());
  s.append("level 1 file sizes: [");
  for (int i = 0; i < current_->files_[1].size(); i++) {
    uint64_t seg_count = current_->files_[1][i]->segments.size();
    uint64_t file_size_mb = current_->files_[1][i]->file_size / 1024 / 1024;
    s.append(std::to_string(file_size_mb) + "/" + std::to_string(seg_count) +
             ",");
  }
  s.append("] \n");
  // for (int i = 0; i < current_->files_[1].size(); i++) {
  //   s.append("File_num: " + std::to_string(current_->files_[1][i]->number) +
  //            "\n");
  //   s.append(
  //       "file total size:" +
  //       std::to_string(current_->files_[1][i]->file_size) + " segment count:"
  //       + std::to_string(current_->files_[1][i]->segments.size()) + "\n");
  //   s.append("each segment info:\n");
  //   // const auto& segs = current_->files_[1][i]->segments;
  //   // for (int j = 0; j < segs.size(); j++) {
  //   //   // s.append("seg_num: %d, file_num:")
  //   //   s.append("seg_num:" + std::to_string(segs[j]->seg_number) +
  //   //            "file_num:" + std::to_string(segs[j]->file_number) +
  //   //            "seg_size:" + std::to_string(segs[j]->seg_size) + "\n");
  //   // }
  //   // printf("%s\n", s.data());
  // }
  return s.data();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < 2; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        for (int seg_idx = 0; seg_idx < files[i]->segments.size(); seg_idx++) {
          live->insert(files[i]->segments[seg_idx]->file_number);
        }
      }
    }
    for (int level = 2; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        // for(size_t j = 0; j < files[i]->segments.size(); j++) {
        // live->insert(files[i]->segments[j].file_number);
        live->insert(files[i]->number);
        // }
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    // FileMetaData* f = inputs[i];
    FileMetaData* f = inputs[i];
    if (i == 0) {
      // *smallest = f->smallest;
      *smallest = f->smallest;
      // *largest = f->largest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      assert(c->level() + which > 0);
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          // list[num++] = table_cache_->NewIterator(options, files[i]->number,
          // files[i]->file_size);
          list[num++] =
              table_cache_nvm_->NewIterator(options, files[i], nullptr);
        }
      } else if (c->level() == 1 && which == 0) {
        TableCacheNVMAndInputs* cache_inputs = new TableCacheNVMAndInputs;
        cache_inputs->inputs = &c->inputs_[which];
        cache_inputs->table_cache = table_cache_nvm_;
        Iterator* iter = NewTwoLevelIterator(
            new Version::InputsIndexIterator(icmp_, &c->inputs_[which]),
            &GetTableIterator, cache_inputs, options);
        iter->RegisterCleanup(CleanupDeleteCacheAndInputs, cache_inputs,
                              nullptr);
        list[num++] = iter;
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

std::vector<SubCompaction*> Compaction::GetAllSubCompaction() {
  return sub_compactions_;
}

bool VersionSet::L1NeedCompaction() {
  return current_->compaction_score_ >= 1 && current()->compaction_level_ == 1;
}
struct FileSizeAndIndex {
  int index;
  uint64_t file_size;
};
bool FileSizeAndIndexComp(const FileSizeAndIndex& f1,
                          const FileSizeAndIndex& f2) {
  return f1.file_size > f2.file_size;
}

uint64_t Compaction::NumSubCompaction() const {
  return sub_compactions_.size();
}
bool FileSizeComp(const FileMetaData* f1, const FileMetaData* f2) {
  return f1->file_size > f2->file_size;
}

SuperCompaction* VersionSet::PickSubCompaction(int level) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  assert(level == 1);
  assert(level + 1 < config::kNumLevels);
  std::string msg = "compaction level is: " + std::to_string(level);

  // pick how many subcompaction ?
  int compaction_num = 2;
  std::set<uint64_t> picked_files;
  SuperCompaction* super_compaction = new SuperCompaction();
  // Compaction* c = new Compaction(options_, level);
  // c->sub_compactions_.resize(compaction_num);
  super_compaction->input_version = current_;
  super_compaction->input_version->Ref();
  // sort the file in size order
  // pick the first file out, then SetupOtherInputs
  // remove all files in inputs[0] in the set

  // std::vector<FileSizeAndIndex> file_idxs;
  // for (int i = 0; i < current_->files_[level].size(); i++) {
  //   FileMetaData* f = current_->files_[level][i];
  //   file_idxs.push_back({i, f->file_size});
  // }
  std::vector<FileMetaData*> inputs0_sort = current_->files_[level];
  std::sort(inputs0_sort.begin(), inputs0_sort.end(), FileSizeComp);

  // for (int i = 0; i < file_idxs.size(); i++) {
  //   printf("file size: %ld, file idx: %d\n", file_idxs[i].file_size,
  //          file_idxs[i].index);
  // }
  // maintain a
  // pick

  int inputs0_idx = 0;
  // the compaction num may not be consumed
  for (int i = 0; i < compaction_num &&
                  picked_files.size() < current_->files_[level].size();
       i++) {
    // still pick files with biggest size;
    // we still have some file left in current level to pick

    for (; inputs0_idx < inputs0_sort.size(); inputs0_idx++) {
      // int actual_file_idx = file_idxs[file_size_idx].index;
      uint64_t file_num = inputs0_sort[inputs0_idx]->number;
      // current file not yet picked
      if (picked_files.find(file_num) == picked_files.end()) {
        picked_files.insert(file_num);
        // I am not sure if the content of subcompaction will be copied into the
        // vector I'lll try it out
        // SubCompaction* newsub = new SubCompaction(level);
        Compaction* sub_compaction = new Compaction(options_, level);
        sub_compaction->is_subcompaction = true;
        sub_compaction->input_version_ = current_;
        super_compaction->sub_compactions.push_back(sub_compaction);
        // c->sub_compactions_.push_back(newsub);
        // c->sub_compactions_.back().inputs[0].push_back(
        //     current_->files_[level][inputs0_idx]);
        sub_compaction->inputs_[0].push_back(
            current_->files_[level][inputs0_idx]);
        // always add files to the last sub compaction in the sub_compactions_
        // vector;
        // SubSetupOtherInputs(sub_compaction);
        SetupOtherInputs(sub_compaction);
        for (int selected_input0_idx = 0;
             selected_input0_idx < sub_compaction->inputs_[0].size();
             selected_input0_idx++) {
          picked_files.insert(
              sub_compaction->inputs_[0][selected_input0_idx]->number);
        }
        break;
      }
    }
  }

  int sub_file1_num = 0;
  int sub_fiel2_num = 0;
  // printf("file num is: %d, subc size: %d\n", current_->files_[level].size(),
  //        super_compaction->sub_compactions.size());
  for (int i = 0; i < super_compaction->sub_compactions.size(); i++) {
    sub_file1_num += super_compaction->sub_compactions[i]->inputs_[0].size();
    sub_fiel2_num += super_compaction->sub_compactions[i]->inputs_[1].size();
    // printf("subc file idx: %ld, file size: %ld ",
    //        c->sub_compactions_[i].inputs[0][0]->number,
    //        c->sub_compactions_[i].inputs[0][0]->file_size);
  }
  // printf("\n");
  assert(sub_file1_num <= current_->files_[level].size());
  assert(sub_fiel2_num <= current_->files_[level + 1].size());
  return super_compaction;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);

  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    std::string msg = "compaction level is: " + std::to_string(level);
    Log(options_->info_log, msg.data());
    c = new Compaction(options_, level);

    if (level == 1) {
      int file_idx = 0;
      uint64_t max_file_size = 0;
      for (int i = 0; i < current_->files_[level].size(); i++) {
        FileMetaData* f = current_->files_[level][i];
        if (f->file_size > max_file_size) {
          file_idx = i;
          max_file_size = f->file_size;
        }
      }
      c->inputs_[0].push_back(current_->files_[level][file_idx]);
    } else {
      // Pick the first file that comes after compact_pointer_[level]
      for (size_t i = 0; i < current_->files_[level].size(); i++) {
        FileMetaData* f = current_->files_[level][i];
        if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
          c->inputs_[0].push_back(f);
          break;
        }
      }
    }

    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

/*
else if(split_compaction) {
  // we can pick multiple tables for split compaciton.
  // this seems ok
  // but we can't pick too many tables at the same time.
  // for size compaction, usualy we pick 11 tables for one compaction.
  // for split compaction,we could probably pick 5 or 6 tables,
  // since the number of new tables after split compaction will
  // be twice of the original number.

  // 2021.8.21 what' s our purpose for doing split compaction?
  // we want to increase the get operation speed
  // and inspired by MatrixKV, the level compaction could reduce write stall
under write intensive workload
  // can we reduce write amplification?
  //


  // we are definitely need a split score in case there are too many tables in
the upper level
  // that exceed tabe limit compared to lower level.
  // and we still prefer size compaction.
  level = current_->compaction_level_;
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);
  c = new Compaction(options_, level);
  c->is_split_ = true;

  FileMetaData* max_f = current_->files_[level][0];

  for(size_t i=0; i < current_->files_[level].size(); i++) {

    FileMetaData* f = current_->files_[level][i];
    if(f->file_size > max_f->file_size) {
      max_f = f;
    }
    // if(f->raw_data_size > TableSizeLimit) {
    //   if(first_f == nullptr) {
    //     first_f = f;
    //   }
    //   if(compact_pointer_[level].empty() ||
icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
    //     // if(compact_pointer_f == nullptr) {
    //     //   compact_pointer_f = f;
    //     // }
    //     c->inputs_[0].push_back(f);
    //     break;
    //   }
    // }
  }
  c->inputs_[0].push_back(max_f);

  // if (c->inputs_[0].empty()) {
  //   assert(first_f != nullptr);
  //   c->inputs_[0].push_back(first_f);
  // }
}
*/

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

// void DoTableSplit(const FileMetaData* f, std::vector<>)
void VersionSet::SetupTableIntervalsUniform(
    std::vector<TableInterval*>& intervals,
    const std::vector<FileMetaData*>& input, port::Mutex* mu) {}

// create TableIntervals from input for new segments to be appended to
void VersionSet::SetupTableIntervals(std::vector<TableInterval*>& tmp_intervals,
                                     const std::vector<FileMetaData*>& input,
                                     port::Mutex* mu, bool split,
                                     int remain_interval_num) {
  assert(!input.empty());
  int max_idx = 0;
  uint64_t max_file_size = 0;
  std::vector<FileSizeAndIndex> file_idxs;
  for (int i = 0; i < input.size(); i++) {
    file_idxs.push_back({i, input[i]->file_size});
    if (input[i]->file_size > max_file_size) {
      max_idx = i;
      max_file_size = input[i]->file_size;
    }
  }

  int file_idxs_idx = 0;
  std::sort(file_idxs.begin(), file_idxs.end(), FileSizeAndIndexComp);
  std::unordered_set<int> file_idx_set;

  int num_to_split_file = std::min(remain_interval_num, int(input.size()));
  // printf("num to split file is %d\n", num_to_split_file);
  for (int i = 0; i < num_to_split_file; i++) {
    file_idx_set.insert(file_idxs[i].index);
  }

  // printf("file size:");
  // for (int i = 0; i < file_idxs.size(); i++) {
  //   printf("%ld ", file_idxs[i].file_size);
  // }

  // std::fprintf(stdout, "input size: %d, max file size %ld MB\n",
  // input.size(), max_file_size / 1024 / 1024);
  // we nned to do the segments split here
  // we also need boundry key of split segment  [left_key, right_key]
  for (int i = 0; i < input.size(); i++) {
    // InternalKey* left1 = nullptr;
    // InternalKey *right1= nullptr;
    // InternalKey* left2 = nullptr;
    // InternalKey* right2 = nullptr;
    Slice left1;
    Slice right1;
    Slice left2;
    Slice right2;
    if (i != 0) {
      // left = new InternalKey(tmp_intervals.back().right_bound);
      // left1 = new InternalKey();
      // left1->DecodeFrom(tmp_intervals.back()->right_bound->Encode());
      left1 = Slice(tmp_intervals.back()->right_bound->Encode());
    }

    // right1 = new InternalKey();
    // right1->DecodeFrom(input[i]->smallest.Encode());
    right1 = Slice(input[i]->smallest.Encode());

    // left2 = new InternalKey();
    // left2->DecodeFrom(input[i]->smallest.Encode());
    left2 = Slice(input[i]->smallest.Encode());

    // right2 = new InternalKey();
    // right2->DecodeFrom(input[i]->largest.Encode());
    right2 = Slice(input[i]->largest.Encode());

    uint64_t trigger_split_size =
        uint64_t(1.5 * double(config::kL1_StopSplitTrigger));
    // && input[i]->file_size > (0.6 * config::kL1_MaxSegBytes)
    bool do_split = false;
    if (remain_interval_num > 0 && file_idx_set.find(i) != file_idx_set.end()) {
      do_split = true;
      remain_interval_num--;
    }
    if (input[i]->file_size > config::kL1_MaxSegBytes) {
      // printf("file size too big: %ld\n", input[i]->file_size);
      do_split = true;
    }
    if (do_split) {
      // printf("split value is %d\n", split);
      // what if there is only one kv in the the segment ?
      // we will do a empty check, if it is empty then we will not add another
      // interval.

      // [key, key_data_offset] the second one is possibly empty
      // left_table store upper bound of left part of split table
      // right_table stores lower bound of right part of split table
      const FileMetaData* f = input[i];
      int seg_count = f->segments.size();
      // std::vector<std::pair<Slice, Slice> > left_table(seg_count);
      // std::vector<std::pair<Slice, Slice> > right_table(seg_count);
      std::vector<SegSepKeyData> left_table(seg_count);
      std::vector<SegSepKeyData> right_table(seg_count);

      Status s = table_cache_nvm_->GetMidKeys(f, left_table, right_table);
      // if new split table is empty then don't push any new interval, or if
      // some segments is empty, we dont' put that segments in the newly
      // generated table either. we need to be carefule about the total size of
      // the new segments
      uint64_t left_total_size = 0;
      uint64_t right_total_size = 0;
      bool right_empty = true;
      std::vector<SegmentMeta*> left_seg_metas;
      std::vector<SegmentMeta*> right_seg_metas;
      Slice left_max_key;
      Slice right_min_key;
      // buid left table and right table of the split table
      for (int seg_idx = 0; seg_idx < f->segments.size(); seg_idx++) {
        // we will create new segments here
        Slice cur_seg_left_key = left_table[seg_idx].key;
        const SegmentMeta* old_seg = f->segments[seg_idx];
        uint64_t left_key_size = 0;
        uint64_t left_data_size = 0;
        uint64_t cur_left_seg_size = 0;
        uint64_t cur_right_seg_size = 0;

        uint64_t end_key_offset = old_seg->key_offset + old_seg->key_size;
        uint64_t end_data_offset = old_seg->data_offset + old_seg->data_size;
        if (!cur_seg_left_key.empty()) {
          uint64_t left_key_offset, left_data_offset;
          DecodeMetaVals(left_table[seg_idx].key_data_offset, &left_key_offset,
                         &left_data_offset);

          left_key_size =
              left_table[seg_idx].next_key_offset - old_seg->key_offset;
          left_data_size =
              left_table[seg_idx].next_data_offset - old_seg->data_offset;
          cur_left_seg_size = left_key_size + left_data_size;
          left_total_size += cur_left_seg_size;
          Slice left_seg_max_key = left_table[seg_idx].key;
          // assert(!left_seg_max_key.empty());
          mu->Lock();
          uint64_t new_seg_num = NewSegNumber();
          mu->Unlock();
          SegmentMeta* left_seg = new SegmentMeta(
              old_seg->data_offset, left_data_size, old_seg->key_offset,
              left_key_size, old_seg->file_number, new_seg_num,
              old_seg->smallest.Encode(), left_seg_max_key);
          assert(icmp_.Compare(old_seg->smallest.Encode(), left_seg_max_key) <=
                 0);
          left_seg_metas.push_back(left_seg);

          if (left_max_key.empty() ||
              icmp_.Compare(left_seg_max_key, left_max_key) > 0) {
            left_max_key = left_seg_max_key;
          }
        }

        // key not empty means there is elements in the
        // right part of the segment
        uint64_t right_key_size = 0;
        uint64_t right_data_size = 0;
        Slice right_seg_min_key = right_table[seg_idx].key;
        if (!right_seg_min_key.empty()) {
          right_empty = false;
          uint64_t right_key_offset;
          uint64_t right_data_offset;
          DecodeMetaVals(right_table[seg_idx].key_data_offset,
                         &right_key_offset, &right_data_offset);
          right_key_size = old_seg->key_size - left_key_size;
          right_data_size = old_seg->data_size - left_data_size;
          // uint64_t right_key_size = end_
          cur_right_seg_size = right_key_size + right_data_size;
          right_total_size += cur_right_seg_size;

          mu->Lock();
          uint64_t new_seg_num = NewSegNumber();
          mu->Unlock();
          SegmentMeta* right_seg = new SegmentMeta(
              right_data_offset, right_data_size, right_key_offset,
              right_key_size, old_seg->file_number, new_seg_num,
              right_seg_min_key, old_seg->largest.Encode());
          assert(icmp_.Compare(right_seg_min_key, old_seg->largest.Encode()) <=
                 0);
          right_seg_metas.push_back(right_seg);
          if (right_min_key.empty() ||
              icmp_.Compare(right_seg_min_key, right_min_key) < 0) {
            right_min_key = right_seg_min_key;
          }
          assert(right_key_offset + right_key_size == end_key_offset);

        } else {
          assert(left_key_size == old_seg->key_size);
        }
        // assert(cur_left_seg_size + cur_right_seg_size ==
        // f->segments[seg_idx]->seg_size); assert(right_key_size +
        // left_key_size == old_seg->key_size); assert(right_data_size +
        // left_data_size == old_seg->data_size);
      }

      // printf("split left size: %ld, right size: %ld\n", left_total_size,
      //        right_total_size);

      assert(left_total_size + right_total_size == f->file_size);
      //  new interval should be  [left1, left_seg_right_bound]
      // tmp_intervals.push_back(new TableInterval(&left1, &right1));

      // tmp_intervals.push_back(new TableInterval(&left2, &left_max_key,
      // &left2, &left_max_key)); we merge middle empty part to the left since
      // it is small range space.
      Slice left_seg_right_bound;
      if (right_empty) {
        left_seg_right_bound = right2;
      } else {
        left_seg_right_bound = right_min_key;
      }

      // prevent first table interval grow too larger
      if (i == 0) {
        if (left_total_size >= config::kL1_MaxSegBytes) {
          tmp_intervals.push_back(new TableInterval(nullptr, &left1));
        }
      }
      tmp_intervals.push_back(new TableInterval(&left1, &left_seg_right_bound,
                                                &left2, &left_max_key));
      tmp_intervals.back()->segments = left_seg_metas;
      tmp_intervals.back()->total_size = left_total_size;
      if (!right_empty) {
        assert(icmp_.Compare(right_min_key, left_seg_right_bound) == 0);
        tmp_intervals.push_back(new TableInterval(
            &left_seg_right_bound, &right2, &right_min_key, &right2));
        tmp_intervals.back()->segments = right_seg_metas;
        tmp_intervals.back()->total_size = right_total_size;
      } else {
        assert(left_max_key.compare(right2) == 0);
      }

      if (!left_max_key.empty() && !right_min_key.empty()) {
        assert(icmp_.Compare(left_max_key, right_min_key) < 0);
      }
    } else {
      bool whole_range = false;
      if (i == 0 && input[i]->file_size >= config::kL1_MaxSegBytes) {
        tmp_intervals.push_back(new TableInterval(nullptr, &right1));
        tmp_intervals.push_back(
            new TableInterval(&left2, &right2, &left2, &right2));
        whole_range = true;

      } else {
        tmp_intervals.push_back(
            new TableInterval(&left1, &right2, &left2, &right2));
        tmp_intervals.back()->segments = (input[i]->segments);
        tmp_intervals.back()->total_size = input[i]->file_size;
      }
      // segment->ref++?
    }
  }
  // InternalKey * left=  new InternalKey();
  // left->DecodeFrom(input.back()->largest.Encode());
  // Slice left(input.back()->largest.Encode());
  // tmp_intervals.push_back(new TableInterval(&left, nullptr));

  if (tmp_intervals.back()->total_size < config::kL1_MaxSegBytes) {
    tmp_intervals.back()->right_bound = nullptr;
  } else {
    Slice new_end(tmp_intervals.back()->right_bound->Encode());
    tmp_intervals.push_back(new TableInterval(&new_end, nullptr));
  }
  // if back is small, we just update it, else we add another interval.

  for (int i = 1; i < tmp_intervals.size(); i++) {
    assert(icmp_.Compare(*(tmp_intervals[i]->left_bound),
                         *(tmp_intervals[i - 1]->right_bound)) >= 0);
  }
}

// the compaction must happen at level1
Status VersionSet::MakeNewFiles(Compaction* compact,
                                std::vector<LevelOutput>& res,
                                port::Mutex* mu) {
  assert(compact->level() == 0);
  // res must be ordered.
  std::vector<TableInterval*> tmp_intervals;

  // compact->inputs_[1] maybe empty
  // so in this case, we will just add inputs_[0] to the final LevelOutput, and
  // that's it.

  // segments with bigger should be at the front or at the back
  // for each segment of each SSTable in level i
  // we will try to split the file
  std::vector<FileMetaData*> inputs0 = compact->inputs_[0];
  if (compact->level() == 0) {
    sort(inputs0.begin(), inputs0.end(), NewestFirst);
  }
  std::vector<FileMetaData*> inputs1 = (compact->inputs_[1]);

  // printf("input 0 size: %ld, input 1 size: %ld", inputs0.size(),
  //        inputs1.size());
  // todo:
  // if the size of inputs1 < preset Table limit
  // then we will double that table interval to add more internvals
  // else we will not add any new intervals and just append new segments to
  // the original ones.
  if (inputs1.empty()) {
    FileMetaData* inputs0_oldest = inputs0.back();
    inputs0.pop_back();
    inputs1.push_back(inputs0_oldest);
  }

  bool is_split = false;
  if (current_->files_[1].size() < config::kL1_StopSplitTrigger) {
    is_split = true;
  }

  int remain_interval_num =
      config::kL1_StopSplitTrigger - current()->files_[1].size();
  // do how manay times split
  SetupTableIntervals(tmp_intervals, inputs1, mu, is_split,
                      remain_interval_num);

  // there is optimizaiton we can do, but for now,
  // I just pick the simplest solution
  for (int i = inputs0.size() - 1; i >= 0; i--) {
    assert(inputs0[i]->segments.size() == 1);
    const SegmentMeta* seg = inputs0[i]->segments[0];
    std::vector<Iterator*> seg_iters =
        table_cache_nvm_->NewIndexIterator(inputs0[i]);
    assert(seg_iters.size() == 1);
    SplitFileForInputs(seg, seg_iters[0], tmp_intervals, mu);

    for (int j = 0; j < seg_iters.size(); j++) {
      delete seg_iters[j];
    }
  }

  // SplitInterval(new_intervals, tmp_intervals, mu);
  for (int i = 0; i < tmp_intervals.size(); i++) {
    if (tmp_intervals[i]->segments.size() > 0) {
      if (!res.empty()) {
        assert(icmp_.Compare(*(tmp_intervals[i]->real_left_bound),
                             res.back().largest) > 0);
      }

      if (!tmp_intervals[i]->segments.empty()) {
        mu->Lock();
        uint64_t new_file_number = NewFileNumber();
        mu->Unlock();
        res.push_back(LevelOutput(new_file_number, tmp_intervals[i]->total_size,
                                  *(tmp_intervals[i]->real_left_bound),
                                  *(tmp_intervals[i]->real_right_bound),
                                  tmp_intervals[i]->segments));
      }
    }
  }

  // we do the split work here .

  // for (int i = 0; i < res.size(); i++) {
  //   printf("file size: %ld MB", res[i].file_size / 1024 / 1024);
  // }
  // printf("\n");

  for (int i = 0; i < tmp_intervals.size(); i++) {
    delete tmp_intervals[i];
  }
  return Status::OK();
}

int FindTableInterval(const InternalKeyComparator& icmp,
                      const std::vector<TableInterval*>& intervals,
                      const Slice& key) {
  uint32_t left = 0;
  uint32_t right = intervals.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const TableInterval* interval = intervals[mid];

    if (interval->right_bound != nullptr &&
        icmp.Compare(interval->right_bound->Encode(), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return right;
}

static bool BeforeInterval(const InternalKeyComparator& icmp,
                           const Slice& user_key,
                           const TableInterval* interval) {
  return ((interval->left_bound != nullptr &&
           icmp.Compare(user_key, (interval->left_bound->Encode())) < 0));
}

Status VersionSet::DecodeMetaVals(Slice& val, uint64_t* key_offset,
                                  uint64_t* key_data_offset) {
  *key_offset = DecodeFixed64(val.data());
  *key_data_offset = DecodeFixed64(val.data() + 8);

  return Status::OK();
}

// how to open a table ?
// I think we should pass table_cache_ to this function.
// anyway I just want it to be quick.
// we should unlock mutex during file reading
Status VersionSet::SplitFileForInputs(const SegmentMeta* seg,
                                      Iterator* seg_key_iter,
                                      std::vector<TableInterval*>& base_files,
                                      port::Mutex* mu) {
  // first we need to locate the first file that has overlapping with seg
  // then from the first overlapping file we start splitting the segments
  // until we get to the end of the segment
  // while we are splitting the segment,we should append that split part to the
  // base_files.

  size_t first_overlap_file_idx =
      FindTableInterval(icmp_, base_files, seg->smallest.Encode());

  assert(first_overlap_file_idx < base_files.size());
  assert(!BeforeInterval(icmp_, seg->largest.Encode(),
                         base_files[first_overlap_file_idx]));

  // how about using an iterator
  uint64_t prev_data_start = seg->data_offset;
  uint64_t prev_key_start = seg->key_offset;
  // this assert is true because we are only split segments from level0
  assert(prev_data_start == 0);
  InternalKey prev_key;
  uint64_t base_files_idx = first_overlap_file_idx;

  // we can iterate through the index key part instead of the data part.
  seg_key_iter->SeekToFirst();
  if (!prev_key.DecodeFrom(seg_key_iter->key())) {
    return Status::Corruption("prev key decode failed");
  }
  while (seg_key_iter->Valid() && base_files_idx < base_files.size()) {
    bool end = false;
    if (base_files_idx == base_files.size() - 1) {
      seg_key_iter->SeekToLast();
      end = true;
    } else {
      seg_key_iter->Seek(base_files[base_files_idx]->right_bound->Encode());
    }
    // what if the right bound key is too large that iter->valid() is not true,
    // so in this case , we will just call iter->seektoLast() and push the
    // segment and then call iter->next() to set iter invalid;
    if (!seg_key_iter->Valid()) {
      seg_key_iter->SeekToLast();
      end = true;
    }
    InternalKey cur_key;
    cur_key.DecodeFrom(seg_key_iter->key());

    // value format: key_offset, offset_in_data_part
    Slice val = seg_key_iter->value();
    uint64_t cur_key_offset;
    uint64_t key_data_offset;
    if (end) {
      // this is true because key part and data part is continuous
      cur_key_offset = seg->seg_size;
      key_data_offset = seg->data_size;
    } else {
      Status s = DecodeMetaVals(val, &cur_key_offset, &key_data_offset);
    }

    if (cur_key_offset > prev_key_start || end) {
      uint64_t cur_key_size = cur_key_offset - prev_key_start;
      uint64_t cur_key_data_size = key_data_offset - prev_data_start;

      InternalKey seg_largest_key;
      if (!end) {
        seg_key_iter->Prev();
      }
      assert(seg_key_iter->Valid());
      seg_largest_key.DecodeFrom(seg_key_iter->key());
      seg_key_iter->Next();

      mu->Lock();
      uint64_t new_seg_num = NewSegNumber();
      mu->Unlock();
      SegmentMeta* seg_meta = new SegmentMeta(
          prev_data_start, cur_key_data_size, prev_key_start, cur_key_size,
          seg->file_number, new_seg_num, prev_key, seg_largest_key);

      base_files[base_files_idx]->segments.push_back(seg_meta);
      base_files[base_files_idx]->total_size += seg_meta->seg_size;

      // ToDo:: we need to update interval boundary !!!!
      // InternalKey* base_smallest =
      // base_files[base_files_idx]->real_left_bound;
      InternalKey*& base_smallest = base_files[base_files_idx]->real_left_bound;
      if (base_smallest == nullptr ||
          icmp_.Compare(prev_key, *base_smallest) < 0) {
        if (base_smallest == nullptr) {
          base_smallest = new InternalKey();
        }
        base_smallest->DecodeFrom(prev_key.Encode());
      }
      if (base_files_idx > 0) {
        if (base_files[base_files_idx - 1]->real_right_bound) {
          assert(
              icmp_.Compare(
                  base_smallest->Encode(),
                  base_files[base_files_idx - 1]->real_right_bound->Encode()) >
              0);
        }

        assert(icmp_.Compare(
                   base_smallest->Encode(),
                   base_files[base_files_idx - 1]->right_bound->Encode()) >= 0);
      }

      InternalKey*& base_file_largest =
          base_files[base_files_idx]->real_right_bound;
      if (base_file_largest == nullptr ||
          icmp_.Compare(*base_file_largest, seg_largest_key) < 0) {
        if (base_file_largest == nullptr) {
          base_file_largest = new InternalKey();
        }
        base_file_largest->DecodeFrom(seg_largest_key.Encode());
      }
      if (base_files_idx < base_files.size() - 1) {
        if (base_files[base_files_idx + 1]->real_left_bound) {
          assert(
              icmp_.Compare(
                  base_file_largest->Encode(),
                  base_files[base_files_idx + 1]->real_left_bound->Encode()) <
              0);
        }

        assert(
            icmp_.Compare(base_files[base_files_idx + 1]->left_bound->Encode(),
                          base_file_largest->Encode()) >= 0);
      }
    }

    prev_key = cur_key;
    prev_data_start = key_data_offset;
    prev_key_start = cur_key_offset;

    base_files_idx++;
  }

  // have some elements left in the segment
  // that is to  be appended to the last tableInterval

  // if(seg_key_iter->Valid() ) {
  //   seg_key_iter->SeekToLast();
  //   uint64_t cur_key_offset;
  //   uint64_t key_data_offset;
  //   InternalKey cur_key;
  //   cur_key.DecodeFrom(seg_key_iter->key());
  //   Slice meta_val = seg_key_iter->value();
  //   Status s = DecodeMetaVals(meta_val, &cur_key_offset, &key_data_offset);
  //   if(!s.ok()) {
  //     return s;
  //   }

  //   uint64_t cur_key_size = cur_key_offset - prev_key_start;
  //   uint64_t cur_key_data_size = key_data_offset - prev_data_start;

  //   assert(base_files_idx == base_files.size()-1);
  //   //
  //   base_files[base_files_idx].segments.push_back(SegmentMeta{prev_data_start,
  //   cur_key_data_size, prev_key_start,
  //   // cur_key_data_size, seg.file_number, prev_key, cur_key});

  //   seg_key_iter->Next();
  //   // assert(seg_key_iter->Valid());
  // }

  assert(!seg_key_iter->Valid());

  return Status::OK();

  // that's it.
}

// void VersionSet::SubSetupOtherInputs(Compaction* c) {
//   const int level = c->level();

//   InternalKey smallest, largest;
//   SubCompaction* cur_sub = c->sub_compactions_.back();
//   AddBoundaryInputs(icmp_, current_->files_[level], &cur_sub->inputs[0]);
//   GetRange(cur_sub->inputs[0], &smallest, &largest);

//   current_->GetOverlappingInputs(level + 1, &smallest, &largest,
//                                  &cur_sub->inputs[1]);

//   AddBoundaryInputs(icmp_, current_->files_[level + 1], &cur_sub->inputs[1]);

//   InternalKey all_start, all_limit;
//   GetRange2(cur_sub->inputs[0], cur_sub->inputs[1], &all_start, &all_limit);

//   // if (!c->input[1].empty()) {
//   //   std::vector<FileMetaData*> expand0;
//   //   current_->GetOverlappingInputs(level, &all_start, &all_limit,
//   &expand0);
//   //   AddBoundaryInputs(icmp_, current_->files_[level], &expand0);
//   //   const int64_t inputs0_size =
//   //       TotalFileSize(c->sub_compactions_.back().inputs[0]);
//   //   const int64_t inputs1_size =
//   //       TotalFileSize(c->sub_compactions_.back().inputs[1]);
//   //   const int64_t expand0_size = TotalFileSize(expand0);

//   // }
//   if (level + 2 < config::kNumLevels) {
//     current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
//                                    &cur_sub->grandparent);
//   }

//   compact_pointer_[level] = largest.Encode().ToString();
//   c->edit_.SetCompactPointer(level, largest);
// }

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      is_split_(false),
      is_subcompaction(false) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (!is_subcompaction && input_version_ != nullptr) {
    input_version_->Unref();
  }
  // for (int i = 0; i < sub_compactions_.size(); i++) {
  //   delete sub_compactions_[i];
  // }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          (level_ == 0 || level_ > 1) &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

bool Compaction::IsSplitCompaction() const { return is_split_; }

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
