// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include "db/dbformat.h"
#include <set>
#include <utility>
#include <vector>

#include "table_nvm/table_nvm.h"

namespace leveldb {

class VersionSet;

struct SegmentMeta {
  int refs;  // there maybe multiple table_nvm which references this segment
  uint64_t data_offset;
  uint64_t data_size;
  uint64_t key_offset;
  uint64_t key_size;
  uint64_t seg_size;

  // uint64_t sstable_number;

  uint64_t file_number;  // physical file number to which this segment belongs
  uint64_t seg_number;   // unique id for cache_nvm

  InternalKey smallest;
  InternalKey largest;

  SegmentMeta(uint64_t d_offset, uint64_t d_size, uint64_t k_offset,
              uint64_t k_size, uint64_t f_num, uint64_t s_num,
              InternalKey& small, InternalKey& large)
      : refs(0),
        data_offset(d_offset),
        data_size(d_size),
        key_offset(k_offset),
        key_size(k_size),
        file_number(f_num),
        seg_number(s_num),
        seg_size(d_size + k_size) {
    smallest.DecodeFrom(small.Encode());
    largest.DecodeFrom(large.Encode());
  }
  // std::vector<KeyMetaData*> key_metas;

  SegmentMeta(uint64_t d_offset, uint64_t d_size, uint64_t k_offset,
              uint64_t k_size, uint64_t f_num, uint64_t s_num,
              const Slice& small, const Slice& large)
      : refs(0),
        data_offset(d_offset),
        data_size(d_size),
        key_offset(k_offset),
        key_size(k_size),
        file_number(f_num),
        seg_number(s_num),
        seg_size(d_size + k_size) {
    smallest.DecodeFrom(small);
    largest.DecodeFrom(large);
  }
};

// filemetadata is a logic SSTable that contain multiple
// segments of data whose key lying in the same range
struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}
  ~FileMetaData() {
    // for(size_t i=0; i < segments.size(); i++) {
    //   delete segments[i];
    // }
    for (int i = 0; i < segments.size(); i++) {
      segments[i]->refs--;
      if (segments[i]->refs == 0) {
        delete segments[i];
      }
    }
  }
  int refs;  // there maybe multiple version which references this FileMetaData
  int allowed_seeks;     // Seeks allowed until compaction
  uint64_t number;       // sstable number
  uint64_t file_size;    // File size in bytes, = total size of all segments
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table

  bool is_nvm;  // is the sstable on level 0 or on level 1 ?
  std::vector<SegmentMeta*> segments;
  // uint64_t raw_data_size;
  // uint64_t key_meta_offset;
  // uint64_t key_meta_size;
  // uint64_t meta_size;
  // uint64_t meta_index_size;

  // once we open a table, we will read all the meta keys into the rep of the
  // table this sounds reasonable
};

// VersionEdit represent an atomic
// update of current version
// a versionEdit includes updating
// next file numebr, last sequence number,
// and next compact pointers for each level.
// a version change will also inlucde adding new
// files and deleting files
// Now it make sense to me for why we need such class.
// great!!
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetNextSSTable(uint64_t num) {
    has_next_file_number_ = true;
    next_sstable_number_ = num;
  }

  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  void AddSSTable(int level, uint64_t sstable, uint64_t sstable_size,
                  const InternalKey& smallest, const InternalKey& largest,
                  const std::vector<SegmentMeta*>& segments) {
    FileMetaData f;
    f.number = sstable;
    f.file_size = sstable_size;
    f.smallest = smallest;
    f.largest = largest;
    // f.segments.resize(segments.size());
    // for(size_t i=0; i < segments.size(); i++) {
    //   f.segments[i] = segments[i];
    // }
    f.segments = std::move(segments);
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  uint64_t next_sstable_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
  bool has_next_sstable_number_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  // deleted_files is the set of sstable numbers
  // we want to remove from prev version
  DeletedFileSet deleted_files_;
  // the new sstable numbers we want to add to
  // prev version
  std::vector<std::pair<int, FileMetaData>> new_files_;

  // std::vector<std::pair<int, SSTableMeta>> new_sstables_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
