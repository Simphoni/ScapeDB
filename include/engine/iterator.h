#pragma once

#include <map>
#include <memory>
#include <vector>

#include <engine/defs.h>
#include <storage/defs.h>
#include <utils/config.h>

const int QUERY_MAX_BLOCK = 8 << 20; /// 8MB
const int QUERY_MAX_PAGES = QUERY_MAX_BLOCK / Config::PAGE_SIZE;

/// Iterator class
/// a iterator reads/gathers records from a "source" (aka "all") and buffers
/// them in a temporary file.
/// it provides two levels of iteration:
/// - block level
/// - source level
/// the buffer implementation is identical across all iterators

class Iterator {
protected:
  IteratorType type;
  bool source_ended;
  int fd_dst, fd_src;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  std::vector<std::shared_ptr<Field>> fields_src, fields_dst;
  int record_len, record_per_page;
  /// for each field_to, its original offset in the record
  std::vector<std::pair<int, int>> offset_remap;
  int n_records;
  int dst_iter{0}, pagenum_dst{0}, slotnum_dst{0};
  uint8_t *current_dst_page;

public:
  inline int max_record_capacity() const {
    return record_per_page * QUERY_MAX_PAGES;
  }
  /// we process data in a per-block basis
  /// Iterator exposes fill_next_block() method that filters source data
  /// into a temporary buffer that will reside in memory during query
  /// this should speed up brute-force iterative query
  void reset_block() { pagenum_dst = slotnum_dst = 0; }
  void block_next();
  bool block_end() const { return dst_iter == n_records; }
  bool all_end() const { return source_ended; }
  virtual void get(std::vector<uint8_t> &buf);
  virtual int fill_next_block() = 0;
  virtual void reset_all() = 0;
};

class RecordIterator : public Iterator {
private:
  int pagenum_src, slotnum_src;
  uint8_t *current_src_page;
  std::shared_ptr<RecordManager> record_manager;
  std::vector<int> valid_records;
  std::vector<int>::iterator it;

public:
  /// @param cons consists only of self-related constraints
  /// @param fields_dst will be filtered with fields_src
  RecordIterator(std::shared_ptr<RecordManager> rec,
                 std::vector<std::shared_ptr<WhereConstraint>> &&cons,
                 const std::vector<std::shared_ptr<Field>> &fields_src,
                 const std::vector<std::shared_ptr<Field>> &fields_dst);
  bool get_next_valid();
  void reset_all() override;
  int fill_next_block() override;
};

class ResultIterator : public Iterator {
private:
  int fd, n_records;
  std::shared_ptr<Iterator> lhs, rhs;
};