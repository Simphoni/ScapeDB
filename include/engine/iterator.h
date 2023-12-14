#pragma once

#include <map>
#include <memory>
#include <set>
#include <vector>

#include <engine/defs.h>
#include <engine/field.h>
#include <storage/defs.h>
#include <utils/config.h>

const int QUERY_MAX_BLOCK = 8 << 20; /// 8MB
const int QUERY_MAX_PAGES = QUERY_MAX_BLOCK / Config::PAGE_SIZE;

struct field_caster {
  DataType type;
  int len, idx, offset;
};

/// Iterator class
/// an iterator reads/gathers records from a "source" (aka "all") and buffers
/// them in a temporary file.
/// it provides two levels of iteration:
/// - block level
/// - source level
/// the buffer implementation is identical across all iterators

class Iterator {
protected:
  IteratorType type;
  std::vector<std::shared_ptr<Field>> fields_dst;
  int record_len;

  Iterator(IteratorType type) : type(type) {}

public:
  const std::vector<std::shared_ptr<Field>> &get_fields_dst() const {
    return fields_dst;
  }
  virtual bool get_next_valid() = 0;
  virtual const uint8_t *get() const = 0;
};

class BlockIterator : public Iterator {
protected:
  bool source_ended{false};
  int fd_dst;
  std::set<unified_id_t> table_ids;
  int record_per_page;
  int n_records{0}, dst_iter{0};

  BlockIterator(IteratorType type) : Iterator(type) {}

public:
  /// we process data in a per-block basis
  /// Iterator exposes fill_next_block() method that filters source data
  /// into a temporary buffer that will reside in memory during query
  /// this should speed up brute-force iterative query
  void reset_block() { dst_iter = 0; }
  void block_next() {
    if (dst_iter == n_records)
      return;
    dst_iter++;
  }
  bool block_end() const { return dst_iter == n_records; }
  bool all_end() const { return source_ended && dst_iter == n_records; }
  const std::set<unified_id_t> &get_table_ids() const { return table_ids; }

  const uint8_t *get() const override;
  /// fill next block of records into buffer and reset block iter
  /// @return number of records filled
  virtual int fill_next_block() = 0;
  virtual void reset_all() = 0;
};

class RecordIterator : public BlockIterator {
private:
  int fd_src, pagenum_src, slotnum_src;
  std::shared_ptr<RecordManager> record_manager;
  std::vector<std::shared_ptr<Field>> fields_src;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  std::vector<int> valid_records;
  std::vector<int>::iterator it;

public:
  /// @param cons will be filtered
  /// @param fileds_src fields directly taken from table_manager
  /// @param fields_dst will be filtered with fields_src
  RecordIterator() = delete;
  RecordIterator(std::shared_ptr<RecordManager> rec,
                 const std::vector<std::shared_ptr<WhereConstraint>> &cons,
                 const std::vector<std::shared_ptr<Field>> &fields_src,
                 const std::vector<std::shared_ptr<Field>> &fields_dst);
  ~RecordIterator();
  bool get_next_valid_no_check();
  bool get_next_valid() override;
  void reset_all() override;
  int fill_next_block() override;
  /// for delete/set operatione
  std::pair<int, int> get_locator();
};

class IndexIterator : public BlockIterator {
private:
  std::shared_ptr<BPlusTree> tree;
  int fd_src, pagenum_src, slotnum_src;
  int pagenum_init, slotnum_init;
  int leaf_data_len, leaf_max, key_num;
  bool store_full_data;
  int lbound, rbound;
  std::vector<std::shared_ptr<Field>> fields_src;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;

public:
  /// [lbound, rbound)
  IndexIterator(std::shared_ptr<IndexMeta> index, int lbound, int rbound,
                const std::vector<std::shared_ptr<WhereConstraint>> &cons,
                const std::vector<std::shared_ptr<Field>> &fields_src,
                const std::vector<std::shared_ptr<Field>> &fields_dst);
  // todo: write this
  // ~IndexIterator();
  bool get_next_valid() override;
  void reset_all() override;
  int fill_next_block() override;
};

class JoinIterator : public BlockIterator {
private:
  std::shared_ptr<BlockIterator> lhs, rhs;
  std::vector<std::pair<int, int>> pos_dst_lhs, pos_dst_rhs;
  std::vector<std::shared_ptr<Field>> fields_dst_lhs, fields_dst_rhs;
  std::vector<std::shared_ptr<ColumnOpColumnConstraint>> constraints;

public:
  JoinIterator(std::shared_ptr<BlockIterator> lhs,
               std::shared_ptr<BlockIterator> rhs,
               const std::vector<std::shared_ptr<WhereConstraint>> &cons,
               const std::vector<std::shared_ptr<Field>> &fields_dst);

  bool get_next_valid() override;
  void reset_all() override;
  int fill_next_block() override;
};

class PermuteIterator : public Iterator {
private:
  std::shared_ptr<BlockIterator> iter;
  std::vector<std::pair<int, int>> permute_info;
  std::vector<std::shared_ptr<Field>> fields_src;
  std::vector<uint8_t> buffer;

public:
  PermuteIterator(std::shared_ptr<BlockIterator> iter,
                  const std::vector<std::shared_ptr<Field>> fields_dst);
  bool get_next_valid() override;
  const uint8_t *get() const override;
};

class GatherIterator : public Iterator {
protected:
  bool built{false};
  int fd, record_per_page;
  int n_records{0}, iter_dst{0};
  std::vector<field_caster> caster;
  std::vector<std::shared_ptr<Field>> fields_src;

  GatherIterator(IteratorType type) : Iterator(type) {}

  virtual void build() = 0;
};

class AggregateIterator : public GatherIterator {
private:
  typedef int count_t;
  static_assert(std::is_same<count_t, IntType::DType>::value,
                "count_t must equal IntType::DType");
  std::shared_ptr<BlockIterator> iter;
  std::shared_ptr<Field> group_by_field;
  int group_by_field_offset;
  int export_len;
  std::vector<Aggregator> aggrs;
  std::vector<uint8_t> buffer;

  void build() override;
  void update(uint8_t *p, const uint8_t *o);
  int assign_page();

public:
  AggregateIterator(std::shared_ptr<BlockIterator> iterator,
                    std::shared_ptr<Field> group_by_field,
                    const std::vector<std::shared_ptr<Field>> fields_dst_,
                    const std::vector<Aggregator> &aggrs);
  bool get_next_valid() override;
  const uint8_t *get() const override;
};