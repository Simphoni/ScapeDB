#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <engine/defs.h>
#include <engine/field.h>
#include <storage/defs.h>

struct Selector {
  std::vector<std::string> header;
  std::vector<std::shared_ptr<Field>> columns;
  std::vector<Aggregator> aggrs;
  bool has_aggregate;

  Selector(std::vector<std::string> &&header,
           std::vector<std::shared_ptr<Field>> &&columns,
           std::vector<Aggregator> &&aggrs)
      : header(std::move(header)), columns(std::move(columns)),
        aggrs(std::move(aggrs)) {
    for (auto aggr : aggrs) {
      if (aggr != Aggregator::NONE) {
        has_aggregate = true;
        break;
      }
    }
  }
};

/// managed with shared_ptr
struct WhereConstraint {
  unified_id_t table_id;

  virtual bool check(const uint8_t *record, const uint8_t *other) const {
    return true;
  }
  virtual bool live_in(unified_id_t table_id_) { return table_id == table_id_; }
};

struct ColumnOpValueConstraint : public WhereConstraint {
  std::function<bool(const char *)> cmp;
  /// reserved for BPlusTree, which supports only integer
  int column_offset, value;
  Operator op;

  ColumnOpValueConstraint(std::shared_ptr<Field> field, Operator op,
                          std::any val);
  bool check(const uint8_t *record, const uint8_t *other) const override {
    return cmp((char *)record);
  }
};

struct ColumnOpColumnConstraint : public WhereConstraint {
  DataType dtype;
  Operator optype;
  bool swap_input{false};
  int len;
  unified_id_t table_id_other;
  unified_id_t field_id1, field_id2;
  std::function<bool(const char *, const char *)> cmp;

  ColumnOpColumnConstraint(std::shared_ptr<Field> field, Operator op,
                           std::shared_ptr<Field> other);
  bool check(const uint8_t *record, const uint8_t *other) const override {
    return swap_input ? cmp((char *)other, (char *)record)
                      : cmp((char *)record, (char *)other);
  }
  bool live_in(unified_id_t table_id_) override {
    return table_id == table_id_ && table_id_other == table_id_;
  }
  void build(int col_idx, int col_off, int col_idx_o, int col_off_o);
};

struct ColumnNullConstraint : public WhereConstraint {
  std::function<bool(const char *)> chk;

  ColumnNullConstraint(std::shared_ptr<Field> field, bool field_not_null);
  bool check(const uint8_t *record, const uint8_t *other) const override;
};

struct SetVariable {
  std::function<void(char *)> set;
  SetVariable(std::shared_ptr<Field> field, std::any &&value);
};

class QueryPlanner {
private:
  std::vector<std::shared_ptr<Iterator>> direct_iterators;
  std::shared_ptr<Iterator> iter{nullptr};
  std::vector<uint8_t> buffer;
  std::vector<std::pair<int, int>> permute_info;

public:
  std::vector<std::shared_ptr<TableManager>> tables;
  std::shared_ptr<Selector> selector;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;

  /// engine

  void generate_plan();
  const uint8_t *get() const;
  bool next();
};