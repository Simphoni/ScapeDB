#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include <engine/defs.h>
#include <engine/field.h>
#include <storage/defs.h>

struct Selector {
  std::vector<std::string> header;
  std::vector<std::shared_ptr<Field>> columns;
  std::vector<Aggregator> aggrs;
  bool has_aggregate{false};

  Selector(std::vector<std::string> &&header_,
           std::vector<std::shared_ptr<Field>> &&columns_,
           std::vector<Aggregator> &&aggrs_)
      : header(std::move(header_)), columns(std::move(columns_)),
        aggrs(std::move(aggrs_)) {
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

  virtual bool check(const uint8_t *record, const uint8_t *other) const = 0;
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

struct ColumnLikeStringConstraint : public WhereConstraint {
  std::function<bool(const char *)> cmp;

  ColumnLikeStringConstraint(std::shared_ptr<Field> field,
                             std::string &&pattern_);
  bool check(const uint8_t *record, const uint8_t *other) const override {
    return cmp((char *)record);
  }
};

struct ColumnOpSubqueryConstraint : public WhereConstraint {
  std::shared_ptr<QueryPlanner> subquery;
  std::function<bool(const char *)> cmp;

  ColumnOpSubqueryConstraint(std::shared_ptr<Field> field, Operator op,
                             std::shared_ptr<QueryPlanner> subquery);
  bool check(const uint8_t *record, const uint8_t *other) const override {
    return cmp((char *)record);
  }
};

struct ColumnInSubqueryConstraint : public WhereConstraint {
  std::shared_ptr<QueryPlanner> subquery;
  std::set<int> vals_int;
  std::set<double> vals_float;
  std::set<std::string> vals_str;

  std::function<bool(const char *)> cmp;

  ColumnInSubqueryConstraint(std::shared_ptr<Field> field,
                             std::shared_ptr<QueryPlanner> subquery);
  bool check(const uint8_t *record, const uint8_t *other) const override {
    return cmp((char *)record);
  }
};

struct SetVariable {
  std::function<void(char *)> set;
  SetVariable(std::shared_ptr<Field> field, std::any &&value);
};

class QueryPlanner {
private:
  std::vector<std::shared_ptr<BlockIterator>> direct_iterators;
  std::shared_ptr<Iterator> iter{nullptr};

public:
  std::vector<std::shared_ptr<TableManager>> tables;
  std::shared_ptr<Selector> selector;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;
  std::shared_ptr<Field> group_by_field, order_by_field;
  int req_offset{0}, req_limit{INT_MAX};
  bool order_by_desc;

  void generate_plan();
  const uint8_t *get() const;
  bool next();
};