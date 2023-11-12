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

struct WhereConstraint {
  int table_id;
  ConstraintType type;

  virtual bool check(bitmap_t nullstate, const uint8_t *record) const {
    return true;
  }
  virtual bool live_in(int table_id_) { return table_id == table_id_; }
};

struct ColumnOpValueConstraint : public WhereConstraint {
  std::function<bool(bitmap_t, const char *)> cmp;

  ColumnOpValueConstraint(std::shared_ptr<Field> field, Operator op,
                          std::any val);
  bool check(bitmap_t nullstate, const uint8_t *record) const override;
};

struct QueryPlanner {
  std::vector<std::shared_ptr<TableManager>> tables;
  std::shared_ptr<Selector> selector;
  std::vector<std::shared_ptr<WhereConstraint>> constraints;

  /// engine
  std::vector<std::shared_ptr<Iterator>> direct_iterators;
  std::shared_ptr<Iterator> iter{nullptr};

  void generate_plan();
};