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
  std::shared_ptr<Field> field;
  ConstraintType type;
  int column_offset;

  virtual bool check(bitmap_t nullstate, const uint8_t *record) const {
    return true;
  }
};

struct ColumnOpValueConstraint : public WhereConstraint {
  Operator op;
  DataType type;
  std::function<bool(bitmap_t, const uint8_t *)> cmp;

  bool check(bitmap_t nullstate, const uint8_t *record) const override;
};

struct QueryPlanner {
  std::vector<std::shared_ptr<TableManager>> tables;
  std::shared_ptr<Selector> selector;
  std::shared_ptr<WhereConstraint> where;

  /// engine
  std::vector<std::shared_ptr<Iterator>> direct_iterators;
  std::shared_ptr<Iterator> iter{nullptr};

  void generate_plan();
};