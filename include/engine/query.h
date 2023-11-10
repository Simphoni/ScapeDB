#pragma once

#include <memory>
#include <string>
#include <vector>

#include <engine/defs.h>
#include <engine/field.h>
#include <storage/defs.h>

struct Selector {
  bool is_all;
  bool has_aggregate;
  std::vector<std::string> header;
  std::vector<std::pair<std::shared_ptr<Field>, Aggregator>> columns;
  std::vector<std::shared_ptr<TableManager>> tables;

  bool parse_from_query(std::shared_ptr<DatabaseManager> db,
                        const std::vector<std::string> &table_names,
                        std::vector<std::pair<std::string, Aggregator>> &&cols);
  bool to_string();
};

struct WhereConstraint {
  ConstraintType type;
  std::shared_ptr<Field> field;
  int column_offset;

  virtual bool check(bitmap_t nullstate, const uint8_t *record) const {
    return true;
  }
};

struct QueryPlanner {
  int fd;
  std::shared_ptr<SequentialAccessor> accessor;

  int n_pages;
  int n_rows;
  std::shared_ptr<Selector> selector;
  std::shared_ptr<WhereConstraint> where;

  QueryPlanner();
  ~QueryPlanner();
};