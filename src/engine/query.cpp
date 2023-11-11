#include "engine/defs.h"
#include "storage/paged_buffer.h"
#include <cassert>
#include <set>

#include <engine/query.h>
#include <engine/system_manager.h>
#include <storage/storage.h>

bool Selector::parse_from_query(std::shared_ptr<DatabaseManager> db,
                                const std::vector<std::string> &table_names,
                                std::vector<std::string> &&cols,
                                std::vector<Aggregator> &&aggrs_) {
  if (db == nullptr) {
    printf("ERROR: no database selected\n");
    has_err = true;
    return false;
  }
  std::set<std::string> tables_name_set;
  for (auto tab_name : table_names) {
    auto table = db->get_table_manager(tab_name);
    if (table == nullptr) {
      printf("ERROR: TABLE %s not found\n", tab_name.data());
      has_err = true;
      return false;
    } else {
      tables.push_back(table);
      tables_name_set.insert(tab_name);
    }
  }
  if (cols.size() == 0) {
    is_all = true;
    for (auto table : tables) {
      for (auto field : table->get_fields()) {
        columns.push_back(field);
        aggrs.push_back(Aggregator::NONE);
        header.push_back(field->field_name);
      }
    }
    return true;
  }
  for (int i = 0; i < cols.size(); i++) {
    const std::string &col = cols[i];
    Aggregator aggr = aggrs_[i];
    if (aggr != NONE) {
      has_aggregate = true;
    }
    /// generate header for display, also formats selector
    if (col == "") {
      columns.push_back(nullptr);
      aggrs.push_back(Aggregator::COUNT);
      header.push_back("COUNT(*)");
      continue;
    }
    if (aggr == NONE) {
      header.push_back(col);
    } else {
      header.push_back(aggr2str(aggr) + "(" + col + ")");
    }
    if (col.find('.') == std::string::npos) {
      int num = 0;
      for (auto table : tables) {
        num += (table->get_field(col) != nullptr);
      }
      if (num > 1) {
        printf("ERROR: ambiguous column name %s\n", col.data());
        has_err = true;
        return false;
      } else if (num == 0) {
        printf("ERROR: COLUMN %s not found\n", col.data());
        has_err = true;
        return false;
      }
      for (auto table : tables) {
        auto field = table->get_field(col);
        if (field != nullptr) {
          columns.push_back(field);
          aggrs.push_back(aggr);
          break;
        }
      }
    } else {
      int dot = col.find('.');
      std::string tab_name = col.substr(0, dot);
      std::string col_name = col.substr(dot + 1);
      auto table = db->get_table_manager(tab_name);
      if (table == nullptr || !tables_name_set.contains(tab_name)) {
        printf("ERROR: cannot locate %s\n", col.data());
        has_err = true;
        return false;
      }
      auto field = table->get_field(col_name);
      if (field == nullptr) {
        printf("ERROR: COLUMN %s not found\n", col.data());
        has_err = true;
        return false;
      }
      columns.push_back(field);
      aggrs.push_back(aggr);
    }
  }
  return true;
}

void QueryPlanner::generate_plan() {
  if (direct_iterators.size() == 1) {
    iter = direct_iterators[0];
    return;
  }
}