#include "storage/paged_buffer.h"
#include <cassert>
#include <set>

#include <engine/query.h>
#include <engine/system_manager.h>
#include <storage/storage.h>

bool Selector::parse_from_query(
    std::shared_ptr<DatabaseManager> db,
    const std::vector<std::string> &table_names,
    std::vector<std::pair<std::string, Aggregator>> &&cols) {
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
        columns.emplace_back(field, NONE);
        header.emplace_back(field->field_name);
      }
    }
    return true;
  }
  for (auto col : cols) {
    if (col.second != NONE) {
      has_aggregate = true;
    }
    /// generate header for display, also formats selector
    if (col.first == "") {
      columns.emplace_back(nullptr, col.second);
      header.emplace_back("COUNT(*)");
      continue;
    }
    if (col.second == NONE) {
      header.emplace_back(col.first);
    } else {
      header.emplace_back(aggr2str(col.second) + "(" + col.first + ")");
    }
    const std::string &f = col.first;
    if (f.find('.') == std::string::npos) {
      int num = 0;
      for (auto table : tables) {
        num += (table->get_field(f) != nullptr);
      }
      if (num > 0) {
        printf("ERROR: ambiguous column name %s\n", f.data());
        has_err = true;
        return false;
      } else if (num == 0) {
        printf("ERROR: COLUMN %s not found\n", f.data());
        has_err = true;
        return false;
      }
      for (auto table : tables) {
        auto field = table->get_field(f);
        if (field != nullptr) {
          columns.push_back(std::make_pair(field, col.second));
          break;
        }
      }
    } else {
      int dot = f.find('.');
      std::string tab_name = f.substr(0, dot);
      std::string col_name = f.substr(dot + 1);
      auto table = db->get_table_manager(tab_name);
      if (table == nullptr || !tables_name_set.contains(tab_name)) {
        printf("ERROR: cannot locate %s\n", f.data());
        has_err = true;
        return false;
      }
      auto field = table->get_field(col_name);
      if (field == nullptr) {
        printf("ERROR: COLUMN %s not found\n", f.data());
        has_err = true;
        return false;
      }
      columns.push_back(std::make_pair(field, col.second));
    }
  }
  return true;
}

QueryPlanner::QueryPlanner() {
  fd = FileMapping::get()->create_temp_file();
  accessor = std::make_shared<SequentialAccessor>(fd);
}

QueryPlanner::~QueryPlanner() { FileMapping::get()->close_temp_file(fd); }
